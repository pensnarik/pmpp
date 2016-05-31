\echo Use "create extension pmpp;" to load this file. \quit

set client_min_messages = warning;

create temporary table uname_command_output(uname text);
create temporary table cpu_command_output(num_cpus integer);

create function num_cpus() returns integer
as 'MODULE_PATHNAME','pmpp_num_cpus'
language c immutable strict;

comment on function num_cpus()
is 'The number of cpus detected on this database machine';

create function jsonb_array_to_text_array(jsonb_arr jsonb) returns text[]
language sql as $$
select  array_agg(r)
from    jsonb_array_elements_text(jsonb_arr) r;
$$;

comment on function jsonb_array_to_text_array(jsonb jsonb)
is E'Function to bridge the gap between json arrays and pgsql arrays.\n'
    'This will probably go away when native postgres implements this.';

create type query_manifest as (
    connection text,
    queries text[],
    cpu_multiplier float,
    num_workers integer,
    setup_commands text[]
);

comment on type query_manifest
is 'The basic data structure for nesting commands for distribution';

create function to_query_manifest(item in jsonb) returns query_manifest
language sql strict immutable set search_path from current as $$
select  item->>'connection' as connection,
        -- coalesce the 'queries' array with the 'query' scalar and then remove any nulls.
        -- They shouldn't both appear but if they did there is no reason not to play nice.
        array_remove(jsonb_array_to_text_array(item->'queries') || (item->>'query'),null) as queries,
        (item->>'cpu_multiplier')::float as cpu_multiplier,
        (item->>'num_workers')::integer as num_workers,
        case
            when item->>'statement_timeout' is null then
                jsonb_array_to_text_array(item->'setup_commands')
            else
                jsonb_array_to_text_array(item->'setup_commands') || concat('set statement_timeout = ',item->>'statement_timeout')
        end as setup_commands;
$$;

comment on function to_query_manifest(item in jsonb)
is  'Convert a JSONB manifest item to a query_manifest.\n'
    'This includes support for mapping statement_timeout into the setup_commands array';

create cast (jsonb as query_manifest) with function to_query_manifest(jsonb) as implicit;

comment on cast (jsonb as query_manifest)
is  'Cast JSONB->query_manifest';

create function to_query_manifest_array(manifest in jsonb) returns query_manifest[]
language sql strict immutable set search_path from current as $$
select  array_agg(q::query_manifest)
from    jsonb_array_elements(manifest) q;
$$;

comment on function to_query_manifest_array(item in jsonb)
is  'Convert a JSONB manifest item array to a query_manifest[]';

create cast (jsonb as query_manifest[]) with function to_query_manifest_array(jsonb) as implicit;

comment on cast (jsonb as query_manifest[])
is  'Cast JSONB->query_manifest[]';

create function manifest_set( query_manifest jsonb ) returns setof query_manifest
language sql set search_path from current as $$
select  q::query_manifest
from    jsonb_array_elements(query_manifest) q;
$$;

comment on function manifest_set( query_manifest jsonb )
is E'Decompose a query manifest jsonb into the proper pieces.\n'
    'This is exposed largely for debugging purposes.';

create function manifest_array( query_manifest jsonb ) returns query_manifest[]
language sql set search_path from current as $$
select  array_agg(t)
from    manifest_set(query_manifest) as t;
$$;

comment on function manifest_array( query_manifest jsonb )
is E'Decompose a query manifest jsonb into an array suitable for distribute().\n'
    'This is exposed largely for debugging purposes.';

create function distribute( row_type anyelement,
                            query_manifest query_manifest[] )
                            returns setof anyelement
as 'MODULE_PATHNAME','pmpp_distribute'
language c;

comment on function distribute(anyelement,query_manifest[])
is E'For each query manifest object in the array:\n'
    'Connect to each of the databases with a number of connections equal to num_workers (if specified), or \n'
    'the number of cpus found on the machine at the end of that connection times the mulitplier (defaults to 1.0).\n'
    'Each query specified must return a result set matching the type row_type.\n'
    'The queries are distributed to the connections as they become available, and the results of those queries are\n'
    'in turn pushed to the result set, where they can be further manipulated like any table, view, or set returning function.\n'
    'Returns a result set in the shape of row_type.';

create function distribute( row_type anyelement,
                            query_manifest jsonb )
                            returns setof anyelement
language sql set search_path from current as $$
select distribute(row_type,query_manifest::query_manifest[])
$$;

comment on function distribute(anyelement,jsonb)
is E'Alternate version of distribute that takes a jsonb document instead of an array of query_manifest.\n'
    'The JSON document must be an array of objects in the the following structure:\n'
    '\t [{"connection": "connection-string", "queries": ["query 1","query 2", ...], "num_workers": 4},\n'
    '\t  {"connection": "connection-string", "queries": ["query 10", "query 11", ...], "multiplier": 1.5},\n'
    '\t [{"connection": "connection-string", "query": "query sql"},\n'
    '\t  ...]\n'
    'Other elements within the objects would be ignored.';



create function distribute( row_type anyelement,
                            query_manifest in json )
                            returns setof anyelement
language sql security definer set search_path from current
as $$
select distribute(row_type,query_manifest::jsonb);
$$;

comment on function distribute(anyelement,json)
is 'Pass-through function so that we can accept json as well as jsonb';

create function distribute( row_type anyelement,
                            connection text,
                            sql_list text[],
                            cpu_multiplier float default null, 
                            num_workers integer default null,
                            setup_commands text[] default null)
                            returns setof anyelement
language sql security definer set search_path from current 
as $$
select  distribute(row_type,
                    array[ row(connection,sql_list,cpu_multiplier,num_workers,setup_commands)::query_manifest ]);
$$;

comment on function distribute(anyelement,text,text[],float,integer,text[])
is E'Given an array of sql commands, execute each one against an async conneciton specified,\n'
    'spawning as many connections as specified by the multiplier of the number of cpus on the machine,\n'
    'returning a set of data specified by the row type.\n'
    'This simpler form is geared towards basic parallelism rather than distributed queries';

do $$
declare
    l_role_exists boolean;
begin
    select exists( select null from pg_roles where rolname = 'pmpp') into l_role_exists;
    if not l_role_exists then
        execute 'create role pmpp';
    end if;
end
$$;

create type command_result as ( result text );
comment on type command_result
is 'Simple type for getting the results of non-set-returning-function commands';

create type command_with_result as ( command text, result text );

comment on type command_with_result
is E'Simple type for getting the results of non-set-returning-function commands\n'
    'and the SQL that generated that result';


create function meta(   connection text,
                        sql_list text[],
                        cpu_multiplier float default null,
                        num_workers integer default null,
                        setup_commands text[] default null) returns setof command_with_result
language sql security definer set search_path from current as $$
select  *
from    distribute( null::command_with_result,
                    connection,
                    sql_list,
                    cpu_multiplier,
                    num_workers,
                    setup_commands);
$$;

comment on function meta(text,text[],float,integer,text[])
is E'Convenience routine for executing non-SELECT statements in parallel.\n'
    'Example:\n'
    'SELECT *\n'
    'FROM pmpp.meta(''loopback'', array( SELECT ''CREATE INDEX ON '' || c.table_name || ''('' || c.column_name || '')''\n'
    '                                    FROM information_schema.columns \n'
    '                                    WHERE table_name = ''''my_table_name'''' and table_schema = ''my_schema''))';


create function broadcast(  row_type anyelement,
                            connections text[],
                            query text) returns setof anyelement
language sql
security definer
set search_path from current as $$
select  distribute(row_type,
                    array(  select  row(c.conn, array[query], null, 1, null)::query_manifest
                            from    unnest(connections) c(conn)));
$$;

comment on function broadcast(anyelement,text[],text)
is E'Execute a single SQL query across a list of connections\n'
    'Example:\n'
    'CREATE TEMPORARY TABLE row_counts( rowcount bigint );\n'
    'SELECT *\n'
    'FROM pmpp.broadcast(null::row_counts,\n'
    '                    array( select srvname from pg_foreign_server ),\n'
    '                    ''selct count(*) from pg_class'' );';

grant usage on schema @extschema@ to pmpp;
grant execute on all functions in schema @extschema@ to pmpp;

