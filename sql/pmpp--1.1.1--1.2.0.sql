
alter type query_manifest add attribute result_format text cascade;

create or replace function to_query_manifest(item in jsonb) returns query_manifest
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
        end as setup_commands,
        item->>'result_format' as result_format;
$$;

drop function distribute( anyelement, text, text[], float, integer, text[]);

create function distribute( row_type anyelement,
                            connection text,
                            sql_list text[],
                            cpu_multiplier float default null, 
                            num_workers integer default null,
                            setup_commands text[] default null,
                            result_format text default null)
                            returns setof anyelement
language sql security definer set search_path from current 
as $$
select  distribute(row_type,
                    array[ row(connection,sql_list,cpu_multiplier,num_workers,setup_commands,result_format)::query_manifest ]);
$$;

comment on function distribute(anyelement,text,text[],float,integer,text[],text)
is E'Given an array of sql commands, execute each one against an async conneciton specified,\n'
    'spawning as many connections as specified by the multiplier of the number of cpus on the machine,\n'
    'returning a set of data specified by the row type.\n'
    'This simpler form is geared towards basic parallelism rather than distributed queries';

drop function meta( text, text[], float, integer, text[]);

create function meta(   connection text,
                        sql_list text[],
                        cpu_multiplier float default null,
                        num_workers integer default null,
                        setup_commands text[] default null,
                        result_format text default null) returns setof command_with_result
language sql security definer set search_path from current as $$
select  *
from    distribute( null::command_with_result,
                    connection,
                    sql_list,
                    cpu_multiplier,
                    num_workers,
                    setup_commands,
                    result_format);
$$;

comment on function meta(text,text[],float,integer,text[],text)
is E'Convenience routine for executing non-SELECT statements in parallel.\n'
    'Example:\n'
    'SELECT *\n'
    'FROM pmpp.meta(''loopback'', array( SELECT ''CREATE INDEX ON '' || c.table_name || ''('' || c.column_name || '')''\n'
    '                                    FROM information_schema.columns \n'
    '                                    WHERE table_name = ''''my_table_name'''' and table_schema = ''my_schema''))';


drop function broadcast(anyelement,text[],text);

create function broadcast(  row_type anyelement,
                            connections text[],
                            query text,
                            result_formats text[] default null) returns setof anyelement
language sql
security definer
set search_path from current as $$
with
    c as ( select * from unnest(connections) with ordinality as c(conn,ord) ),
    r as ( select * from unnest(result_formats) with ordinality as r(result_format,ord))
select  distribute(row_type,
                    array(  select  row(c.conn, array[query], null, 1, null,r.result_format)::query_manifest
                            from    c
                            left outer join r on r.ord = c.ord));
$$;

comment on function broadcast(anyelement,text[],text,text[])
is E'Execute a single SQL query across a list of connections\n'
    'Example:\n'
    'CREATE TEMPORARY TABLE row_counts( rowcount bigint );\n'
    'SELECT *\n'
    'FROM pmpp.broadcast(null::row_counts,\n'
    '                    array( select srvname from pg_foreign_server ),\n'
    '                    ''selct count(*) from pg_class'' );';
 
create function to_query_manifest_array(manifest in json) returns query_manifest[]
language sql strict immutable set search_path from current as $$
select to_query_manifest_array(manifest::jsonb);
$$;

comment on function to_query_manifest_array(item in json)
is  'Convert a JSON manifest item array to a query_manifest[]';

create cast (json as query_manifest[]) with function to_query_manifest_array(json) as implicit;

comment on cast (json as query_manifest[])
is  'Cast JSON->query_manifest[]';

create function distribute( query_manifest query_manifest[] ) returns setof record
as 'MODULE_PATHNAME','pmpp_distribute'
language c;

comment on function distribute(query_manifest[])
is E'For each query manifest object in the array:\n'
    'Connect to each of the databases with a number of connections equal to num_workers (if specified), or \n'
    'the number of cpus found on the machine at the end of that connection times the mulitplier (defaults to 1.0).\n'
    'Each query specified must return a result set matching the row specification.\n'
    'The queries are distributed to the connections as they become available, and the results of those queries are\n'
    'in turn pushed to the result set, where they can be further manipulated like any table, view, or set returning function.\n'
    'Returns a result set in the shape of row_type.';

create function distribute( query_manifest jsonb ) returns setof record
as 'MODULE_PATHNAME','pmpp_distribute'
language c;

comment on function distribute(jsonb)
is E'Alternate version of distribute that takes a jsonb document instead of an array of query_manifest.\n'
    'The JSON document must be an array of objects in the the following structure:\n'
    '\t [{"connection": "connection-string", "queries": ["query 1","query 2", ...], "num_workers": 4},\n'
    '\t  {"connection": "connection-string", "queries": ["query 10", "query 11", ...], "multiplier": 1.5},\n'
    '\t [{"connection": "connection-string", "query": "query sql"},\n'
    '\t  ...]\n'
    'Other elements within the objects would be ignored.';

create function distribute( query_manifest in json ) returns setof record
as 'MODULE_PATHNAME','pmpp_distribute'
language c;

comment on function distribute(json)
is E'Pass-through function so that we can accept json as well as jsonb';

create or replace function distribute( row_type anyelement, query_manifest jsonb ) returns setof anyelement
as 'MODULE_PATHNAME','pmpp_distribute' language c;

create or replace function distribute( row_type anyelement, query_manifest json ) returns setof anyelement
as 'MODULE_PATHNAME','pmpp_distribute' language c;

grant usage on schema @extschema@ to pmpp;
grant execute on all functions in schema @extschema@ to pmpp;
