
drop type query_manifest cascade;

create type query_manifest as (
    connection text,
    queries text[],
    cpu_multiplier float,
    num_workers integer,
    setup_commands text[]
);

drop function if exists num_cpus(float);
drop function if exists num_cpus_remote(text,float);
drop function if exists disconnect();

create or replace function num_cpus() returns integer
as 'MODULE_PATHNAME','num_cpus'
language c immutable strict;

create or replace function distribute(  row_type anyelement,
                                        query_manifest query_manifest[] )
                                        returns setof anyelement
as 'MODULE_PATHNAME','distribute'
language c;

-- added setup_commands
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
        end as setup_commands;
$$;

comment on function to_query_manifest(item in jsonb)
is  'Convert a JSONB manifest item to a query_manifest.\n'
    'This includes support for mapping statement_timeout into the setup_commands array';

-- added default null on cpu_multiplier, switch to setup_commands
create or replace function distribute( row_type anyelement,
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

create or replace function meta(   connection text,
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

drop function execute_command(sql text);

