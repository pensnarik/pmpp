
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

drop function distribute( row_type anyelement,
                            connection text,
                            sql_list text[],
                            cpu_multiplier float default null, 
                            num_workers integer default null,
                            setup_commands text[] default null);

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

drop function meta(   connection text,
                        sql_list text[],
                        cpu_multiplier float default null,
                        num_workers integer default null,
                        setup_commands text[] default null);

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
 
