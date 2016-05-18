
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

-- added default null on cpu_multiplier
create or replace function distribute( row_type anyelement,
                            connection text,
                            sql_list text[],
                            cpu_multiplier float default null, 
                            num_workers integer default null,
                            statement_timeout integer default null)
                            returns setof anyelement
language sql security definer set search_path from current 
as $$
select  distribute(row_type,
                    array[ row(connection,sql_list,cpu_multiplier,num_workers,statement_timeout)::query_manifest ]);
$$;

create or replace function meta(   connection text,
                        sql_list text[],
                        cpu_multiplier float default null,
                        num_workers integer default null,
                        statement_timeout integer default null) returns setof command_with_result
language sql security definer set search_path from current as $$
select  *
from    distribute( null::command_with_result,
                    connection,
                    sql_list,
                    cpu_multiplier,
                    num_workers,
                    statement_timeout);
$$;

drop function execute_command(sql text);

