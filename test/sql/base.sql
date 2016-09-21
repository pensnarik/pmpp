--
-- roles must exist on both sides of the connection so they cannot be created in a transaction
--
\set pmpp_test_user 'pmpp_test_user'
\set password 'dummy';
\set nopw_test_user 'nopw_test_user'
create role :pmpp_test_user password :'password' login;
create role :nopw_test_user password '' login;
grant pmpp to :pmpp_test_user, :nopw_test_user;
create extension postgres_fdw; -- a user-mappings convenience, not a dependency
create extension pmpp;

select sign(pmpp.num_cpus());

select  current_database() as dbname,
        'dbname=' || current_database() as loopback_su_conn_str
\gset 

\set pmpp_localhost_server 'localhost_server'

select  format('[{"connection": "%s", "query": "select 1"}]',
                :'loopback_su_conn_str') as json_str_1,
        format('[{"connection": "%s", "query": "select 999"}]',
                :'pmpp_localhost_server') as json_str_2,
        format('[{"connection": "%s", "query": "select count(pg_sleep(1))", "statement_timeout": 100}]',
                :'pmpp_localhost_server') as json_str_timeout,
        format('[{"connection": "%s", "num_workers": 1, "queries": ["select 1","select 2/0","select 3"]}]',
                :'pmpp_localhost_server') as json_str_div_zero,
        format('[{"connection": "%s", "num_workers": 1, "queries": ["select 1","select 2","select 3"],'
                    ' "result_format": "binary" }]',
                :'pmpp_localhost_server') as json_str_binary_mode
\gset 

select pmpp.to_query_manifest('{"connection": "foo"}'::jsonb);

create table parallel_index_test( b integer, c integer, d integer );
grant all on parallel_index_test to public;

insert into parallel_index_test
select  b.b, c.c, d.d
from    generate_series(1,10) as b,
        generate_series(1,10) as c,
        generate_series(1,10) as d;

select * from parallel_index_test order by 1,2,3 limit 10;

select  b, sum(c), sum(d)
from    parallel_index_test
group by b
order by b;

--
-- test cast of array which tests cast of pmpp.query_manifest as well
--
select  t.*
from    unnest(:'json_str_1'::jsonb::pmpp.query_manifest[]) t;

create temporary table x(y integer);

select *
from    pmpp.distribute(null::x,:'loopback_su_conn_str',array['select 1','select 2','select 3'])
order by y;

select  *
from    pmpp.manifest_set(:'json_str_1'::jsonb);

select  queries
from    pmpp.manifest_set(:'json_str_1'::jsonb);

select  *
from    pmpp.manifest_set(:'json_str_div_zero'::jsonb);

select  *
from    pmpp.manifest_set(:'json_str_timeout'::jsonb);

create temporary table r as
select  array_agg(t) r
from    pmpp.manifest_set(:'json_str_1'::jsonb) as t;

select *
from    pmpp.distribute(null::x, :'json_str_1'::jsonb);

--
-- test meta() which is just distribute() with a specific result set
--
select  *
from    pmpp.meta('dbname=' || current_database(), 
                    array(  select format('create index on %s(%s)',c.table_name,c.column_name)
                            from    information_schema.columns c
                            where   c.table_name = 'parallel_index_test'
                            and     c.table_schema = 'public'),
                    setup_commands := array ['set application_name = indexer','set client_encoding = UTF8'])
order by 1;

\d+ parallel_index_test

select  b, sum(c), sum(d)
from    pmpp.distribute(null::parallel_index_test,
                        'dbname=' || current_database(),
                        array(  select  format('select %s, sum(c), sum(d) from parallel_index_test where b = %s',b.b,b.b)
                                from    generate_series(1,10) as b ))
group by b
order by b;





create server :pmpp_localhost_server foreign data wrapper postgres_fdw
options (host 'localhost', dbname :'dbname' );

create user mapping for public server :pmpp_localhost_server options(user :'pmpp_test_user', password :'password');

--
-- test normal legit queryset
--
select *
from    pmpp.distribute(null::x,:'pmpp_localhost_server',array['select 1','select 2','select 3']);

--
-- test result set with too many columns
--
select *
from    pmpp.distribute(null::x,:'pmpp_localhost_server',array['select 1','select 2','select 3, 4']);

--
-- test result set with right number of columns but wrong type
--
select *
from    pmpp.distribute(null::x,:'pmpp_localhost_server',array['select 1','select ''2016-01-01''::date','select 3']);


--
-- test legit json input
--
select *
from    pmpp.distribute(null::x, :'json_str_2'::jsonb);

--
-- test a query that times out
--
select *
from    pmpp.distribute(null::x, :'json_str_timeout'::jsonb);


select *
from pmpp.broadcast(null::x, array[ :'loopback_su_conn_str' , :'pmpp_localhost_server' ], 'select 1');

select *
from    pmpp.distribute(null::x, :'json_str_div_zero'::jsonb);

select  *
from    pmpp.distribute(null::x, :'json_str_binary_mode'::jsonb);


/*
drop user mapping for public server :pmpp_localhost_server;
create user mapping for public server :pmpp_localhost_server options(user :'nopw_test_user', password '');

select *
from    pmpp.distribute(null::x, :'json_str_2'::jsonb);
*/


select *
from    pmpp.distribute(null::x, '[{"connection": "bad_connstr_1", "query": "select 1"},'
                                 '{"connection": "bad_connstr2", "query": "select 2", '
                                 '  "setup_commands": ["set application_name = test1", "set client_encoding = UTF8" ] }]'::jsonb); 

drop role :pmpp_test_user;
drop role :nopw_test_user;

