
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

