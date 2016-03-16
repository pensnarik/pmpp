#include "postgres.h"
#include "fmgr.h"
/*
#include "funcapi.h"
#include "miscadmin.h" 
#include "access/htup_details.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "parser/scansup.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/typcache.h"
#include "libpq-fe.h"
*/
#include <unistd.h>

PG_FUNCTION_INFO_V1(pmpp_num_cpus);

Datum
pmpp_num_cpus(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32((int)sysconf(_SC_NPROCESSORS_ONLN));
}

