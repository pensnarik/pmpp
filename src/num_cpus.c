#include "postgres.h"
#include "fmgr.h"
#include <unistd.h>

PG_FUNCTION_INFO_V1(pmpp_num_cpus);

Datum
pmpp_num_cpus(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32((int)sysconf(_SC_NPROCESSORS_ONLN));
}

