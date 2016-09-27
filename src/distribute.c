#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h" /* for work_mem */
#include "access/htup_details.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "parser/scansup.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/typcache.h"
#include "libpq-fe.h"
#include <math.h>


#define QUERY_MANIFEST_CONNECTION_ATTR_INDEX 0
#define QUERY_MANIFEST_QUERIES_ATTR_INDEX 1
#define QUERY_MANIFEST_CPU_MULTIPLIER_ATTR_INDEX 2
#define QUERY_MANIFEST_NUM_WORKERS_ATTR_INDEX 3
#define QUERY_MANIFEST_SETUP_COMMANDS_ATTR_INDEX 4

#define ARRAY_OK 0
#define ARRAY_CONTAINS_NULLS 1

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif



typedef struct {
	PGconn	*connection;
	char	*connstr;
	char	*current_query;
} worker;

/*
	query_manifest
	--------------

	num_workers:
		The number of elements in the workers array.
	num_queries:
		The number of elements in the queries array.
	num_setup_commands:
		The number of elements in the setup_commands array.
	next_query:
		Index of the first un-executed query in the queries array.
	cpu_multiplier:
		If the remote system can reveal how many CPUs it has, that number is multiplied by cpu_multiplier
		to give the number of connections to create. The number of connections created will not be below 1
		and will not exceed the number of elements in the queries array.
	connection_string:
		The connection string given to us by the user, the one that should be used in error messages
	resolved_connection_string:
		The actual string to be sent to libpq. This will be the same value as connection_string except in cases
		where connection_string references a foreign server. In that case, it is the string derived from the
		foreign server and user mapping.
	workers:
		The worker processes that will run the queries given.
	queries:
		All of the queries to be sent to the worker(s) that will use this connection string.
	setup_commands:
		A list of commands (set application_name = ..., set session_timeout = ..., etc) that must be executed
		by each connection prior to executing any queries in the queries attribute.
*/
 
typedef struct {
	int		num_workers;
	int		num_queries;
	int		num_setup_commands;
	int		next_query;
	float	cpu_multiplier;
	char	*connection_string;
	char	*resolved_connection_string;
	worker	*workers;
	char	**queries;
	char	**setup_commands;
} query_manifest;

/*
 * Escaping libpq connect parameter strings.
 *
 * Replaces "'" with "\'" and "\" with "\\".
 * copied from dblink.c
 */
static char *
escape_param_str(const char *str)
{
	const char *cp;
	StringInfo	buf = makeStringInfo();

	for (cp = str; *cp; cp++)
	{
		if (*cp == '\\' || *cp == '\'')
			appendStringInfoChar(buf, '\\');
		appendStringInfoChar(buf, *cp);
	}

	return buf->data;
}

/*
 * Obtain connection string for a foreign server
 * copied from dblink.c
 */
static char *
get_connect_string(const char *servername)
{
	ForeignServer *foreign_server = NULL;
	UserMapping *user_mapping;
	ListCell   *cell;
	StringInfo	buf = makeStringInfo();
	ForeignDataWrapper *fdw;
	AclResult	aclresult;
	char	   *srvname;

	/* first gather the server connstr options */
	srvname = pstrdup(servername);
	truncate_identifier(srvname, strlen(srvname), false);
	foreign_server = GetForeignServerByName(srvname, true);

	if (foreign_server)
	{
		Oid			serverid = foreign_server->serverid;
		Oid			fdwid = foreign_server->fdwid;
		Oid			userid = GetUserId();

		user_mapping = GetUserMapping(userid, serverid);
		fdw = GetForeignDataWrapper(fdwid);

		/* Check permissions, user must have usage on the server. */
		aclresult = pg_foreign_server_aclcheck(serverid, userid, ACL_USAGE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_FOREIGN_SERVER, foreign_server->servername);

		foreach(cell, fdw->options)
		{
			DefElem    *def = lfirst(cell);

			appendStringInfo(buf, "%s='%s' ", def->defname,
							 escape_param_str(strVal(def->arg)));
		}

		foreach(cell, foreign_server->options)
		{
			DefElem    *def = lfirst(cell);

			appendStringInfo(buf, "%s='%s' ", def->defname,
							 escape_param_str(strVal(def->arg)));
		}

		foreach(cell, user_mapping->options)
		{

			DefElem    *def = lfirst(cell);

			appendStringInfo(buf, "%s='%s' ", def->defname,
							 escape_param_str(strVal(def->arg)));
		}

		return buf->data;
	}
	else
		return NULL;
}


/*
 * Callback function which is called when error occurs during fetch of 
 * results from a remote worker.
 */
static void
worker_error_callback(void *arg)
{
	worker *w = (worker *) arg;
	errcontext("query: %s on connection to: %s", w->current_query, w->connstr);
}

/*
 * report errors/notices
 * adapted from dblink_res_error()
 */
static void
res_error(PGresult *res, const char *connstr, const char *querystr, bool fail)
{
	char	   *pg_diag_sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
	char	   *pg_diag_message_primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
	char	   *pg_diag_message_detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
	char	   *pg_diag_message_hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);
	char	   *pg_diag_context = PQresultErrorField(res, PG_DIAG_CONTEXT);
	char	   *message_primary = (pg_diag_message_primary != NULL) ? pstrdup(pg_diag_message_primary) : NULL;
	char	   *message_detail = (pg_diag_message_detail != NULL) ? pstrdup(pg_diag_message_detail) : NULL;
	char	   *message_hint = (pg_diag_message_hint != NULL) ? pstrdup(pg_diag_message_hint) : NULL;
	char	   *message_context = (pg_diag_context != NULL) ? pstrdup(pg_diag_context) : NULL;
	int			sqlstate;

	if (pg_diag_sqlstate)
	{
		sqlstate = MAKE_SQLSTATE(pg_diag_sqlstate[0],
								 pg_diag_sqlstate[1],
								 pg_diag_sqlstate[2],
								 pg_diag_sqlstate[3],
								 pg_diag_sqlstate[4]);
	}
	else
	{
		sqlstate = ERRCODE_CONNECTION_FAILURE;
	}

	if (res)
	{
		PQclear(res);
	}

	ereport(fail ? ERROR : NOTICE,
			(errcode(sqlstate),
			 message_primary ? errmsg_internal("%s", message_primary) :
			 errmsg("unknown error"),
			 message_detail ? errdetail_internal("%s", message_detail) : 0,
			 message_hint ? errhint("%s", message_hint) : 0,
			 message_context ? errcontext("%s", message_context) : 0,
			errcontext("Error occurred on a connection to: %s executing query: %s.",
						connstr,querystr)));
}

/*
 * make a connection to a remote(?) database, and run the setup_commands, if any
 */
static PGconn*
make_async_connection(query_manifest* m)
{
	PGconn	*conn;
	int		i;

	conn = PQconnectdb( m->resolved_connection_string );

	if (PQstatus(conn) == CONNECTION_BAD)
	{
		char   *msg = pstrdup(PQerrorMessage(conn));
		PQfinish(conn);
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not establish connection"),
				 errdetail_internal("%s", msg)));
	}
	/* attempt to set client encoding to match server encoding, if needed */
	if (PQclientEncoding(conn) != GetDatabaseEncoding())
	{
		PQsetClientEncoding(conn, GetDatabaseEncodingName());
	}

	for ( i = 0; i < m->num_setup_commands; i++ ) 
	{
		PGresult	*result = PQexec(conn,m->setup_commands[i]);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			res_error(result,m->connection_string,m->setup_commands[i],true);
		}
		PQclear(result);
	}
	return conn;
}


static int 
num_remote_cpus(PGconn *conn, char *connstr, float cpu_multiplier)
{
	char		*num_cpus_query = "select pmpp.num_cpus()";
	PGresult	*result = PQexecParams(conn,num_cpus_query,0,NULL,NULL,NULL,NULL,1);
	int			num_remote_cpus;

	if (PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		res_error(result,connstr,num_cpus_query,true);
	}

	if ((PQnfields(result) != 1) || (PQntuples(result) != 1))
	{
		ereport(ERROR,
				 (errmsg("result set must be 1 row, 1 column. connection: %s, query: %s",connstr,num_cpus_query)));
	}

	num_remote_cpus = ntohl(*(int *)PQgetvalue(result, 0, 0));
	num_remote_cpus = (int)floor((cpu_multiplier * (float) num_remote_cpus));
	if (num_remote_cpus < 1)
	{
		num_remote_cpus = 1;
	}

	PQclear(result);

	return num_remote_cpus;
}

/*
 * unpack a datum that happens to be text[]
 */
static int
unpack_datum_to_cstring_array(Datum datum, char ***cstring_array, int *array_length)
{
	ArrayType	*array = DatumGetArrayTypeP(datum);
	Oid			oid = ARR_ELEMTYPE(array);
	int16		element_length;
	bool		element_pass_by_value;
	char		element_align;

	Datum		*datum_list;
	int			i;

	if (array_contains_nulls(array))
	{
		return ARRAY_CONTAINS_NULLS;
	}

	get_typlenbyvalalign(oid, &element_length, &element_pass_by_value, &element_align);

	/* translate array into list of Datums datum_list */
	deconstruct_array(	array,
						oid,
						element_length,
						element_pass_by_value,
						element_align,
						&datum_list,
						NULL,
						array_length);

	/* convert the list of datums into an array of c strings */
	*cstring_array = (char**) palloc0( sizeof(char*) * (*array_length) );

	for (i = 0; i < (*array_length); i++)
	{
		text *t = DatumGetTextP(datum_list[i]);
		(*cstring_array)[i] = text_to_cstring(t);
	}
	return ARRAY_OK;
}



/*
 * distribute
 */
PG_FUNCTION_INFO_V1(pmpp_distribute);

Datum
pmpp_distribute(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc		outrs_tupdesc;
	AttInMetadata	*outrs_attinmeta;
	Tuplestorestate *outrs_tupstore = NULL;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;

	/*
	 * 1st Arg is row_type, it is needed only for polymorphism, so we can ignore that
	 * 2nd Arg is the query_manifest, and must be of type query_manifest_t
	 */

	ArrayType	*query_manifest_t_param = PG_GETARG_ARRAYTYPE_P(1);
	Oid			query_manifest_t_param_oid = ARR_ELEMTYPE(query_manifest_t_param);
	int16		query_manifest_t_param_element_length;
	bool		query_manifest_t_param_element_pass_by_value;
	char		query_manifest_t_param_element_align;
	Datum		*query_manifest_t_datum_list;
	int			query_manifest_t_num_datums;
	int			i;


	TupleDesc   query_manifest_t_tupdesc = lookup_rowtype_tupdesc(query_manifest_t_param_oid,-1);
	Datum		*query_manifest_t_attr_datums = (Datum *) palloc(query_manifest_t_tupdesc->natts * sizeof(Datum));
	bool		*query_manifest_t_attr_nulls = (bool *) palloc(query_manifest_t_tupdesc->natts * sizeof(bool));

	query_manifest	*manifest, *m;
	int				total_number_of_workers = 0;

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	}

	if (!(rsinfo->allowedModes & SFRM_Materialize) || rsinfo->expectedDesc == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));
	}

	if (PG_NARGS() != 2)
	{
		ereport(ERROR,
						(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
						 errmsg("Invalid argument list")));
	}

	/* query_manifest_t_param must be one dimensional */
	if (ARR_NDIM(query_manifest_t_param) > 1)
	{
		ereport(ERROR,
						(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						 errmsg("array must be one-dimensional")));
	}

	/* query_manifest_t_param must contain no nulls */
	if (array_contains_nulls(query_manifest_t_param))
	{
		ereport(ERROR,
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						 errmsg("query_manifet array must not contain nulls")));
	}

	/* extract query_manifest_t type info from dictionary */
	get_typlenbyvalalign(	query_manifest_t_param_oid,
							&query_manifest_t_param_element_length,
							&query_manifest_t_param_element_pass_by_value,
							&query_manifest_t_param_element_align);

	/* translate query_manifest_t_param into list of Datums manifest_datum_list */
	deconstruct_array(	query_manifest_t_param,
						query_manifest_t_param_oid,
						query_manifest_t_param_element_length,
						query_manifest_t_param_element_pass_by_value,
						query_manifest_t_param_element_align,
						&query_manifest_t_datum_list,
						NULL,
						&query_manifest_t_num_datums);

	/* we now know how many elements the manifest will contain */
	manifest = (query_manifest *) palloc0( sizeof(query_manifest) * query_manifest_t_num_datums );

	PG_TRY();
	{

		for (i = 0, m = &manifest[0]; i < query_manifest_t_num_datums; i++, m++ )
		{
			HeapTupleHeader tuple_header = DatumGetHeapTupleHeader(query_manifest_t_datum_list[i]);
			HeapTupleData tuple_data;
			PGconn		*first_connection;
			worker		*cur_worker;

			/* build a HeapTupleData record */
			tuple_data.t_len = HeapTupleHeaderGetDatumLength(tuple_header);
			ItemPointerSetInvalid(&(tuple_data.t_self));
			tuple_data.t_tableOid = InvalidOid;
			tuple_data.t_data = tuple_header;

			heap_deform_tuple(&tuple_data, query_manifest_t_tupdesc,
								query_manifest_t_attr_datums, query_manifest_t_attr_nulls);

			/* attr 0: connection */
			if (query_manifest_t_attr_nulls[QUERY_MANIFEST_CONNECTION_ATTR_INDEX])
			{
				ereport(ERROR,
							(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							 errmsg("connection cannot be null")));
			}
			m->connection_string = text_to_cstring(DatumGetTextP(query_manifest_t_attr_datums[QUERY_MANIFEST_CONNECTION_ATTR_INDEX]));

			/* if the connection string was actually a foreign server name, use the credentials from that instead */
			m->resolved_connection_string = get_connect_string(m->connection_string);
			if (m->resolved_connection_string == NULL)
			{
				m->resolved_connection_string = m->connection_string;
			}

			m->cpu_multiplier = (query_manifest_t_attr_nulls[QUERY_MANIFEST_CPU_MULTIPLIER_ATTR_INDEX]) ?  (float) 1.0 :
				DatumGetFloat4(query_manifest_t_attr_datums[QUERY_MANIFEST_CPU_MULTIPLIER_ATTR_INDEX]);

			m->num_workers = (query_manifest_t_attr_nulls[QUERY_MANIFEST_NUM_WORKERS_ATTR_INDEX]) ? -1 :
				DatumGetInt32(query_manifest_t_attr_datums[QUERY_MANIFEST_NUM_WORKERS_ATTR_INDEX]);

			if (query_manifest_t_attr_nulls[QUERY_MANIFEST_QUERIES_ATTR_INDEX])
			{
				ereport(ERROR,
							(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							 errmsg("queries cannot be null")));
			}
			if ( unpack_datum_to_cstring_array( query_manifest_t_attr_datums[QUERY_MANIFEST_QUERIES_ATTR_INDEX],
												&(m->queries),
												&(m->num_queries) ) == ARRAY_CONTAINS_NULLS)
			{
				ereport(ERROR,
								(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
								 errmsg("queries array must not contain nulls")));
			}

			if (query_manifest_t_attr_nulls[QUERY_MANIFEST_SETUP_COMMANDS_ATTR_INDEX])
			{
				m->num_setup_commands = 0;
			}
			else
			{
				if ( unpack_datum_to_cstring_array( query_manifest_t_attr_datums[QUERY_MANIFEST_SETUP_COMMANDS_ATTR_INDEX],
													&(m->setup_commands),
													&(m->num_setup_commands) ) == ARRAY_CONTAINS_NULLS)
				{
					ereport(ERROR,
									(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
									 errmsg("setup_commands array must not contain nulls")));
				}
			}

			first_connection = make_async_connection(m);

			/* in cases where num_workers wasn't explicit, we must either ask or infer the number */
			if ( m->num_workers == -1 )
			{
				if ( m->num_queries == 1 )
				{
					/* only one query, no need to ask remote how many cpus it has */
					m->num_workers = 1;
				}
				else
				{
					/* ask remote how many cpus it has */
					m->num_workers = num_remote_cpus(first_connection, m->connection_string, m->cpu_multiplier);
				}
			}

			/* either way, do not allocate more workers than you have queries */
			if ( m->num_workers > m->num_queries )
			{
				m->num_workers = m->num_queries;
			}

			/* keep a running total of the number of workers we have across all manifests */
			total_number_of_workers += m->num_workers;

			m->workers = (worker*) palloc0( sizeof(worker) * m->num_workers );
			cur_worker = (worker*) m->workers;
			cur_worker->connection = first_connection;
			m->next_query = 0;

			/* loop through all connections dispatching a query with each one */
			while(1)
			{
				cur_worker->connstr = m->connection_string;
				cur_worker->current_query = m->queries[m->next_query];
				if (PQsendQuery(cur_worker->connection,cur_worker->current_query) != 1)
				{
					ereport(ERROR,
							(errmsg("errors %s sending query: %s to connection %s",
									PQerrorMessage(cur_worker->connection),
													cur_worker->current_query,
													cur_worker->connstr)));
				}

				m->next_query++;
				if (m->next_query == m->num_workers)
				{
					break;
				}
				cur_worker++;
				cur_worker->connection = make_async_connection(m);
			} 
		}
		ReleaseTupleDesc(query_manifest_t_tupdesc);

		/* let the caller know we're sending back a tuplestore */
		rsinfo->returnMode = SFRM_Materialize;
		per_query_ctx = fcinfo->flinfo->fn_mcxt;
		oldcontext = MemoryContextSwitchTo(per_query_ctx);
		outrs_tupdesc = CreateTupleDescCopy(rsinfo->expectedDesc);
		outrs_tupstore = tuplestore_begin_heap(true,false,work_mem);
		MemoryContextSwitchTo(oldcontext);

		outrs_attinmeta = TupleDescGetAttInMetadata(outrs_tupdesc);

		while(total_number_of_workers > 0)
		{
			bool		got_a_result = false;
			bool		connection_active = false;
			int			i, j;
			worker		*cur_worker;
			PGresult	*result;

			for (i = 0, m = &manifest[0]; i < query_manifest_t_num_datums; i++, m++ )
			{
				for (j = 0, cur_worker = (worker*) m->workers; j < m->num_workers; j++, cur_worker++)
				{
					/* skip connections that we've closed */
					if (cur_worker->connection != NULL)
					{
						PQconsumeInput(cur_worker->connection);
						if ((total_number_of_workers > 1) && (PQisBusy(cur_worker->connection) == 1))
						{
							/* connection is busy and there is more than one connection to wait on */
							connection_active = true;
						}
						else
						{
							/* 
							 * connection is either no longer busy, or it's the last connection so
							 * there's no point in looking elsewhere
							 */
							while ((result = PQgetResult(cur_worker->connection)) != NULL)
							{
								if (PQresultStatus(result) == PGRES_TUPLES_OK)
								{
									int nfields;
									int ntuples;

									nfields = PQnfields(result);
									ntuples = PQntuples(result);
									if (nfields != outrs_tupdesc->natts)
									{
										ereport(ERROR,
												(errcode(ERRCODE_DATATYPE_MISMATCH),
												 errmsg("result rowtype does not match expected rowtype connection: %s query: %s",
														cur_worker->connstr, cur_worker->current_query)));
									}
									
									if (ntuples > 0)
									{
										char	  **values = (char **) palloc(nfields * sizeof(char *));
										int			row;

										/* put all tuples into the tuplestore */
										for (row = 0; row < ntuples; row++)
										{
											ErrorContextCallback errcallback;
											HeapTuple	tuple;
											int			i;
											for (i = 0; i < nfields; i++)
											{
												if (PQgetisnull(result, row, i))
													values[i] = NULL;
												else
													values[i] = PQgetvalue(result, row, i);
											}
											errcallback.callback = worker_error_callback;
											errcallback.arg = (void *) cur_worker;
											errcallback.previous = error_context_stack;
											error_context_stack = &errcallback;

											/* build the tuple and put it into the tuplestore. */
											tuple = BuildTupleFromCStrings(outrs_attinmeta, values);
											error_context_stack = errcallback.previous;
											tuplestore_puttuple(outrs_tupstore, tuple);
										}
									}
								}
								else if (PQresultStatus(result) == PGRES_COMMAND_OK)
								{
									/* Non-query commands only return one row (query,status) */
									ErrorContextCallback errcallback;
									char	  **values = (char **) palloc(2 * sizeof(char *));
									HeapTuple	tuple;

									errcallback.callback = worker_error_callback;
									errcallback.arg = (void *) cur_worker;
									errcallback.previous = error_context_stack;
									error_context_stack = &errcallback;

									values[0] = cur_worker->current_query;
									values[1] = PQcmdStatus(result);
									tuple = BuildTupleFromCStrings(outrs_attinmeta, values);
									error_context_stack = errcallback.previous;
									tuplestore_puttuple(outrs_tupstore, tuple);
								}
								else
								{
									res_error(result,cur_worker->connstr,cur_worker->current_query,true);
								}
								PQclear(result);
							}
						
							got_a_result = true;
							if (m->next_query < m->num_queries)
							{
								cur_worker->current_query = m->queries[m->next_query];
								if (PQsendQuery(cur_worker->connection,cur_worker->current_query) != 1)
								{
									ereport(ERROR,
											(errmsg("errors %s sending query: %s to connection %s",
													PQerrorMessage(cur_worker->connection),
																	cur_worker->current_query,
																	cur_worker->connstr)));
								}
								m->next_query++;
								connection_active = true;
							}
							else
							{
								/* close connection and mark the worker as done */
								PQfinish(cur_worker->connection);
								cur_worker->connection = NULL;
								total_number_of_workers--;
							}

							connection_active = true;
						}
					}
				}
			}

			if (!connection_active)
			{
				break;
			}

			if (!got_a_result)
			{
				/* sleep just enough to give up the timeslice, no sense monopolizing a CPU */
				CHECK_FOR_INTERRUPTS();
			}
		}

		tuplestore_donestoring(outrs_tupstore);
		rsinfo->setResult = outrs_tupstore;
		rsinfo->setDesc = outrs_tupdesc;
		MemoryContextSwitchTo(oldcontext);
	}
	PG_CATCH();
	{
		/* 
         * attempt to cancel all active queries and disconnect all connections before
		 * re-raising the error
         */
		query_manifest	*m;
		int i,j;

		for (i = 0, m = &manifest[0]; i < query_manifest_t_num_datums; i++, m++ )
		{
			worker		*w;
			for (j = 0, w = (worker*) m->workers; j < m->num_workers; j++, w++)
			{
				if (w->connection != NULL)
				{
					PGcancel	*cancel = PQgetCancel(w->connection);
					char		errbuf[256];
					PQcancel(cancel, errbuf, 256);
					PQfreeCancel(cancel);
					PQfinish(w->connection);
				}
			}
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	return (Datum) 0;
}

