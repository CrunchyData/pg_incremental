#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "catalog/dependency.h"
#include "catalog/pg_authid.h"
#include "crunchy/incremental/cron.h"
#include "crunchy/incremental/pipeline.h"
#include "crunchy/incremental/query.h"
#include "crunchy/incremental/sequence.h"
#include "crunchy/incremental/time_interval.h"
#include "executor/spi.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/ruleutils.h"

static void InsertPipeline(char *pipelineName, PipelineType pipelineType, Oid sourceRelationId,
						   char *command);
static void EnsurePipelineOwner(char *pipelineName, Oid ownerId);
static void ExecutePipeline(char *pipelineName, PipelineType pipelineType, char *command);
static void ResetPipeline(char *pipelineName, PipelineType pipelineType);
static void DeletePipeline(char *pipelineName);
static char *GetCronJobNameForPipeline(char *pipelineName);
static char *GetCronCommandForPipeline(char *pipelineName);


PG_FUNCTION_INFO_V1(incremental_create_sequence_pipeline);
PG_FUNCTION_INFO_V1(incremental_create_time_interval_pipeline);
PG_FUNCTION_INFO_V1(incremental_execute_pipeline);
PG_FUNCTION_INFO_V1(incremental_reset_pipeline);
PG_FUNCTION_INFO_V1(incremental_drop_pipeline);


/*
 * incremental_create_sequence_pipeline creates a new pipeline that tracks
 * a sequence.
 */
Datum
incremental_create_sequence_pipeline(PG_FUNCTION_ARGS)
{
	/*
	 * create_sequence_pipeline is not strict because the last argument can be
	 * NULL, so check the arguments that cannot be NULL.
	 */
	if (PG_ARGISNULL(0))
		ereport(ERROR, (errmsg("pipeline_name cannot be NULL")));
	if (PG_ARGISNULL(1))
		ereport(ERROR, (errmsg("sequence_name cannot be NULL")));
	if (PG_ARGISNULL(2))
		ereport(ERROR, (errmsg("command cannot be NULL")));

	char	   *pipelineName = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid			sequenceId = PG_GETARG_OID(1);
	char	   *command = text_to_cstring(PG_GETARG_TEXT_P(2));
	char	   *schedule = PG_ARGISNULL(3) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(3));
	bool		executeImmediately = PG_ARGISNULL(4) ? false : PG_GETARG_BOOL(4);

	Oid			sourceRelationId = InvalidOid;

	switch (get_rel_relkind(sequenceId))
	{
		case RELKIND_SEQUENCE:
			{
				int32		columnNumber = 0;

				if (!sequenceIsOwned(sequenceId, DEPENDENCY_AUTO, &sourceRelationId, &columnNumber))
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("enly sequences that are owned by a table are supported")));
				}

				break;
			}

		case RELKIND_RELATION:
		case RELKIND_FOREIGN_TABLE:
		case RELKIND_PARTITIONED_TABLE:
			{
				sourceRelationId = sequenceId;

				/* user entered a table name, see if it has a single sequence */
				sequenceId = FindSequenceForRelation(sourceRelationId);
				break;
			}

		default:
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("%s is not a table or sequence", get_rel_name(sequenceId))));
			}
	}

	List *paramTypes = list_make2_oid(INT8OID, INT8OID);

	/* sanitize the query */
	Query	   *parsedQuery = ParseQuery(command, paramTypes);
	char	   *sanitizedCommand = DeparseQuery(parsedQuery);

	InsertPipeline(pipelineName, SEQUENCE_RANGE_PIPELINE, sourceRelationId, sanitizedCommand);
	InitializeSequencePipelineState(pipelineName, sequenceId);

	if (executeImmediately)
		ExecutePipeline(pipelineName, SEQUENCE_RANGE_PIPELINE, sanitizedCommand);

	if (schedule != NULL)
	{
		char	   *jobName = GetCronJobNameForPipeline(pipelineName);
		char	   *cronCommand = GetCronCommandForPipeline(pipelineName);

		int64		jobId = ScheduleCronJob(jobName, schedule, cronCommand);

		ereport(NOTICE, (errmsg("pipeline %s: scheduled cron job with ID " INT64_FORMAT
								" and schedule %s",
								pipelineName, jobId, schedule)));
	}

	PG_RETURN_VOID();
}


/*
 * incremental_create_time_interval_pipeline creates a new pipeline that processes
 * time ranges.
 */
Datum
incremental_create_time_interval_pipeline(PG_FUNCTION_ARGS)
{
	/*
	 * create_time_range_pipeline is not strict because the last argument can be
	 * NULL, so check the arguments that cannot be NULL.
	 */
	if (PG_ARGISNULL(0))
		ereport(ERROR, (errmsg("pipeline_name cannot be NULL")));
	if (PG_ARGISNULL(1))
		ereport(ERROR, (errmsg("time_interval cannot be NULL")));
	if (PG_ARGISNULL(2))
		ereport(ERROR, (errmsg("command cannot be NULL")));
	if (PG_ARGISNULL(7))
		ereport(ERROR, (errmsg("min_delay cannot be NULL")));

	char	   *pipelineName = text_to_cstring(PG_GETARG_TEXT_P(0));
	Interval   *timeInterval = PG_GETARG_INTERVAL_P(1);
	char	   *command = text_to_cstring(PG_GETARG_TEXT_P(2));
	bool		batched = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
	TimestampTz startTime = PG_ARGISNULL(4) ? 0 : PG_GETARG_TIMESTAMPTZ(4);
	Oid			relationId = PG_ARGISNULL(5) ? InvalidOid : PG_GETARG_OID(5);
	char	   *schedule = PG_ARGISNULL(6) ? NULL : text_to_cstring(PG_GETARG_TEXT_P(6));
	Interval   *minDelay = PG_GETARG_INTERVAL_P(7);
	bool		executeImmediately = PG_ARGISNULL(8) ? false : PG_GETARG_BOOL(8);

	if (!batched && PG_ARGISNULL(4))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("start_time is required for non-batched pipelines"),
						errdetail("Non-batched pipelines are executed for every interval "
								  "starting from the start_time")));
	}

	List *paramTypes = list_make2_oid(TIMESTAMPTZOID, TIMESTAMPTZOID);

	/* validate and sanitize the query */
	Query	   *parsedQuery = ParseQuery(command, paramTypes);
	char	   *sanitizedCommand = DeparseQuery(parsedQuery);

	InsertPipeline(pipelineName, TIME_INTERVAL_PIPELINE, relationId, sanitizedCommand);
	InitializeTimeRangePipelineState(pipelineName, batched, startTime, timeInterval, minDelay);

	if (executeImmediately)
		ExecutePipeline(pipelineName, TIME_INTERVAL_PIPELINE, sanitizedCommand);

	if (schedule != NULL)
	{
		char	   *jobName = GetCronJobNameForPipeline(pipelineName);
		char	   *cronCommand = GetCronCommandForPipeline(pipelineName);

		int64		jobId = ScheduleCronJob(jobName, schedule, cronCommand);

		ereport(NOTICE, (errmsg("pipeline %s: scheduled cron job with ID " INT64_FORMAT
								" and schedule %s",
								pipelineName, jobId, schedule)));
	}

	PG_RETURN_VOID();
}


/*
 * incremental_execute_pipeline executes a pipeline to its initial state.
 */
Datum
incremental_execute_pipeline(PG_FUNCTION_ARGS)
{
	char	   *pipelineName = text_to_cstring(PG_GETARG_TEXT_P(0));
	PipelineDesc *pipelineDesc = ReadPipelineDesc(pipelineName);

	EnsurePipelineOwner(pipelineName, pipelineDesc->ownerId);
	ExecutePipeline(pipelineName, pipelineDesc->pipelineType, pipelineDesc->command);

	PG_RETURN_VOID();
}


/*
 * incremental_reset_pipeline reset a pipeline to its initial state.
 */
Datum
incremental_reset_pipeline(PG_FUNCTION_ARGS)
{
	char	   *pipelineName = text_to_cstring(PG_GETARG_TEXT_P(0));
	PipelineDesc *pipelineDesc = ReadPipelineDesc(pipelineName);

	EnsurePipelineOwner(pipelineName, pipelineDesc->ownerId);
	ResetPipeline(pipelineName, pipelineDesc->pipelineType);

	PG_RETURN_VOID();
}


/*
 * incremental_drop_pipeline drops a pipeline by name.
 */
Datum
incremental_drop_pipeline(PG_FUNCTION_ARGS)
{
	char	   *pipelineName = text_to_cstring(PG_GETARG_TEXT_P(0));
	PipelineDesc *pipelineDesc = ReadPipelineDesc(pipelineName);

	EnsurePipelineOwner(pipelineName, pipelineDesc->ownerId);
	DeletePipeline(pipelineName);

	UnscheduleCronJob(GetCronJobNameForPipeline(pipelineName));

	PG_RETURN_VOID();
}


/*
 * InsertPipeline adds a new pipeline.
 */
static void
InsertPipeline(char *pipelineName, PipelineType pipelineType, Oid sourceRelationId,
			   char *command)
{
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have write
	 * privileges for the pipelines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	char	   *query =
		"insert into incremental.pipelines "
		"(pipeline_name, pipeline_type, owner_id, source_relation, command) "
		"values ($1, $2, $3, $4, $5)";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 5;
	Oid			argTypes[] = {TEXTOID, CHAROID, OIDOID, OIDOID, TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		CharGetDatum(pipelineType),
		ObjectIdGetDatum(savedUserId),
		ObjectIdGetDatum(sourceRelationId),
		CStringGetTextDatum(command)
	};
	char	   *argNulls = "     ";

	SPI_connect();
	SPI_execute_with_args(query,
						  argCount,
						  argTypes,
						  argValues,
						  argNulls,
						  readOnly,
						  tupleCount);
	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * ReadPipelineCommand returns a full description of a pipeline.
 */
PipelineDesc *
ReadPipelineDesc(char *pipelineName)
{
	PipelineDesc *pipelineDesc = (PipelineDesc *) palloc0(sizeof(PipelineDesc));

	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have read
	 * privileges for the pipelines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	MemoryContext callerContext = CurrentMemoryContext;

	char	   *query =
		"select pipeline_type, owner_id, source_relation, command "
		"from incremental.pipelines "
		"where pipeline_name operator(pg_catalog.=) $1";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 1;
	Oid			argTypes[] = {TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName)
	};
	char	   *argNulls = " ";

	/* we do not switch user, because we want to preserve permissions */

	SPI_connect();
	SPI_execute_with_args(query,
						  argCount,
						  argTypes,
						  argValues,
						  argNulls,
						  readOnly,
						  tupleCount);

	if (SPI_processed <= 0)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("no such pipeline named \"%s\"", pipelineName)));

	TupleDesc	rowDesc = SPI_tuptable->tupdesc;
	HeapTuple	row = SPI_tuptable->vals[0];

	bool		isNull = false;
	Datum		pipelineTypeDatum = SPI_getbinval(row, rowDesc, 1, &isNull);
	Datum		ownerIdDatum = SPI_getbinval(row, rowDesc, 2, &isNull);
	Datum		sourceRelationDatum = SPI_getbinval(row, rowDesc, 3, &isNull);
	Datum		commandDatum = SPI_getbinval(row, rowDesc, 4, &isNull);

	MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

	pipelineDesc->pipelineName = pstrdup(pipelineName);
	pipelineDesc->pipelineType = DatumGetChar(pipelineTypeDatum);
	pipelineDesc->ownerId = DatumGetObjectId(ownerIdDatum);
	pipelineDesc->sourceRelationId = DatumGetObjectId(sourceRelationDatum);
	pipelineDesc->command = TextDatumGetCString(commandDatum);

	MemoryContextSwitchTo(spiContext);

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return pipelineDesc;
}



/*
 * EnsurePipelineOwner throws an error if the current user is not
 * superuser and not the pipeline owner.
 */
static void
EnsurePipelineOwner(char *pipelineName, Oid ownerId)
{
	if (superuser())
		return;

	if (ownerId != GetUserId())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied for pipeline %s", pipelineName)));
}


/*
 * ExecutePipeline executes a pipeline.
 */
static void
ExecutePipeline(char *pipelineName, PipelineType pipelineType, char *command)
{
	switch (pipelineType)
	{
		case SEQUENCE_RANGE_PIPELINE:
			ExecuteSequenceRangePipeline(pipelineName, command);
			break;

		case TIME_INTERVAL_PIPELINE:
			ExecuteTimeIntervalPipeline(pipelineName, command);
			break;

		default:
			elog(ERROR, "unknown pipeline type: %c", pipelineType);
	}
}


/*
 * ResetPipeline reset a pipeline to its initial state.
 */
static void
ResetPipeline(char *pipelineName, PipelineType pipelineType)
{
	switch (pipelineType)
	{
		case SEQUENCE_RANGE_PIPELINE:
			UpdateLastProcessedSequenceNumber(pipelineName, 0);
			break;

		case TIME_INTERVAL_PIPELINE:
			UpdateLastProcessedTimeInterval(pipelineName, 0);
			break;

		default:
			elog(ERROR, "unknown pipeline type: %c", pipelineType);
	}
}


/*
 * DeletePipeline removes a pipeline from the pipeline.pipelines table.
 */
static void
DeletePipeline(char *pipelineName)
{
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have write
	 * privileges for the pipelines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	char	   *query =
		"delete from incremental.pipelines "
		"where pipeline_name operator(pg_catalog.=) $1";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 1;
	Oid			argTypes[] = {TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName)
	};
	char	   *argNulls = " ";

	SPI_connect();
	SPI_execute_with_args(query,
						  argCount,
						  argTypes,
						  argValues,
						  argNulls,
						  readOnly,
						  tupleCount);

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * GetCronJobNameForPipeline returns the name of the cron job to use for a given pipeline.
 */
static char *
GetCronJobNameForPipeline(char *pipelineName)
{
	return psprintf("pipeline:%s", pipelineName);
}


/*
 * GetCronCommandForPipeline returns the command of the cron job to use for a given pipeline.
 */
static char *
GetCronCommandForPipeline(char *pipelineName)
{
	return psprintf("call incremental.execute_pipeline(%s)",
					quote_literal_cstr(pipelineName));
}
