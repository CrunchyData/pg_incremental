#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "catalog/dependency.h"
#include "catalog/pg_authid.h"
#include "crunchy/incremental/pipeline.h"
#include "crunchy/incremental/time_interval.h"
#include "crunchy/incremental/init.h"
#include "executor/spi.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"


/*
 * TimeIntervalRange represents a time range that can
 * be safely processed.
 */
typedef struct TimeIntervalRange
{
	TimestampTz rangeStart;
	TimestampTz rangeEnd;

	Interval   *interval;
	bool		batched;
}			TimeIntervalRange;


static void ExecuteTimeIntervalPipelineForRange(char *pipelineName, char *command,
												TimestampTz rangeStart, TimestampTz rangeEnd);
static TimeIntervalRange * PopTimeIntervalRange(char *pipelineName,
												Oid relationId);
static TimeIntervalRange * GetSafeTimeIntervalRange(char *pipelineName);


/*
 * InitializeSequencePipelineStats adds the initial time interval pipeline state.
 */
void
InitializeTimeRangePipelineState(char *pipelineName,
								 bool batched,
								 TimestampTz startTime,
								 Interval *timeInterval,
								 Interval *minDelay)
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
		"insert into incremental.time_interval_pipelines "
		"(pipeline_name, batched, last_processed_time, time_interval, min_delay) "
		"values ($1, $2, $3, $4, $5)";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 5;
	Oid			argTypes[] = {TEXTOID, BOOLOID, TIMESTAMPTZOID, INTERVALOID, INTERVALOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		BoolGetDatum(batched),
		TimestampTzGetDatum(startTime),
		IntervalPGetDatum(timeInterval),
		IntervalPGetDatum(minDelay)
	};
	char		argNulls[] = "     ";

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
 * ExecuteTimeIntervalPipeline executes a time interval pipeline from
 * the last processed time up to the end of the most recent time interval.
 */
void
ExecuteTimeIntervalPipeline(char *pipelineName, char *command)
{
	PipelineDesc *pipelineDesc = ReadPipelineDesc(pipelineName);

	/* get the full range of data to process */
	TimeIntervalRange *range = PopTimeIntervalRange(pipelineName, pipelineDesc->sourceRelationId);

	if (range->rangeStart >= range->rangeEnd)
	{
		if (IncrementalLogStatement)
		{
			ereport(NOTICE, (errmsg("pipeline %s: no rows to process",
								pipelineName)));
		}
		return;
	}

	if (range->batched)
	{
		ExecuteTimeIntervalPipelineForRange(pipelineName, command,
											range->rangeStart, range->rangeEnd);
	}
	else
	{
		Datum		rangeStartDatum = TimestampTzGetDatum(range->rangeStart);
		Datum		rangeEndDatum = TimestampTzGetDatum(range->rangeEnd);

		char	   *rangeStartStr =
			DatumGetCString(DirectFunctionCall1(timestamptz_out, rangeStartDatum));
		char	   *rangeEndStr =
			DatumGetCString(DirectFunctionCall1(timestamptz_out, rangeEndDatum));

		if (IncrementalLogStatement)
		{
			ereport(NOTICE, (errmsg("pipeline %s: processing overall range from %s to %s",
								pipelineName, rangeStartStr, rangeEndStr)));
		}

		TimestampTz nextStart = range->rangeStart;

		/* while the next start is smaller than the range end */
		while (TimestampDifferenceMilliseconds(nextStart, range->rangeEnd) > 0)
		{
			/*
			 * start at the end of the last interval (or overall start of the
			 * range)
			 */
			TimestampTz currentStart = nextStart;

			/* end of time is start_time + interval (exclusive) */
			Datum		currentEndDatum =
				DirectFunctionCall2(timestamptz_pl_interval,
									TimestampTzGetDatum(currentStart),
									IntervalPGetDatum(range->interval));

			TimestampTz currentEnd = DatumGetTimestampTz(currentEndDatum);

			/* execute the pipeline */
			ExecuteTimeIntervalPipelineForRange(pipelineName, command,
												currentStart, currentEnd);

			/* next interval starts at the end of this one */
			nextStart = currentEnd;
		}
	}
}


/*
 * ExecuteTimeIntervalPipelineForRange executes a time interval pipeline for
 * the given time range.
 */
static void
ExecuteTimeIntervalPipelineForRange(char *pipelineName, char *command,
									TimestampTz rangeStart, TimestampTz rangeEnd)
{
	Datum		rangeStartDatum = TimestampTzGetDatum(rangeStart);
	Datum		rangeEndDatum = TimestampTzGetDatum(rangeEnd);

	char	   *rangeStartStr =
		DatumGetCString(DirectFunctionCall1(timestamptz_out, rangeStartDatum));
	char	   *rangeEndStr =
		DatumGetCString(DirectFunctionCall1(timestamptz_out, rangeEndDatum));

	ereport(NOTICE, (errmsg("pipeline %s: processing time range from %s to %s",
							pipelineName, rangeStartStr, rangeEndStr)));

	PushActiveSnapshot(GetTransactionSnapshot());

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 2;
	Oid			argTypes[] = {TIMESTAMPTZOID, TIMESTAMPTZOID};
	Datum		argValues[] = {
		rangeStartDatum,
		rangeEndDatum
	};
	char	   *argNulls = "  ";

	SPI_connect();
	SPI_execute_with_args(command,
						  argCount,
						  argTypes,
						  argValues,
						  argNulls,
						  readOnly,
						  tupleCount);
	SPI_finish();

	PopActiveSnapshot();
}


/*
 * PopTimeInterval range returns a range of time range that can
 * be safely processed by taking the last returned sequence number as the
 * end of the range, and waiting for all concurrent writers to finish.
 *
 * The start of the range is the end of the previous range + 1.
 *
 * Note: An assumptions is that writers will only insert sequence numbers
 * that were obtained after locking the table.
 */
static TimeIntervalRange *
PopTimeIntervalRange(char *pipelineName, Oid relationId)
{
	TimeIntervalRange *range = GetSafeTimeIntervalRange(pipelineName);

	if (range->rangeStart < range->rangeEnd)
	{
		LOCKTAG		tableLockTag;

		SET_LOCKTAG_RELATION(tableLockTag, MyDatabaseId, relationId);

		/*
		 * Wait for concurrent writers that may have seen now() results lower
		 * than the start of the time range.
		 */
		WaitForLockers(tableLockTag, ShareLock, true);

		/*
		 * We update the last-processed time interval, which will commit or
		 * abort with the current (sub)transaction.
		 */
		UpdateLastProcessedTimeInterval(pipelineName, range->rangeEnd);
	}

	return range;
}


/*
 * GetSafeTimeIntervalRange reads the current state of the given sequence pipeline
 * and returns whether there are rows to process.
 */
static TimeIntervalRange *
GetSafeTimeIntervalRange(char *pipelineName)
{
	TimeIntervalRange *range = (TimeIntervalRange *) palloc0(sizeof(TimeIntervalRange));

	range->interval = palloc0(sizeof(Interval));

	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have write
	 * privileges for the pipelines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Get the last-processed timestamp and the last time interval end that
	 * precedes the current time minus minDelay.
	 */
	char	   *query =
		"select"
		" last_processed_time,"
		" pg_catalog.date_bin(time_interval, now() - min_delay, '2001-01-01'),"
		" time_interval,"
		" batched "
		"from incremental.time_interval_pipelines "
		"where pipeline_name operator(pg_catalog.=) $1 "
		"for update";

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

	if (SPI_processed <= 0)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("pipeline \"%s\" cannot be found",
							   pipelineName)));

	TupleDesc	rowDesc = SPI_tuptable->tupdesc;
	HeapTuple	row = SPI_tuptable->vals[0];

	/* read last_processed_time result */
	bool		isNull = false;
	Datum		rangeStartDatum = SPI_getbinval(row, rowDesc, 1, &isNull);

	if (!isNull)
		range->rangeStart = DatumGetTimestampTz(rangeStartDatum);

	/* read current time, rounded down by time interval */
	Datum		rangeEndDatum = SPI_getbinval(row, rowDesc, 2, &isNull);

	range->rangeEnd = DatumGetTimestampTz(rangeEndDatum);

	Datum		intervalDatum = SPI_getbinval(row, rowDesc, 3, &isNull);

	memcpy(range->interval, DatumGetIntervalP(intervalDatum), sizeof(Interval));

	Datum		batchedDatum = SPI_getbinval(row, rowDesc, 4, &isNull);

	range->batched = DatumGetBool(batchedDatum);

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	/* return whether there are rows to process */
	return range;
}


/*
 * UpdateLastProcessedTimeInterval updates the last_processed_time
 * in pipeline.time_interval_pipelines to the given value to indicate which
 * intervals have been processed.
 */
void
UpdateLastProcessedTimeInterval(char *pipelineName, TimestampTz lastProcessedInterval)
{
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have write
	 * privileges for the pipleines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Get the last-drawn sequence number, which may be part of a write that
	 * has not committed yet. Also block other pipeline rollups.
	 */
	char	   *query =
		"update incremental.time_interval_pipelines "
		"set last_processed_time = $2 "
		"where pipeline_name operator(pg_catalog.=) $1";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 2;
	Oid			argTypes[] = {TEXTOID, TIMESTAMPTZOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		TimestampTzGetDatum(lastProcessedInterval)
	};
	char	   *argNulls = "  ";

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
						errmsg("pipeline \"%s\" cannot be found",
							   pipelineName)));

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}
