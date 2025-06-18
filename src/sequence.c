#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "catalog/dependency.h"
#include "catalog/pg_authid.h"
#include "crunchy/incremental/pipeline.h"
#include "crunchy/incremental/sequence.h"
#include "crunchy/incremental/init.h"
#include "executor/spi.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"


/*
 * SequenceNumberRange represents a range of sequence numbers that can
 * be safely processed.
 */
typedef struct SequenceNumberRange
{
	uint64		rangeStart;
	uint64		rangeEnd;
}			SequenceNumberRange;


static SequenceNumberRange * PopSequenceNumberRange(char *pipelineName, Oid sequenceId);
static SequenceNumberRange * GetSequenceNumberRange(char *pipelineName);


PG_FUNCTION_INFO_V1(incremental_sequence_range);


/*
 * InitializeSequencePipelineStats adds the initial sequence pipeline state.
 */
void
InitializeSequencePipelineState(char *pipelineName, Oid sequenceId)
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
		"insert into incremental.sequence_pipelines "
		"(pipeline_name, sequence_name) "
		"values ($1, $2)";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 2;
	Oid			argTypes[] = {TEXTOID, OIDOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		ObjectIdGetDatum(sequenceId)
	};
	char	   *argNulls = "   ";

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
 * incremental_sequence_range determines a safe range that can be processed
 * and stores progress as part of the transaction.
 */
void
ExecuteSequenceRangePipeline(char *pipelineName, char *command)
{
	PipelineDesc *pipelineDesc = ReadPipelineDesc(pipelineName);

	SequenceNumberRange *range =
		PopSequenceNumberRange(pipelineName, pipelineDesc->sourceRelationId);

	if (range->rangeStart > range->rangeEnd)
	{
		if (IncrementalLogStatement)
		{
			ereport(NOTICE, (errmsg("pipeline %s: no rows to process",
								pipelineName)));
		}
		return;
	}

	if (IncrementalLogStatement)
	{
		ereport(NOTICE, (errmsg("pipeline %s: processing sequence values from "
							INT64_FORMAT " to " INT64_FORMAT,
							pipelineName, range->rangeStart, range->rangeEnd)));
	}

	PushActiveSnapshot(GetTransactionSnapshot());

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 2;
	Oid			argTypes[] = {INT8OID, INT8OID};
	Datum		argValues[] = {
		Int64GetDatum(range->rangeStart),
		Int64GetDatum(range->rangeEnd)
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
 * PopSequenceNumber range returns a range of sequence numbers that can
 * be safely processed by taking the last returned sequence number as the
 * end of the range, and waiting for all concurrent writers to finish.
 *
 * The start of the range is the end of the previous range + 1.
 *
 * Note: An assumptions is that writers will only insert sequence numbers
 * that were obtained after locking the table.
 */
static SequenceNumberRange *
PopSequenceNumberRange(char *pipelineName, Oid relationId)
{
	SequenceNumberRange *range = GetSequenceNumberRange(pipelineName);

	if (range->rangeStart <= range->rangeEnd)
	{
		LOCKTAG		tableLockTag;

		SET_LOCKTAG_RELATION(tableLockTag, MyDatabaseId, relationId);

		/*
		 * Wait for concurrent writers that may have seen sequence numbers <=
		 * the last-drawn sequence number.
		 */
		WaitForLockers(tableLockTag, ShareLock, true);

		/*
		 * We update the last-processed sequence number, which will commit or
		 * abort with the current (sub)transaction.
		 */
		UpdateLastProcessedSequenceNumber(pipelineName, range->rangeEnd);
	}

	return range;
}


/*
 * GetSequenceNumberRange reads the current state of the given sequence pipeline
 * and returns whether there are rows to process.
 */
static SequenceNumberRange *
GetSequenceNumberRange(char *pipelineName)
{
	SequenceNumberRange *range = (SequenceNumberRange *) palloc0(sizeof(SequenceNumberRange));

	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have write
	 * privileges for the pipelines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Get the last-drawn sequence number, which may be part of a write that
	 * has not committed yet. Also block other pipeline rollups.
	 */
	char	   *query =
		"select"
		" last_processed_sequence_number + 1,"
		" pg_catalog.pg_sequence_last_value(sequence_name) seq "
		"from incremental.sequence_pipelines "
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

	/* read last_processed_sequence_number + 1 result */
	bool		rangeStartIsNull = false;
	Datum		rangeStartDatum = SPI_getbinval(row, rowDesc, 1, &rangeStartIsNull);

	if (!rangeStartIsNull)
		range->rangeStart = DatumGetInt64(rangeStartDatum);

	/* read pg_sequence_last_value result */
	bool		rangeEndIsNull = false;
	Datum		rangeEndDatum = SPI_getbinval(row, rowDesc, 2, &rangeEndIsNull);

	if (!rangeEndIsNull)
		range->rangeEnd = DatumGetInt64(rangeEndDatum);

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	/* return whether there are rows to process */
	return range;
}

/*
 * UpdateLastProcessedSequenceNumber updates the last_processed_sequence_number
 * in pipeline.pipelines to the given values.
 */
void
UpdateLastProcessedSequenceNumber(char *pipelineName, int64 lastSequenceNumber)
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
		"update incremental.sequence_pipelines "
		"set last_processed_sequence_number = $2 "
		"where pipeline_name operator(pg_catalog.=) $1";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 2;
	Oid			argTypes[] = {TEXTOID, INT8OID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		Int64GetDatum(lastSequenceNumber)
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


/*
 * FindSequenceForRelation returns the Oid of a sequence belonging to the
 * given relation.
 *
 * We currently don't detect sequences that were manually added through
 * DEFAULT nextval(...).
 */
Oid
FindSequenceForRelation(Oid relationId)
{
	List	   *sequences = getOwnedSequences(relationId);

	if (list_length(sequences) == 0)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("relation \"%s\" does not have any sequences associated "
							   "with it",
							   get_rel_name(relationId))));

	if (list_length(sequences) > 1)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("relation \"%s\" has multiple sequences associated "
							   "with it",
							   get_rel_name(relationId)),
						errhint("Specify the name of the sequence to use instead of "
								"the table name")));

	return linitial_oid(sequences);
}
