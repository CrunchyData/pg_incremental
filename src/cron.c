#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "catalog/pg_authid.h"
#include "commands/extension.h"
#include "crunchy/incremental/cron.h"
#include "executor/spi.h"
#include "utils/builtins.h"


/*
 * ScheduleCronJob schedules a new pg_cron job and returns the job ID.
 */
int64
ScheduleCronJob(char *jobName, char *schedule, char *command)
{
	bool		missingOk = true;

	if (get_extension_oid("pg_cron", missingOk) == InvalidOid)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("pg_cron extension is not created"),
						errdetail("By default, pg_incremental uses pg_cron to schedule a periodic job"),
						errhint("Run CREATE EXTENSION pg_cron; or pass schedule := NULL")));

	char	   *query =
		"SELECT cron.schedule($1, $2, $3)";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 3;
	Oid			argTypes[] = {TEXTOID, TEXTOID, TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(jobName),
		CStringGetTextDatum(schedule),
		CStringGetTextDatum(command)
	};
	char	   *argNulls = "   ";

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
						errmsg("no job scheduled")));

	TupleDesc	rowDesc = SPI_tuptable->tupdesc;
	HeapTuple	row = SPI_tuptable->vals[0];

	/* read owner_id */
	bool		jobIdIsNull = false;
	Datum		jobIdDatum = SPI_getbinval(row, rowDesc, 1, &jobIdIsNull);

	Assert(!jobIdIsNull);

	int64		jobId = DatumGetInt64(jobIdDatum);

	SPI_finish();

	return jobId;
}


/*
 * UnscheduleCronJob unschedules a pg_cron job.
 */
void
UnscheduleCronJob(char *jobName)
{
	char	   *query =
		"SELECT cron.unschedule(jobid) from cron.job where jobname = $1";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 1;
	Oid			argTypes[] = {TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(jobName)
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
}
