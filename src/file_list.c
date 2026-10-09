#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "catalog/dependency.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_proc.h"
#include "crunchy/incremental/file_list.h"
#include "crunchy/incremental/pipeline.h"
#include "executor/spi.h"
#include "parser/parse_func.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"


/*
 * FileList represents a set of files that can be safely processed.
 */
typedef struct FileList
{
	List	   *files;
	bool		batched;
	int			maxBatchSize;
	int			maxBatchesPerRun;
}			FileList;


static void ExecuteFileListPipelineForFile(char *pipelineName, char *command, char *path);
static void ExecuteBatchedFileListPipeline(char *pipelineName, char *command, FileList * fileList,
										   int offset);
static void ExecuteFileListPipelineForFileArray(char *pipelineName, char *command,
												ArrayType *filePaths);
static FileList * GetUnprocessedFilesForPipeline(char *pipelineName, int shard);
static List *GetUnprocessedFileList(char *pipelineName, char *listFunction,
									char *filePattern, int shardCount, int shard);
static void LockFileListPipelineShard(char *pipelineName, int shard);


/* crunchy_lake.default_file_list_function setting */
char	   *DefaultFileListFunction = DEFAULT_FILE_LIST_FUNCTION;



/*
 * InitializeFileListPipelineState adds the initial file list pipeline state.
 */
void
InitializeFileListPipelineState(char *pipelineName, char *pattern, bool batched,
								char *listFunction, int maxBatchSize,
								int maxBatchesPerRun, int shardCount)
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
		"insert into incremental.file_list_pipelines "
		"(pipeline_name, file_pattern, batched, list_function, max_batch_size, max_batches_per_run, "
		"shard_count) "
		"values ($1, $2, $3, $4, $5, $6, $7)";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 7;
	Oid			argTypes[] = {TEXTOID, TEXTOID, BOOLOID, TEXTOID, INT4OID, INT4OID, INT4OID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		CStringGetTextDatum(pattern),
		BoolGetDatum(batched),
		CStringGetTextDatum(listFunction),
		Int32GetDatum(maxBatchSize),
		Int32GetDatum(maxBatchesPerRun),
		Int32GetDatum(shardCount)
	};
	char		argNulls[] = {
		' ', ' ', ' ', ' ', maxBatchSize > 0 ? ' ' : 'n', ' ', ' '
	};

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
 * ExecuteFileListPipeline executes a file list pipeline. If shard is
 * ALL_SHARDS, all unprocessed files are considered. Otherwise, only the
 * unprocessed files that hash to the given shard are considered, and
 * other shards of the same pipeline can be executed concurrently.
 */
void
ExecuteFileListPipeline(char *pipelineName, char *command, int shard)
{
	/* get the full fileList of data to process */
	FileList   *fileList = GetUnprocessedFilesForPipeline(pipelineName, shard);

	if (fileList->files == NIL)
	{
		ereport(NOTICE, (errmsg("pipeline %s: no files to process",
								pipelineName)));
		return;
	}

	if (fileList->batched)
	{
		int			offset = 0;
		int			batchesDone = 0;

		do
		{
			ExecuteBatchedFileListPipeline(pipelineName, command, fileList, offset);

			batchesDone++;
			offset += fileList->maxBatchSize;

			if (fileList->maxBatchesPerRun >= 0 &&
				batchesDone >= fileList->maxBatchesPerRun)
				break;
		}
		while (fileList->maxBatchSize > 0 && offset < list_length(fileList->files));
	}
	else
	{
		ListCell   *fileCell = NULL;
		int			batchesDone = 0;

		foreach(fileCell, fileList->files)
		{
			char	   *path = lfirst(fileCell);

			ereport(NOTICE, (errmsg("pipeline %s: processing file list pipeline for %s",
									pipelineName, path)));

			ExecuteFileListPipelineForFile(pipelineName, command, path);
			InsertProcessedFile(pipelineName, path);
			batchesDone++;

			if (fileList->maxBatchesPerRun >= 0 &&
				batchesDone >= fileList->maxBatchesPerRun)
				break;
		}
	}
}


/*
 * ExecuteFileListPipelineForFile executes a file list pipeline for
 * the given file.
 */
static void
ExecuteFileListPipelineForFile(char *pipelineName, char *command, char *path)
{
	PushActiveSnapshot(GetTransactionSnapshot());

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 1;
	Oid			argTypes[] = {TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(path)
	};
	char	   *argNulls = " ";

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
 * ExecuteBatchedFileListPipeline executes a pipeline using text array to
 * pass in the filename.
 */
static void
ExecuteBatchedFileListPipeline(char *pipelineName, char *command, FileList * fileList,
							   int offset)
{
	int			fileCount = list_length(fileList->files) - offset;

	if (fileList->maxBatchSize > 0 && fileCount > fileList->maxBatchSize)
		fileCount = fileList->maxBatchSize;

	Datum	   *fileDatums = palloc0(sizeof(Datum) * fileCount);
	int			datumIndex = 0;
	ListCell   *fileCell = NULL;

	for_each_from(fileCell, fileList->files, offset)
	{
		char	   *path = lfirst(fileCell);

		fileDatums[datumIndex] = CStringGetTextDatum(path);
		datumIndex += 1;

		if (fileList->maxBatchSize > 0 && datumIndex == fileList->maxBatchSize)
			break;
	}

	ArrayType  *filesArray = construct_array(fileDatums,
											 fileCount,
											 TEXTOID,
											 -1,
											 false,
											 TYPALIGN_INT);

	ereport(NOTICE, (errmsg("pipeline %s: processing file list pipeline for %d files",
							pipelineName,
							fileCount)));

	ExecuteFileListPipelineForFileArray(pipelineName, command, filesArray);

	int			fileIndex = 0;

	for_each_from(fileCell, fileList->files, offset)
	{
		char	   *path = lfirst(fileCell);

		InsertProcessedFile(pipelineName, path);

		fileIndex += 1;

		if (fileList->maxBatchSize > 0 && fileIndex == fileList->maxBatchSize)
			break;
	}
}



/*
 * ExecuteFileListPipelineFor executes a file list pipeline for the given
 * files array.
 */
static void
ExecuteFileListPipelineForFileArray(char *pipelineName, char *command, ArrayType *filePaths)
{
	PushActiveSnapshot(GetTransactionSnapshot());

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 1;
	Oid			argTypes[] = {TEXTARRAYOID};
	Datum		argValues[] = {
		PointerGetDatum(filePaths)
	};
	char	   *argNulls = " ";

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
 * GetUnprocessedFilesForPipeline returns the list of files that are not
 * yet processed, optionally restricted to a single shard.
 *
 * An unsharded execution locks the pipeline row exclusively, which
 * serializes it with all other executions of the pipeline. A sharded
 * execution only takes a share lock on the pipeline row, such that
 * different shards can run concurrently, and an advisory lock on the
 * shard, such that executions of the same shard are serialized. Shards
 * process disjoint sets of files, so they never insert the same path into
 * processed_files.
 */
static FileList *
GetUnprocessedFilesForPipeline(char *pipelineName, int shard)
{
	MemoryContext outerContext = CurrentMemoryContext;

	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have write
	 * privileges for the pipelines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Get the file list pipeline properties.
	 */
	char	   *query = shard == ALL_SHARDS ?
		"select batched, list_function, file_pattern, max_batch_size, max_batches_per_run, "
		"shard_count "
		"from incremental.file_list_pipelines "
		"where pipeline_name operator(pg_catalog.=) $1 "
		"for update" :
		"select batched, list_function, file_pattern, max_batch_size, max_batches_per_run, "
		"shard_count "
		"from incremental.file_list_pipelines "
		"where pipeline_name operator(pg_catalog.=) $1 "
		"for share";

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

	bool		isNull = false;
	Datum		batchedDatum = SPI_getbinval(row, rowDesc, 1, &isNull);
	Datum		listFunctionDatum = SPI_getbinval(row, rowDesc, 2, &isNull);
	Datum		filePatternDatum = SPI_getbinval(row, rowDesc, 3, &isNull);
	Datum		maxBatchSizeDatum = SPI_getbinval(row, rowDesc, 4, &isNull);

	int			maxBatchSize = -1;

	if (!isNull)
		maxBatchSize = DatumGetInt32(maxBatchSizeDatum);

	bool		maxBatchesIsNull = false;
	Datum		maxBatchesDatum = SPI_getbinval(row, rowDesc, 5, &maxBatchesIsNull);

	int			maxBatchesPerRun = -1;

	if (!maxBatchesIsNull)
		maxBatchesPerRun = DatumGetInt32(maxBatchesDatum);

	Datum		shardCountDatum = SPI_getbinval(row, rowDesc, 6, &isNull);
	int			shardCount = DatumGetInt32(shardCountDatum);

	MemoryContext oldContext = MemoryContextSwitchTo(outerContext);

	bool		batched = DatumGetBool(batchedDatum);
	char	   *listFunction = TextDatumGetCString(listFunctionDatum);
	char	   *filePattern = TextDatumGetCString(filePatternDatum);

	MemoryContextSwitchTo(oldContext);

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	if (shard != ALL_SHARDS)
	{
		if (shard < 0 || shard >= shardCount)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("shard %d is out of range for pipeline \"%s\"",
								   shard, pipelineName),
							errdetail("The pipeline has %d shards, numbered 0 to %d.",
									  shardCount, shardCount - 1)));

		LockFileListPipelineShard(pipelineName, shard);
	}

	FileList   *fileList = (FileList *) palloc0(sizeof(FileList));

	fileList->batched = batched;
	fileList->maxBatchSize = maxBatchSize;
	fileList->maxBatchesPerRun = maxBatchesPerRun;
	fileList->files = GetUnprocessedFileList(pipelineName, listFunction, filePattern,
											 shardCount, shard);

	return fileList;
}


/*
 * LockFileListPipelineShard takes a transaction-level advisory lock on a
 * shard of a file list pipeline, such that concurrent executions of the
 * same shard are serialized.
 */
static void
LockFileListPipelineShard(char *pipelineName, int shard)
{
	char	   *query =
		"select pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext($1), $2)";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 2;
	Oid			argTypes[] = {TEXTOID, INT4OID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		Int32GetDatum(shard)
	};
	char	   *argNulls = "	";

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


/*
 * GetUnprocessedFileList lists the current set of files and subtracts
 * the already processed files. If shard is not ALL_SHARDS, only files
 * that hash to the given shard are returned.
 */
static List *
GetUnprocessedFileList(char *pipelineName, char *listFunction, char *filePattern,
					   int shardCount, int shard)
{
	List	   *fileList = NIL;
	MemoryContext outerContext = CurrentMemoryContext;

	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have write
	 * privileges for the pipelines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Get the unprocessed files.
	 */
	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "select list.path "
					 "from %s($2) as list(path) "
					 "left join incremental.processed_files proc "
					 "on (pipeline_name operator(pg_catalog.=) $1 "
					 "and list.path operator(pg_catalog.=) proc.path) "
					 "where proc.path is null",
					 listFunction);

	if (shard != ALL_SHARDS)
		appendStringInfoString(query,
							   " and pg_catalog.abs(pg_catalog.hashtextextended(list.path, 0) "
							   "operator(pg_catalog.%) $3) operator(pg_catalog.=) $4");

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = shard != ALL_SHARDS ? 4 : 2;
	Oid			argTypes[] = {TEXTOID, TEXTOID, INT8OID, INT8OID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		CStringGetTextDatum(filePattern),
		Int64GetDatum(shardCount),
		Int64GetDatum(shard)
	};
	char	   *argNulls = "	";

	SPI_connect();
	SPI_execute_with_args(query->data,
						  argCount,
						  argTypes,
						  argValues,
						  argNulls,
						  readOnly,
						  tupleCount);

	TupleDesc	rowDesc = SPI_tuptable->tupdesc;

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		HeapTuple	row = SPI_tuptable->vals[rowIndex];

		bool		isNull = false;
		Datum		pathDatum = SPI_getbinval(row, rowDesc, 1, &isNull);

		MemoryContext oldContext = MemoryContextSwitchTo(outerContext);

		fileList = lappend(fileList, TextDatumGetCString(pathDatum));

		MemoryContextSwitchTo(oldContext);
	}

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return fileList;
}


/*
 * InsertProcessedFile adds a new processed file to the processed_files
 * table.
 */
void
InsertProcessedFile(char *pipelineName, char *path)
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
		"insert into incremental.processed_files (pipeline_name, path) "
		"values ($1, $2)";

	bool		readOnly = false;
	int			tupleCount = 0;
	int			argCount = 2;
	Oid			argTypes[] = {TEXTOID, TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(pipelineName),
		CStringGetTextDatum(path)
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
 * RemoveProcessedFileList removes all the processed files for the given pipeline.
 */
void
RemoveProcessedFileList(char *pipelineName)
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
		"delete from incremental.processed_files "
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

	if (SPI_processed <= 0)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("pipeline \"%s\" cannot be found",
							   pipelineName)));

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * ListFunctionExists determines whether the given list function
 * exists.
 */
bool
ListFunctionExists(char *listFunction)
{
#if (PG_VERSION_NUM >= 160000)
	List	   *names = stringToQualifiedNameList(listFunction, NULL);
#else
	List	   *names = stringToQualifiedNameList(listFunction);
#endif
	Oid			argTypes[] = {TEXTOID};
	bool		missingOk = true;
	Oid			functionId = LookupFuncName(names, 1, argTypes, missingOk);

	return OidIsValid(functionId);
}


/*
 * SanitizeListFunction qualifies a list function name and errors
 * if the function cannot be found.
 */
char *
SanitizeListFunction(char *listFunction)
{
#if (PG_VERSION_NUM >= 160000)
	List	   *names = stringToQualifiedNameList(listFunction, NULL);
#else
	List	   *names = stringToQualifiedNameList(listFunction);
#endif
	Oid			argTypes[] = {TEXTOID};
	bool		missingOk = false;
	Oid			functionId = LookupFuncName(names, 1, argTypes, missingOk);

	HeapTuple	procTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(functionId));

	if (!HeapTupleIsValid(procTuple))
		elog(ERROR, "could not find function with OID %d", functionId);

	Form_pg_proc procForm = (Form_pg_proc) GETSTRUCT(procTuple);
	char	   *functionName = NameStr(procForm->proname);
	char	   *schemaName = get_namespace_name(procForm->pronamespace);

	ReleaseSysCache(procTuple);

	return quote_qualified_identifier(schemaName, functionName);
}


/*
 * GetFileListPipelineShardCount returns the number of shards of a file
 * list pipeline.
 */
int
GetFileListPipelineShardCount(char *pipelineName)
{
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	/*
	 * Switch to superuser in case the current user does not have read
	 * privileges for the pipelines table.
	 */
	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, SECURITY_LOCAL_USERID_CHANGE);

	char	   *query =
		"select shard_count "
		"from incremental.file_list_pipelines "
		"where pipeline_name operator(pg_catalog.=) $1";

	bool		readOnly = true;
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

	bool		isNull = false;
	Datum		shardCountDatum = SPI_getbinval(SPI_tuptable->vals[0],
												SPI_tuptable->tupdesc, 1, &isNull);
	int			shardCount = DatumGetInt32(shardCountDatum);

	SPI_finish();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return shardCount;
}
