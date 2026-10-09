#pragma once

#define DEFAULT_FILE_LIST_FUNCTION "lake_file.list"
#define ORIGINAL_DEFAULT_FILE_LIST_FUNCTION "crunchy_lake.list_files"

/* shard argument value to process all files of a file list pipeline */
#define ALL_SHARDS (-1)

extern char *DefaultFileListFunction;

void		InitializeFileListPipelineState(char *pipelineName, char *prefix, bool batched, char *listFunction, int maxBatchSize,
										  int maxBatchesPerRun, int shardCount);
void		RemoveProcessedFileList(char *pipelineName);
void		ExecuteFileListPipeline(char *pipelineName, char *command, int shard);
int			GetFileListPipelineShardCount(char *pipelineName);
bool		ListFunctionExists(char *listFunction);
char	   *SanitizeListFunction(char *listFunction);
void		InsertProcessedFile(char *pipelineName, char *path);
