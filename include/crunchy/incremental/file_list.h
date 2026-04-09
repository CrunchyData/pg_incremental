#pragma once

#define DEFAULT_FILE_LIST_FUNCTION "lake_file.list"
#define ORIGINAL_DEFAULT_FILE_LIST_FUNCTION "crunchy_lake.list_files"

extern char *DefaultFileListFunction;

void		InitializeFileListPipelineState(char *pipelineName, char *prefix, bool batched, char *listFunction, int maxBatchSize,
										  int maxBatchesPerRun);
void		RemoveProcessedFileList(char *pipelineName);
void		ExecuteFileListPipeline(char *pipelineName, char *command);
bool		ListFunctionExists(char *listFunction);
char	   *SanitizeListFunction(char *listFunction);
void		InsertProcessedFile(char *pipelineName, char *path);
