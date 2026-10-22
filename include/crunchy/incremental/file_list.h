#pragma once

#define DEFAULT_FILE_LIST_FUNCTION "lake_file.list"
#define ORIGINAL_DEFAULT_FILE_LIST_FUNCTION "crunchy_lake.list_files"

/* job index value to process the files of all jobs of a file list pipeline */
#define ALL_JOBS (-1)

extern char *DefaultFileListFunction;

void		InitializeFileListPipelineState(char *pipelineName, char *prefix, bool batched, char *listFunction, int maxBatchSize,
										  int maxBatchesPerRun, int jobCount);
void		RemoveProcessedFileList(char *pipelineName);
void		ExecuteFileListPipeline(char *pipelineName, char *command, int jobIndex);
int			GetFileListPipelineJobCount(char *pipelineName);
int			SetFileListPipelineJobCount(char *pipelineName, int jobCount);
bool		ListFunctionExists(char *listFunction);
char	   *SanitizeListFunction(char *listFunction);
void		InsertProcessedFile(char *pipelineName, char *path);
