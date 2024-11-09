#pragma once

#define SEQUENCE_RANGE_PIPELINE 's'
#define TIME_INTERVAL_PIPELINE 't'

typedef char PipelineType;

/*
 * PipelineDesc describes a pipeline.
 */
typedef struct PipelineDesc
{
	/* name of the pipeline */
	char	   *pipelineName;

	/* type of the pipeline */
	PipelineType pipelineType;

	/* user ID of the pipeline owner */
	Oid			ownerId;

	/* OID of the source relation or sequence */
	Oid			sourceRelationId;

	/* command to run for the pipeline */
	char	   *command;
}			PipelineDesc;

PipelineDesc *ReadPipelineDesc(char *pipelineName);
