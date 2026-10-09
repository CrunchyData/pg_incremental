-- job_count for file list pipelines (see CHANGELOG v1.6.0).
ALTER TABLE incremental.file_list_pipelines
  ADD COLUMN job_count int NOT NULL DEFAULT 1 CHECK (job_count > 0);

DROP FUNCTION incremental.create_file_list_pipeline(text,text,text,text,bool,int,text,bool,int);

CREATE FUNCTION incremental.create_file_list_pipeline(
    pipeline_name text,
    file_pattern text,
    command text,
    list_function text default NULL,
    batched bool default false,
    max_batch_size int default 100,
    schedule text default '*/15 * * * *',
    execute_immediately bool default true,
    max_batches_per_run int default -1,
    job_count int default 1)
 RETURNS void
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$incremental_create_file_list_pipeline$function$;
COMMENT ON FUNCTION incremental.create_file_list_pipeline(text,text,text,text,bool,int,text,bool,int,int)
 IS 'create a pipeline of new files';

CREATE PROCEDURE incremental.execute_pipeline(
    pipeline_name text,
    job_index int)
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$incremental_execute_pipeline_job$function$;
COMMENT ON PROCEDURE incremental.execute_pipeline(text,int)
 IS 'execute the pipeline command for a single job of a file list pipeline';

CREATE FUNCTION incremental.alter_file_list_pipeline(
    pipeline_name text,
    job_count int)
 RETURNS void
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$incremental_alter_file_list_pipeline$function$;
COMMENT ON FUNCTION incremental.alter_file_list_pipeline(text,int)
 IS 'change the number of parallel jobs of a file list pipeline';
