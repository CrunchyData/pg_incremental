-- The event trigger and its function live outside the extension so they survive
-- DROP EXTENSION and can clean up after it. To update them we must drop and
-- recreate them, then immediately remove them from extension membership again.
DROP EVENT TRIGGER incremental_drop_extension_trigger;
DROP FUNCTION incremental._drop_extension_trigger();

CREATE FUNCTION incremental._drop_extension_trigger()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SET search_path = pg_catalog
AS $function$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
        IF obj.object_identity = 'pg_incremental' AND obj.object_type = 'extension' THEN
            IF EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'pg_cron') THEN
                PERFORM cron.unschedule(jobname) FROM cron.job WHERE jobname LIKE 'pipeline:%';
            END IF;
            DROP SCHEMA incremental CASCADE;
        END IF;
    END LOOP;
END;
$function$;

CREATE EVENT TRIGGER incremental_drop_extension_trigger
 ON sql_drop
 WHEN TAG IN ('DROP EXTENSION')
 EXECUTE FUNCTION incremental._drop_extension_trigger();

ALTER EXTENSION pg_incremental DROP EVENT TRIGGER incremental_drop_extension_trigger;
ALTER EXTENSION pg_incremental DROP FUNCTION incremental._drop_extension_trigger();

-- max_batches_per_run for file list pipelines (see CHANGELOG v1.5.0).
ALTER TABLE incremental.file_list_pipelines
  ADD COLUMN max_batches_per_run int NOT NULL DEFAULT -1;

DROP FUNCTION incremental.create_file_list_pipeline(text,text,text,text,bool,int,text,bool);

CREATE FUNCTION incremental.create_file_list_pipeline(
    pipeline_name text,
    file_pattern text,
    command text,
    list_function text default NULL,
    batched bool default false,
    max_batch_size int default 100,
    schedule text default '*/15 * * * *',
    execute_immediately bool default true,
    max_batches_per_run int default -1)
 RETURNS void
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$incremental_create_file_list_pipeline$function$;
COMMENT ON FUNCTION incremental.create_file_list_pipeline(text,text,text,text,bool,int,text,bool,int)
 IS 'create a pipeline of new files';
