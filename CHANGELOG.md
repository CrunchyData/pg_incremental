### pg\_incremental v1.6.0

* Add `job_count` to file list pipelines (`incremental.create_file_list_pipeline` and `incremental.file_list_pipelines`) to process files in parallel. Each file is assigned to one of the jobs by a hash of its path, and `incremental.execute_pipeline(pipeline_name, job_index)` processes only the files of one job. Different jobs can run concurrently, and with a schedule, a pg\_cron job is created for each of them.
* Add `incremental.alter_file_list_pipeline(pipeline_name, job_count)` to change the number of parallel jobs of a file list pipeline, for instance during a backfill.
* With `job_count` greater than 1, `execute_immediately` in `create_file_list_pipeline` and `reset_pipeline` leaves existing files to the parallel jobs.
* `incremental.drop_pipeline` no longer fails when pg\_cron is not installed.
* Pipeline names ending in `:job:<number>` are no longer allowed, and scheduling fails instead of replacing an existing pg\_cron job with the same name and a different command.
* Existing file list pipelines keep working with the new library before `ALTER EXTENSION pg_incremental UPDATE` has run on a 1.4 schema (v1.5.0 failed with `column "max_batches_per_run" does not exist`).
* Security: `incremental._drop_trigger`, the `SECURITY DEFINER` event trigger function that drops pipelines when their source table is dropped, now sets `search_path = pg_catalog, pg_temp` and schema-qualifies the objects it references. Previously, an unprivileged user could create objects in `pg_temp` (such as a type named `record`) that the function would pick up and run with the privileges of the extension owner.
* `ALTER EXTENSION pg_incremental UPDATE` recreates `incremental._drop_trigger` with these settings, so existing installations are fixed by upgrading to 1.6.
* Security: the list function of a file list pipeline now runs as the user executing the pipeline. Previously, it was called with superuser privileges, so any user that could create a function could become superuser by creating a file list pipeline with that function as its `list_function`.
* Security: sequence pipelines now require `SELECT` or `USAGE` on the sequence when the pipeline is created and executed, and read the last sequence value as the current user. Previously, any user could create a sequence pipeline to read the last value of a sequence they do not have access to.

### pg\_incremental v1.5.0

* Add upgrade script `pg_incremental--1.4--1.5.sql` so existing installs receive the `pg_cron` guard in `_drop_extension_trigger` (the trigger function lives outside the extension, so fixing `pg_incremental--1.0.sql` alone does not update it). The migration drops and recreates the function and event trigger and removes them from extension membership again.
* Add `max_batches_per_run` to file list pipelines (`incremental.create_file_list_pipeline` and `incremental.file_list_pipelines`). Default `-1` means a single `execute_pipeline` run processes all unprocessed paths in that invocation; a positive value limits how many batch iterations run per call (one file per iteration when not batched, one array batch when batched).

### pg\_incremental v1.4.1 (December 12, 2025)

* Use `lake_file.list` as the default file list function, fallback to crunchy

### pg\_incremental v1.4.0 (October 29, 2025)

* Add a `max_batch_size` argument to sequence pipelines to limit the number of sequence IDs processed per execution
* Improves handling of large batch uploads by allowing incremental processing in manageable chunks

### pg\_incremental v1.3.0 (May 15, 2025)
* Adds an incremental.skip\_file function to use for erroneuous files in file pipelines
* Removes the hard dependency on pg\_cron at CREATE EXTENSION time

### pg\_incremental v1.2.0 (February 26, 2025)

* Fixes bug that could cause batched file list pipelines to crash
* Add a `max_batch_size` argument to file list pipelines
* Improve performance of batched file list pipelines
* Adjust the default schedule of file list pipelines to every 15 minutes

### pg\_incremental v1.1.1 (January 10, 2025)

* Fixes a bug that prevented file list pipelines from being refreshed

### pg\_incremental v1.1.0 (January 10, 2025)

* Fixes a bug that prevented insert..select pipelines (#2)
* Fixes a bug that caused file list pipelines to repeat first file
* Add batched mode for file list pipeline
* Add incremental.default\_file\_list\_function setting

### pg\_incremental v1.0.1 (December 12, 2024)

* PostgreSQL 15 support

### pg\_incremental v1.0.0 (December 12, 2024)

* Initial release
