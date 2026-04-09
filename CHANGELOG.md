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
