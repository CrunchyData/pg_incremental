create extension pg_incremental cascade;
create schema file_list;
set search_path to file_list;
set client_min_messages to warning;

-- Simulate object storage: a table whose rows stand in for files,
-- and a list function that matches paths against a LIKE pattern.
create table file_registry (path text primary key);

create function list_local_files(pattern text) returns setof text as $$
    select path from file_registry where path like pattern order by path
$$ language sql;

-- Result table for a non-batched pipeline: command receives $1 text
create table processed_log (path text);

-- Populate the registry before creating the pipeline so files are
-- processed immediately upon creation (execute_immediately default true).
insert into file_registry values
    ('/data/a.csv'),
    ('/data/b.csv'),
    ('/data/c.csv');

-- Non-batched pipeline: command receives a single file path as $1 text
select incremental.create_file_list_pipeline(
    'ingest-files',
    '/data/%.csv',
    $$ insert into file_list.processed_log values ($1) $$,
    list_function := 'file_list.list_local_files',
    batched := false,
    schedule := NULL);

select count(*) from processed_log;

-- no new files: pipeline does nothing
call incremental.execute_pipeline('ingest-files');
select count(*) from processed_log;

-- add a new file and process it incrementally
insert into file_registry values ('/data/d.csv');
call incremental.execute_pipeline('ingest-files');
select count(*) from processed_log;

-- no new files again: idempotent
call incremental.execute_pipeline('ingest-files');
select count(*) from processed_log;

-- Batched pipeline: command receives all files in a batch as $1 text[]
create table batched_log (path text);

insert into file_registry values
    ('/batch/1.csv'),
    ('/batch/2.csv'),
    ('/batch/3.csv'),
    ('/batch/4.csv'),
    ('/batch/5.csv');

-- 5 files with max_batch_size=3 triggers two separate command invocations
select incremental.create_file_list_pipeline(
    'batch-ingest',
    '/batch/%.csv',
    $$ insert into file_list.batched_log select unnest($1) $$,
    list_function := 'file_list.list_local_files',
    batched := true,
    max_batch_size := 3,
    schedule := NULL);

select count(*) from batched_log;

-- skip_file: marks a file as processed without running the command
create table skip_log (path text);

insert into file_registry values
    ('/skip/bad.csv'),
    ('/skip/good.csv'),
    ('/skip/ugly.csv');

select incremental.create_file_list_pipeline(
    'skip-test',
    '/skip/%.csv',
    $$ insert into file_list.skip_log values ($1) $$,
    list_function := 'file_list.list_local_files',
    batched := false,
    schedule := NULL,
    execute_immediately := false);

-- skip bad.csv before executing so it is never passed to the command
select incremental.skip_file('skip-test', '/skip/bad.csv');

call incremental.execute_pipeline('skip-test');

-- only good.csv and ugly.csv should have been processed by the command
select count(*) from skip_log;
-- all three paths (including skipped) appear in processed_files
select count(*) from incremental.processed_files where pipeline_name = 'skip-test';

-- max_batches_per_run: explicit -1 processes all pending files in one execute_pipeline
insert into file_registry values
    ('/unlim/x.csv'),
    ('/unlim/y.csv');
create table unlim_log (path text);
select incremental.create_file_list_pipeline(
    'unlimited-per-run',
    '/unlim/%.csv',
    $$ insert into file_list.unlim_log values ($1) $$,
    list_function := 'file_list.list_local_files',
    batched := false,
    schedule := NULL,
    execute_immediately := false,
    max_batches_per_run := -1);
call incremental.execute_pipeline('unlimited-per-run');
select count(*) from unlim_log;

-- max_batches_per_run: positive cap — one file per execute_pipeline when not batched
insert into file_registry values
    ('/cap/a.csv'),
    ('/cap/b.csv'),
    ('/cap/c.csv');
create table cap_log (path text);
select incremental.create_file_list_pipeline(
    'cap-one-per-run',
    '/cap/%.csv',
    $$ insert into file_list.cap_log values ($1) $$,
    list_function := 'file_list.list_local_files',
    batched := false,
    schedule := NULL,
    execute_immediately := false,
    max_batches_per_run := 1);
call incremental.execute_pipeline('cap-one-per-run');
select count(*) from cap_log;
call incremental.execute_pipeline('cap-one-per-run');
select count(*) from cap_log;
call incremental.execute_pipeline('cap-one-per-run');
select count(*) from cap_log;
call incremental.execute_pipeline('cap-one-per-run');
select count(*) from cap_log;

-- max_batches_per_run: batched — one batch iteration per execute_pipeline (max_batch_size=2, five files)
insert into file_registry values
    ('/mcap/1.csv'),
    ('/mcap/2.csv'),
    ('/mcap/3.csv'),
    ('/mcap/4.csv'),
    ('/mcap/5.csv');
create table mcap_log (path text);
select incremental.create_file_list_pipeline(
    'batched-cap-one-batch-per-run',
    '/mcap/%.csv',
    $$ insert into file_list.mcap_log select unnest($1) $$,
    list_function := 'file_list.list_local_files',
    batched := true,
    max_batch_size := 2,
    schedule := NULL,
    execute_immediately := false,
    max_batches_per_run := 1);
call incremental.execute_pipeline('batched-cap-one-batch-per-run');
select count(*) from mcap_log;
call incremental.execute_pipeline('batched-cap-one-batch-per-run');
select count(*) from mcap_log;
call incremental.execute_pipeline('batched-cap-one-batch-per-run');
select count(*) from mcap_log;

-- shard_count: a sharded pipeline whose shards each process a disjoint subset of files
insert into file_registry select '/shard/' || i || '.csv' from generate_series(1, 20) i;
create table shard_log (path text, shard int);
select incremental.create_file_list_pipeline(
    'sharded',
    '/shard/%.csv',
    $$ insert into file_list.shard_log values ($1, current_setting('file_list.shard')::int) $$,
    list_function := 'file_list.list_local_files',
    schedule := NULL,
    execute_immediately := false,
    shard_count := 3);
select shard_count from incremental.file_list_pipelines where pipeline_name = 'sharded';

set file_list.shard to 0;
call incremental.execute_pipeline('sharded', 0);
set file_list.shard to 1;
call incremental.execute_pipeline('sharded', 1);
set file_list.shard to 2;
call incremental.execute_pipeline('sharded', 2);

-- every file is processed exactly once, by the shard its path hashes to
select count(*), count(distinct path) from shard_log;
select count(*) from shard_log
where shard <> abs(hashtextextended(path, 0) % 3);
select count(*) > 0 from shard_log group by shard order by shard;

-- re-running a shard does nothing
call incremental.execute_pipeline('sharded', 0);
select count(*) from shard_log;

-- an unsharded execution processes new files of all shards
insert into file_registry select '/shard/' || i || '.csv' from generate_series(21, 25) i;
set file_list.shard to -1;
call incremental.execute_pipeline('sharded');
select count(*), count(distinct path) from shard_log;
select count(*) from incremental.processed_files where pipeline_name = 'sharded';

-- shard must be in range, and only file list pipelines can be sharded
call incremental.execute_pipeline('sharded', 3);
call incremental.execute_pipeline('sharded', -1);
call incremental.execute_pipeline('sharded', NULL);
create table shard_events (id bigserial, path text);
select incremental.create_sequence_pipeline(
    'not-a-file-list', 'file_list.shard_events', $$ select $1, $2 $$, schedule := NULL);
call incremental.execute_pipeline('not-a-file-list', 0);
select incremental.create_file_list_pipeline(
    'zero-shards', '/shard/%.csv', $$ select $1 $$,
    list_function := 'file_list.list_local_files', schedule := NULL, shard_count := 0);

-- batched sharded pipeline with max_batch_size
insert into file_registry select '/bshard/' || i || '.csv' from generate_series(1, 10) i;
create table bshard_log (batch_size int);
select incremental.create_file_list_pipeline(
    'batched-sharded',
    '/bshard/%.csv',
    $$ insert into file_list.bshard_log values (cardinality($1)) $$,
    list_function := 'file_list.list_local_files',
    batched := true,
    max_batch_size := 2,
    schedule := NULL,
    execute_immediately := false,
    shard_count := 2);
call incremental.execute_pipeline('batched-sharded', 0);
call incremental.execute_pipeline('batched-sharded', 1);
select sum(batch_size), max(batch_size) <= 2 from bshard_log;
select count(*) from incremental.processed_files where pipeline_name = 'batched-sharded';

-- reset clears the processed files of all shards
select incremental.reset_pipeline('batched-sharded', execute_immediately := false);
select count(*) from incremental.processed_files where pipeline_name = 'batched-sharded';

-- drop a sharded pipeline
select incremental.drop_pipeline('batched-sharded');
select count(*) from incremental.file_list_pipelines where pipeline_name = 'batched-sharded';

-- reset_pipeline: clears processed_files so all files are reprocessed
select incremental.reset_pipeline('ingest-files', execute_immediately := false);
call incremental.execute_pipeline('ingest-files');
-- a.csv–d.csv were processed once before reset and once after: 4 + 4 = 8
select count(*) from processed_log;

drop schema file_list cascade;
drop extension pg_incremental;
