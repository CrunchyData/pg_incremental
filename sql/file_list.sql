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

-- reset_pipeline: clears processed_files so all files are reprocessed
select incremental.reset_pipeline('ingest-files', execute_immediately := false);
call incremental.execute_pipeline('ingest-files');
-- a.csv–d.csv were processed once before reset and once after: 4 + 4 = 8
select count(*) from processed_log;

drop schema file_list cascade;
drop extension pg_incremental;
