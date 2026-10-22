-- Existing file list pipelines keep working with the new library before
-- ALTER EXTENSION .. UPDATE has run, and after it.
create extension pg_incremental version '1.4' cascade;
create schema upgrade;
set search_path to upgrade;
set client_min_messages to warning;

create table file_registry (path text primary key);
insert into file_registry values ('/up/a.csv'), ('/up/b.csv');
create function list_files(pattern text) returns setof text as $$
    select path from file_registry where path like pattern order by path
$$ language sql;
create table processed_log (path text);

-- a pipeline as created by version 1.4, with the 1.4 columns only
insert into incremental.pipelines (pipeline_name, pipeline_type, owner_id, command, search_path)
values ('old', 'f', (select oid from pg_roles where rolname = current_user),
        $$ insert into upgrade.processed_log values ($1) $$, 'upgrade');
insert into incremental.file_list_pipelines (pipeline_name, file_pattern, batched, list_function)
values ('old', '/up/%.csv', false, 'upgrade.list_files');

call incremental.execute_pipeline('old');
select count(*) from processed_log;

alter extension pg_incremental update to '1.5';
insert into file_registry values ('/up/c.csv');
call incremental.execute_pipeline('old');
select count(*) from processed_log;

alter extension pg_incremental update;
select job_count from incremental.file_list_pipelines where pipeline_name = 'old';
select incremental.alter_file_list_pipeline('old', 2);
insert into file_registry values ('/up/d.csv'), ('/up/e.csv'), ('/up/f.csv');
call incremental.execute_pipeline('old', 0);
call incremental.execute_pipeline('old', 1);
select count(*), count(distinct path) from processed_log;

select incremental.drop_pipeline('old');
drop schema upgrade cascade;
drop extension pg_incremental;
