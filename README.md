# pg\_incremental: Incremental Data Processing in PostgreSQL

pg\_incremental is a simple extension that helps you do fast, reliable, incremental batch processing in PostgreSQL.

When storing an append-only stream of event data in PostgreSQL (e.g. IoT, time series), a common challenge is to process only the new data. For instance, you might want to create one or more summary tables containing pre-aggregated data, and insert or update aggregates as new data arrives. However, you cannot really know the data that is still being inserted by concurrent transactions, and immediately aggregating data when inserting (e.g. via triggers) is certain to create a concurrency bottleneck. You also want to make sure that all new events are processed successfully exactly once, even when queries fail.

A similar challenge exists with data coming in from cloud storage systems. New files show up in object storage, and they need to get processed or loaded into a table, exactly once.

With pg\_incremental, you define a pipeline with a parameterized query. The pipeline is executed for all existing data when created, and then periodically executed. If there is new data, the query is executed with parameter values that correspond to the new data. Depending on the type of pipeline, the parameters could reflect a new range of sequence values, a new time range, or a new file.

```sql
-- Periodically aggregate rows inserted into the events table into an events_agg table
select incremental.create_sequence_pipeline('event-aggregation', 'events', $$
  insert into events_agg
  select date_trunc('day', event_time), count(*)
  from events
  where event_id between $1 and $2
  group by 1
  on conflict (day) do update set event_count = events_agg.event_count + excluded.event_count;
$$);
```

The internal progress tracking is done in the same transaction as the command, which ensures exactly once delivery.

While there are much more sophisticated approaches to this problem like incremental materialized views or logical decoding-based solutions, they come with many limitations and a lack of flexibility. We felt the need for a simple, fire-and-forget tool that gets the job done without a lot of boilerplate.

## Build and install

pg\_incremental depends on [pg\_cron](https://github.com/citusdata/pg_cron), which needs to be installed first.

To build and install pg\_incremental from source:

```bash
git clone https://github.com/crunchydata/pg_incremental.git
cd pg_incremental
# Ensure pg_config is in your path, e.g.
export PATH=/usr/pgsql-17/bin:$PATH
make && sudo PATH=$PATH make install
```

Once the extension is installed, you can create the extension In PostgreSQL:
```sql
create extension pg_incremental cascade;

/* user needs pg_cron permission to create pipelines */
grant usage on schema cron to application;
```

You can only create pg\_incremental in the database that has pg\_cron.

## Creating incremental processing pipelines

There are 3 types of pipelines in pg\_incremental

- **Sequence pipelines** - The pipeline query is executed for a range of sequence values, with a mechanism to ensure that no more new sequence values will fall in the range. These pipelines are most suitable for incrementally building summary tables.
- **Time interval pipelines** - The pipeline query is executed for a time interval or range of time intervals, after the time interval has passed. These pipelines can be used for incrementally building summary tables or periodically exporting new data.
- **File list pipelines (PREVIEW)** - The pipeline query is executed for a new file obtained from a file list function. These pipelines can be used to import new data.

Each pipeline has a command with 1 or 2 parameters. The pipelines run periodically using [pg\_cron](https://github.com/citusdata/pg_cron) (every minute, by default) and execute the command only if there is new data to process. However, each pipeline execution will appear in `cron.job_run_details` regardless of whether there is new data.

We describe each type of pipeline below.

### Creating a sequence pipeline

You can define a sequence pipeline with the `incremental.create_sequence_pipeline` function by specifying a generic pipeline name, the name of a source table name with a sequence or an explicit sequence name, and a command. The command you pass will be executed in a context where `$1` and `$2` are set to the lowest and highest value of a range of sequence values that can be safely aggregated (bigint).

Example:
```sql
-- create a source table
create table events (
  event_id bigint generated always as identity,
  event_time timestamptz default now(),
  client_id bigint,
  path text,
  response_time double precision
);

-- BRIN indexes are highly effective in selecting new ranges
create index on events using brin (event_id);

-- generate some random inserts
insert into events (client_id, path, response_time)
select s % 100, '/page-' || (s % 3), random() from generate_series(1,1000000) s;

-- create a summary table to pre-aggregate the number of events per day
create table events_agg (
  day timestamptz,
  event_count bigint,
  primary key (day)
);

-- create a pipeline to aggregate new inserts from a postgres table using a sequence
-- $1 and $2 will be set to the lowest and highest (inclusive) sequence values that can be aggregated

select incremental.create_sequence_pipeline('event-aggregation', 'events', $$
  insert into events_agg
  select date_trunc('day', event_time), count(*)
  from events
  where event_id between $1 and $2
  group by 1
  on conflict (day) do update set event_count = events_agg.event_count + excluded.event_count;
$$);
```

When creating the pipeline, the command is executed immediately for all sequence values starting from 0. Immediate execution can be disabled by passing `execute_immediately := false`, in which case the first execution will happen as part of periodic job scheduling.

The pipeline execution ensures that the range of sequence values is known to be safe, meaning that there are no more transactions that might produce sequence values that are within the range. This is ensured by waiting for concurrent write transactions before proceeding with the command. The size of the range is effectively the number of inserts since the last time the pipeline was executed up to the moment that the new pipeline execution started. This technique was first introduced on the [Citus Data blog](https://www.citusdata.com/blog/2018/06/14/scalable-incremental-data-aggregation/) by the author of this extension.

The benefit of sequence pipelines is that they can process the data in small incremental steps and it is agnostic to where the timestamps used in aggregations came from (i.e. late data is fine). The downside is that you almost always have to merge aggregates using an ON CONFLICT clause, and there are situations where that is not possible (e.g. exact distinct counts).

Arguments of the `incremental.create_sequence_pipeline` function:

| Argument name         | Type     | Description                                        | Default                      |
| --------------------- | -------- | -------------------------------------------------- | ---------------------------- |
| `pipeline_name`       | text     | Name of the pipeline that acts as an identifier    | Required                     |
| `sequence_name`       | regclass | Name of a sequence or table with a sequence        | Required                     |
| `command`             | text     | Pipeline command with $1 and $2 parameters         | Required                     |
| `schedule`            | text     | pg\_cron schedule for periodic execution (or NULL) | `* * * * *` (every minute)   |
| `execute_immediately` | bool     | Execute command immediately for existing data      | `true`                       |

### Creating a time interval pipeline

You can define a time interval pipeline with the `incremental.create_time_interval_pipeline` function by specifying a generic pipeline name, an interval, and a command. The command will be executed in a context where `$1` and `$2` are set to the start and end (exclusive) of a range of time intervals that has passed (timestamptz).

Example:
```sql
-- continuing with the data model from the previous section, but with a time range pipeline

-- BRIN indexes are highly effective in selecting new ranges
create index on events using brin (event_time);

-- create a pipeline to aggregate new inserts using a 1 day interval
-- $1 and $2 will be set to the start and end (exclusive) of a range of time intervals that can be aggregated
select incremental.create_time_interval_pipeline('event-aggregation', '1 day', $$
  insert into events_agg
  select event_time::date, count(distinct event_id)
  from events
  where event_time >= $1 and event_time < $2
  group by 1
$$);
```

When creating the pipeline, the command is executed immediately for the time starting from 2000-01-01 00:00:00 (configurable using the `start_time` argument). Immediate execution can be disabled by passing `execute_immediately := false`, in which case the first execution will happen as part of periodic job scheduling.

The command is executed after a time interval has passed. If the interval is 1 day, then the data for the previous day is typically processed at 00:01:00 (delay is configurable). If the query fails multiple times, the range may expand to cover multiple unprocessed intervals, except when using `batched := false`.

When using `batched := false`, the command is executed separately for each time interval. This can be useful to periodically export a specific time interval. It's important to pick a `start_time` that's close to the lowest timestamp in the data to avoid executing the command many times redundantly for intervals that have passed but have no data.

```sql
-- define an export function that wraps a COPY command
create function export_events(start_time timestamptz, end_time timestamptz)
returns void language plpgsql as $function$
declare
  path text := format('/tmp/export/%s.csv', start_time::date);
begin
  execute format($$copy (select * from events where event_time >= start_time and event_time < end_time) to %L$$, path);
end;
$function$;

-- Export events daily to a CSV file, starting from 2024-11-01
-- The command is executed separately for each interval
select incremental.create_time_interval_pipeline('event-export',
  time_interval := '1 day',
  batched := false,
  start_time := '2024-11-01',
  command := $$ select export_events($1, $2) $$
);
```

The pipeline execution logic can also ensure that the range of time intervals is safe, _if the timestamp is generated by the database using now() and assuming no large clock jumps_ (usually safe in cloud environments). In that case, the caller should set the `source_table_name` argument to the name of the source table. The pipeline execution will then wait for concurrent writers to finish before executing the command.

```sql
-- create a pipeline to aggregate new inserts using a 1 day interval
-- also ensure that there are no uncommitted event_time values in the range by specifying source_table_name
select incremental.create_time_interval_pipeline('event-aggregation',
  time_interval := '1 day',
  source_table_name := 'events',
  command := $$
    ...
  $$);
```

The benefit of time interval pipelines is that they are easier to define and can do more complex processing such as exact distinct counts and are also more suitable for exporting data because the command always processes exact time ranges. The downside is that you need to wait until after a time interval passes to see results and inserting old timestamps may cause data to be skipped. Sequence pipelines are more reliable in that sense because the values are always generated by the database.

Arguments of the `incremental.create_time_range_pipeline` function:

| Argument name         | Type        | Description                                        | Default                    |
| --------------------- | ----------- | -------------------------------------------------- | -------------------------- |
| `pipeline_name`       | text        | User-defined name of the pipeline                  | Required                   |
| `time_interval`       | interval    | At which interval to execute the pipeline          | Required                   |
| `command`             | text        | Pipeline command with $1 and $2 parameters         | Required                   |
| `batched`             | text        | Whether to run the command for multiple intervals  | `true`                     |
| `start_time`          | timestamptz | Time from which the intervals start                | `2000-01-01 00:00:00`      |
| `source_table_name`   | regclass    | Wait for lockers of this table before aggregation  | NULL (no waiting)          |
| `schedule`            | text        | pg\_cron schedule for periodic execution (or NULL) | `* * * * *` (every minute) |
| `min_delay`           | interval    | How long to wait to process a past interval        | `30 seconds`               |
| `execute_immediately` | bool        | Execute command immediately for existing data      | `true`                     |

### Creating a file list pipeline (PREVIEW)

You can define a file list pipeline with the `incremental.create_file_list_pipeline` function by specifying a generic pipeline name, a file pattern, and a command. The command will be executed in a context where `$1` is set to the path of a file (text). The pipeline periodically looks for new files returned by a list function and then executes the command for each new file.

Example:
```sql
-- define an import function that wraps a COPY command
create function import_events(path text)
returns void language plpgsql as $function$
begin
	execute format($$copy events from %L$$, path);
end;
$function$;

-- create a pipeline to import new files into a table, one by one.
-- $1 will be set to the path of a new file
select incremental.create_file_list_pipeline('event-import', 's3://mybucket/events/inbox/*.csv', $$
   select import_events($1)
$$);
```

The API of the file list pipeline is still subject to change. It currently defaults to using the [`crunchy_lake.list_files` function](https://docs.crunchybridge.com/warehouse/data-lake#explore-your-object-store-files) function in [Crunchy Data Warehouse](https://www.crunchydata.com/products/warehouse). You can set the list function to another set-returning function that returns a `path` value as `text`.

Arguments of the `incremental.create_file_list_pipeline` function:

| Argument name         | Type        | Description                                         | Default                     |
| --------------------- | ----------- | --------------------------------------------------- | --------------------------- |
| `pipeline_name`       | text        | User-defined name of the pipeline                   | Required                    |
| `file_pattern`        | text        | File pattern to pass to the list function           | Required                    |
| `command`             | text        | Pipeline command with $1 and $2 parameters          | Required                    |
| `batched`             | bool        | Currently unused                                    | `false`                     |
| `list_function`       | text        | Name of the function used to list files             | `crunchy_lake.list_files`   |
| `schedule`            | text        | pg\_cron schedule for periodic execution (or NULL)  | `* * * * *` (every minute)  |
| `execute_immediately` | bool        | Execute command immediately for existing data       | `true`                      |


## Monitoring pipelines

There are two ways to monitor pipelines: 

1) via tables corresponding to each pipeline type: `incremental.sequence_pipelines`, `incremental.time_interval_pipelines`, and `incremental.processed_files`
2) via `cron.job_run_details` to check for errors

See the last processed sequence number in a sequence pipeline:

```sql
select * from incremental.sequence_pipelines ;
┌─────────────────────┬────────────────────────────┬────────────────────────────────┐
│    pipeline_name    │       sequence_name        │ last_processed_sequence_number │
├─────────────────────┼────────────────────────────┼────────────────────────────────┤
│ view-count-pipeline │ public.events_event_id_seq │                        3000000 │
│ event-aggregation   │ events_event_id_seq        │                        1000000 │
└─────────────────────┴────────────────────────────┴────────────────────────────────┘
```

See the last processed time interval in a time interval pipeline:

```sql
select * from incremental.time_interval_pipelines;
┌───────────────┬───────────────┬─────────┬───────────┬────────────────────────┐
│ pipeline_name │ time_interval │ batched │ min_delay │  last_processed_time   │
├───────────────┼───────────────┼─────────┼───────────┼────────────────────────┤
│ export-events │ 1 day         │ f       │ 00:00:30  │ 2024-12-17 00:00:00+01 │
└───────────────┴───────────────┴─────────┴───────────┴────────────────────────┘
```

See the processed files in a file list pipeline:
```sql
select * from incremental.file_list_pipelines ;
┌───────────────┬─────────────────────────────────────┬─────────┬─────────────────────────┐
│ pipeline_name │            file_pattern             │ batched │      list_function      │
├───────────────┼─────────────────────────────────────┼─────────┼─────────────────────────┤
│ event-import  │ s3://marco-crunchy-data/inbox/*.csv │ f       │ crunchy_lake.list_files │
└───────────────┴─────────────────────────────────────┴─────────┴─────────────────────────┘

select * from incremental.processed_files ;
┌───────────────┬────────────────────────────────────────────┐
│ pipeline_name │                    path                    │
├───────────────┼────────────────────────────────────────────┤
│ event-import  │ s3://marco-crunchy-data/inbox/20241215.csv │
│ event-import  │ s3://marco-crunchy-data/inbox/20241215.csv │
└───────────────┴────────────────────────────────────────────┘
```

For all pipelines, you can check the outcome of the underlying [pg_cron](https://github.com/citusdata/pg_cron) job and any error messages.
```sql
select jobname, start_time, status, return_message
from cron.job_run_details join cron.job using (jobid)
where jobname like 'pipeline:event-import%' order by 1 desc limit 3;
┌───────────────────────┬───────────────────────────────┬───────────┬────────────────┐
│        jobname        │          start_time           │  status   │ return_message │
├───────────────────────┼───────────────────────────────┼───────────┼────────────────┤
│ pipeline:event-import │ 2024-12-17 13:27:00.090057+01 │ succeeded │ CALL           │
│ pipeline:event-import │ 2024-12-17 13:26:00.055813+01 │ succeeded │ CALL           │
│ pipeline:event-import │ 2024-12-17 13:25:00.086688+01 │ succeeded │ CALL           │
└───────────────────────┴───────────────────────────────┴───────────┴────────────────┘
```

Note that the jobs run more frequently than the pipeline command is executed. The job will simply be a noop if there is no new work to do.

## Manually executing a pipeline

You can also execute a pipeline manually using the `incremental.execute_pipeline` procedure, though it will only run the command if there is new data to process.

```sql
-- call the incremental.execute_pipeline procedure using the CALL syntax
call incremental.execute_pipeline('event-aggregation');
```

When you create the pipeline, you can pass `schedule := NULL` to disable periodic scheduling, such that you can perform all executions manually.

Arguments of the `incremental.execute_pipeline` function:

| Argument name         | Type        | Description                                       | Default                     |
| --------------------- | ----------- | ------------------------------------------------- | --------------------------- |
| `pipeline_name`       | text        | User-defined name of the pipeline                 | Required                    |


## Resetting an incremental processing pipelines

If you need to rebuild an aggregation you can reset a pipeline to the beginning using the `incremental.reset_pipeline` function.
```sql
-- Clear the summary table and reset the pipeline to rebuild it
begin;
delete from events_agg;
select incremental.reset_pipeline('event-aggregation');
commit;
```
The pipeline will be executed from the start. If execution fails, the pipeline is not reset.

Arguments of the `incremental.reset_pipeline` function:

| Argument name         | Type        | Description                                       | Default                     |
| --------------------- | ----------- | ------------------------------------------------- | --------------------------- |
| `pipeline_name`       | text        | User-defined name of the pipeline                 | Required                    |
| `execute_immediately` | bool        | Execute command immediately for existing data     | `true`                      |

## Dropping an incremental processing pipelines

When you are done with a pipeline, you can drop it using `incremental.drop_pipline(..)`:
```sql
-- Drop the pipeline
select incremental.drop_pipeline('event-aggregation');
```

Arguments of the `incremental.drop_pipeline` function:

| Argument name         | Type        | Description                                       | Default                     |
| --------------------- | ----------- | ------------------------------------------------- | --------------------------- |
| `pipeline_name`       | text        | User-defined name of the pipeline                 | Required                    |

