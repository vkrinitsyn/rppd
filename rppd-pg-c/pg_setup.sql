-- /*%LPH%*/
--
-- NOTE: the canonical definition of these tables is ../rppd.yaml, a SchemaGuard
-- schema. This file is the pgrx/extension form of the same thing, embedded by
-- `extension_sql_file!` so that CREATE EXTENSION keeps working unchanged.
--
-- Keep the two in step. The YAML is the one to edit: it states what should
-- exist and SchemaGuard derives the DDL, including the ALTERs that a
-- CREATE-only script cannot express - which is exactly how the git-source
-- columns (source_git*, min_schema_version, schema_git_tag) came to be missing
-- from installs made by this file.



CREATE table if not exists @extschema@.rppd_config (
    id serial primary key,
    host varchar(64) not null unique,
    host_name varchar(64) not null unique,
    active_since timestamptz default current_timestamp,
    max_db_connections int not null default 10,
    master bool unique
);

comment on table @extschema@.rppd_config is 'App nodes host binding, master host flag. If added by someone, will notify all others';
comment on column @extschema@.rppd_config.active_since is 'The NULL or future value indicate incative nodes';
comment on column @extschema@.rppd_config.host_name is 'The readable name for a node host instance. use --name= to set on server start';
comment on column @extschema@.rppd_config.host is 'The node address and port to connect';
comment on column @extschema@.rppd_config.master is 'The node act a cluster coordinator. Used values: true or null';
comment on column @extschema@.rppd_config.max_db_connections is 'Max connections database connections. To perform concurrent Python events execution on the topic make a queue = false';

CREATE TRIGGER @extschema@_rppd_config_event AFTER INSERT OR UPDATE OR DELETE ON
    @extschema@.rppd_config FOR EACH ROW EXECUTE PROCEDURE @extschema@.rppd_event();



CREATE table if not exists @extschema@.rppd_function (
    id serial primary key,
    code text not null,
    checksum varchar(64) not null,
    schema_table varchar(64) not null,
    topic varchar(64) not null default '', -- ''
    priority int not null default 1,
    queue bool not null default true,
    cleanup_logs_min int not null default 0,
    fn_logging bool not null default false,
    verbose_debug bool not null default false,
    env json, -- reserved for future usage: db pool (read only)/config python param name prefix (mapping)
    sign json, -- reserved for future usage: approve sign
    secret_names text -- [skv] the host's secret store names this function may read as SECRET (rppd.yaml)
);

select nextval('rppd_function_log_id_seq'::regclass); -- start log id from above 0, see ".id > 0"

comment on table @extschema@.rppd_function is 'Python function code, linked table and topic. If modify, than the cache will be refreshed. Functions triggered for same topic OR table i.e. many fn per event will order by id.';
comment on column @extschema@.rppd_function.schema_table is 'The rppd_event() must trigger on the corresponding "schema_table" to fire the function, see topic to identify execution order. if start with "/" then will create a watcher to etcd server. The /q/ or /queue/ required for messaging queue if use a rust etcd impl';
comment on column @extschema@.rppd_function.topic is '"": queue/topic per table (see schema_table); ".{column}": queue per value of the column "id" in this table, the value type must be int; "{any_name}": global queue i.e. multiple tables can share the same queue/topic';
comment on column @extschema@.rppd_function.queue is 'if queue, then perform consequence events execution for the same topic. If set true then etcd message will delete automatically. Ex: if the "topic" is ".id" and not queue, then events for same row will execute in parallel';
comment on column @extschema@.rppd_function.cleanup_logs_min is 'Cleanup rppd_function_log after certain minutes. Will execute only if fn_logging';
comment on column @extschema@.rppd_function.fn_logging is 'Store logs to rppd_function_log on every function call. Otherwise no DB call for a function execution, But DB still available to perform SQL call from Python.';
comment on column @extschema@.rppd_function.verbose_debug is 'Provide extra log message and errors';

CREATE TRIGGER @extschema@_rppd_function_event AFTER INSERT OR UPDATE OR DELETE ON
    @extschema@.rppd_function FOR EACH ROW EXECUTE PROCEDURE @extschema@.rppd_event();


-- function log
CREATE table if not exists @extschema@.rppd_function_log (
    id bigserial primary key,
    node_id int not null,
    fn_id int not null,
    trig_value json,
    trig_type int not null default 0,
    started_at timestamptz not null default current_timestamp,
    took_ms bigint,
    output text,
    error_msg text
);

comment on table @extschema@.rppd_function_log is 'Python function code execution results for a tracking consistency, durability and cadence';
comment on column @extschema@.rppd_function_log.node_id is 'The rppd_config.id as a reference to execution node';
comment on column @extschema@.rppd_function_log.fn_id is 'The rppd_function.id, but no FK to keep performance';
comment on column @extschema@.rppd_function_log.trig_value is 'The value of column to trigger if topic is ".{column}". Use stored value to load on restore queue on startup and continue. Must be column type int to continue otherwise will save null and not able to restore.';
comment on column @extschema@.rppd_function_log.trig_type is 'Type of event: Update = 0, Insert = 1, Delete = 2, Truncate = 3';
comment on column @extschema@.rppd_function_log.took_ms is 'The rppd_function running time in milliseconds or NULL if it is not finished';
comment on column @extschema@.rppd_function_log.output is 'The sysout grabbed from function';
comment on column @extschema@.rppd_function_log.error_msg is 'The error messages includes function init on connect';


-- schedule
CREATE table if not exists @extschema@.rppd_cron (
    id serial primary key,
    fn_id int not null,
    cron varchar(64) not null,
    column_name varchar(64) not null,
    column_value text,
    cadence bool not null default false,
    timeout_sec int,
    finished_at timestamptz,
    started_at timestamptz,
    error_msg text,
    -- [cadence] a row with cadence = true watches a pipeline process: see rppd README
    watch_min int not null default 0,
    retry_sec int not null default 0,
    watch_q varchar(256),
    deadline_q varchar(256),
    state varchar(16) not null default 'ok',
    last_seen timestamptz,
    since timestamptz,
    attempt int not null default 0
);

-- [cadence] an existing table gets the cadence columns (rppd adds them itself at startup too)
ALTER TABLE @extschema@.rppd_cron
    ADD COLUMN IF NOT EXISTS watch_min int not null default 0,
    ADD COLUMN IF NOT EXISTS retry_sec int not null default 0,
    ADD COLUMN IF NOT EXISTS watch_q varchar(256),
    ADD COLUMN IF NOT EXISTS deadline_q varchar(256),
    ADD COLUMN IF NOT EXISTS state varchar(16) not null default 'ok',
    ADD COLUMN IF NOT EXISTS last_seen timestamptz,
    ADD COLUMN IF NOT EXISTS since timestamptz,
    ADD COLUMN IF NOT EXISTS attempt int not null default 0;


comment on table @extschema@.rppd_cron is 'Periodic function python trigger schedule and cadence';
comment on column @extschema@.rppd_cron.fn_id is 'The table reference to the function configuration. The topic in this function must be a column to increment on periodic event fire';
comment on column @extschema@.rppd_cron.cron is 'The schedule see https://en.wikipedia.org/wiki/Cron , Please adjust timeout accordingly.';
comment on column @extschema@.rppd_cron.column_name is 'The column in target table to update';
comment on column @extschema@.rppd_cron.column_value is 'The column SQL value in target table to update';
comment on column @extschema@.rppd_cron.cadence is 'true: the row watches a pipeline process (pp) instead of scheduling a job. fn_id names the function consuming the pp queue (its schema_table is a /q/ path); cron is the EXPECTED cadence - a message is due by the end of the first full cron period after the last one seen. Missed: state watch and a message to watch_q (repeated every retry_sec), after watch_min the state deadline and one message to deadline_q; the next message: state ok and an ok message to the queue of the state left';
comment on column @extschema@.rppd_cron.watch_min is 'cadence: minutes on watch before the deadline; 0 = the deadline message at once';
comment on column @extschema@.rppd_cron.retry_sec is 'cadence: the watch message repeats every retry_sec seconds while on watch; 0 = once';
comment on column @extschema@.rppd_cron.watch_q is 'cadence: the /q/ queue the watch and its ok messages go to; null = none';
comment on column @extschema@.rppd_cron.deadline_q is 'cadence: the /q/ queue the deadline and its ok messages go to; null = none';
comment on column @extschema@.rppd_cron.state is 'cadence, kept by rppd: ok | watch | deadline';
comment on column @extschema@.rppd_cron.last_seen is 'cadence, kept by rppd: the last message seen on the pp queue';
comment on column @extschema@.rppd_cron.since is 'cadence, kept by rppd: when the current state was entered';
comment on column @extschema@.rppd_cron.attempt is 'cadence, kept by rppd: watch messages sent in this watch';
comment on column @extschema@.rppd_cron.finished_at is 'The function wont start if finished_at is null, except timeout. If started_at is null, the function never started yet';
comment on column @extschema@.rppd_cron.timeout is 'The if the finished_at is not null but started_at . If started_at is null, the function never started yet';

-- trigger a refresh
CREATE TRIGGER @extschema@_rppd_cron_event AFTER INSERT OR DELETE ON
    @extschema@.rppd_cron FOR EACH ROW EXECUTE PROCEDURE @extschema@.rppd_event();

CREATE TRIGGER @extschema@_rppd_cron_event_up AFTER UPDATE OF id, fn_id, cron, column_name, column_value, cadence, timeout_sec, watch_min, retry_sec, watch_q, deadline_q
    ON @extschema@.rppd_cron FOR EACH ROW EXECUTE PROCEDURE @extschema@.rppd_event();

comment on function @extschema@.rppd_info(node_id integer) is 'Return node status in json format by node_id from rppd_config.id table';
comment on function @extschema@.rppd_info(cfg text) is 'Use the config table name: The name includes schema, i.e.: schema.table. Default is "public.rppd_config". if ".<table>" than default use schema is "public". if "<schema>." than default use table is "rppd_config"';
comment on function @extschema@.rppd_info(node_id integer, fn_log_id bigint) is 'Same as node status plus function execution status by fn_log.id table';
comment on function @extschema@.rppd_info(node_id integer, fn_log_uuid text) is 'Same as node status plus function execution status by uuid if not store to fn_log.id table. use * to get all uuid.';
