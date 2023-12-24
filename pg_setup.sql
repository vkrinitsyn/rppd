-- /*%LPH%*/


CREATE table if not exists @extschema@.rppd_config (
    id serial primary key,
    host varchar(64) not null unique,
    host_name varchar(64) not null unique,
    active_since timestamp default current_timestamp,
    max_db_connections int not null default 10,
    master bool unique
--  db pool/config name
);
comment on table @extschema@.rppd_config is 'App nodes host binding, master host flag. If added by someone, will notify all others';
comment on column @extschema@.rppd_config.active_since is 'The NULL or future value indicate incative nodes';
comment on column @extschema@.rppd_config.host_name is 'The readable name for a node host instance. use --name= to set on server start';
comment on column @extschema@.rppd_config.host is 'The node address and port to connect';
comment on column @extschema@.rppd_config.master is 'The node act a cluster coordinator';
comment on column @extschema@.rppd_config.max_db_connections is 'Max connections database connections. To perform concurrent Python events execution on the topic make a queue = false';

CREATE TRIGGER @extschema@_rppd_config_event AFTER INSERT OR UPDATE OR DELETE ON
    @extschema@.rppd_config FOR EACH ROW EXECUTE PROCEDURE @extschema@.rppd_event();


CREATE table if not exists @extschema@.rppd_function (
    id serial primary key,
    code text not null,
    checksum varchar(64) not null,
    schema_table varchar(64) not null,
    topic varchar(64) not null default '', -- ''
    queue bool not null default true,
    cleanup_logs_min int not null default 0
-- TODO: sign, approve, cadence, env pass
);

comment on table @extschema@.rppd_function is 'Python function code, linked table and topic. If modify, than the cache will be refreshed. Functions triggered for same topic OR table i.e. many fn per event will order by id.';
comment on column @extschema@.rppd_function.schema_table is 'The rppd_event() must trigger on the corresponding "schema_table" to fire the function, see topic to identify execution order';
comment on column @extschema@.rppd_function.topic is '"": queue/topic per table (see schema_table); ".{column}": queue per value of the column "id" in this table, the value type must be int; "{any_name}": global queue i.e. multiple tables can share the same queue/topic';
comment on column @extschema@.rppd_function.queue is 'if queue, then perform consequence events execution for the same topic. Ex: if the "topic" is ".id" and not queue, then events for same row will execute in parallel';
comment on column @extschema@.rppd_function.cleanup_logs_min is 'Cleanup rppd_function_log after certain minutes,  Zero is not store logs to rppd_function_log';

CREATE TRIGGER @extschema@_rppd_function_event AFTER INSERT OR UPDATE OR DELETE ON
    @extschema@.rppd_function FOR EACH ROW EXECUTE PROCEDURE @extschema@.rppd_event();


-- function log
CREATE table if not exists @extschema@.rppd_function_log (
    id bigserial primary key,
    node_id int not null,
    fn_id int not null,
    trig_value json,
    trig_type int not null default 0,
    finished_at timestamp, -- not null default current_timestamp,
    took_sec int not null default 0,
    error_msg text
);

comment on table @extschema@.rppd_function_log is 'Python function code execution on app nodes';
comment on column @extschema@.rppd_function_log.node_id is 'The rppd_config.id as a reference to execution node';
comment on column @extschema@.rppd_function_log.fn_id is 'The rppd_function.id, but no FK to keep performance';
comment on column @extschema@.rppd_function_log.trig_value is 'The value of column to trigger if topic is ".{column}". Use stored value to load on restore queue on startup and continue. Must be column type int to continue otherwise will save null and not able to restore.';
comment on column @extschema@.rppd_function_log.trig_type is 'Type of event: Update = 0, Insert = 1, Delete = 2, Truncate = 3';
comment on column @extschema@.rppd_function_log.finished_at is 'The rppd_function finushed or NULL if it is not';
comment on column @extschema@.rppd_function_log.error_msg is 'The error indicator. Must be null if completed OK';


-- schedule
CREATE table if not exists @extschema@.rppd_cron (
    id serial primary key,
    fn_id int not null,
    cron varchar(64) not null,
    timeout_sec int,
    finished_at timestamp,
    started_at timestamp,
    error_msg text
);

comment on table @extschema@.rppd_cron is 'Periodic schedule';
comment on column @extschema@.rppd_cron.fn_id is 'The table reference to the function configuration. The topic in this function must be a column to increment on periodic event fire';
comment on column @extschema@.rppd_cron.cron is 'The schedule see https://en.wikipedia.org/wiki/Cron , Please adjust timeout accordingly.';
comment on column @extschema@.rppd_cron.finished_at is 'The function wont start if finished_at is null. If started_at is null, the function never started yet';

-- trigger a refresh
CREATE TRIGGER @extschema@_rppd_cron_event AFTER INSERT OR UPDATE OR DELETE ON
    @extschema@.rppd_cron FOR EACH ROW EXECUTE PROCEDURE @extschema@.rppd_event();