-- /*%LPH%*/


CREATE table if not exists @extschema@.rppd_config (
    id serial primary key,
    host varchar(64) not null unique,
    alias varchar(64) not null unique,
    active_since timestamp default current_timestamp,
    master bool unique
--  db pool/config name
);
comment on table @extschema@.rppd_config is 'App nodes host binding, master host flag. If added by someone, will notify all others';
comment on column @extschema@.rppd_config.active_since is 'The NULL or future value indicate incative nodes';

CREATE TRIGGER @extschema@_rppd_config_event AFTER INSERT OR UPDATE OR DELETE ON
    @extschema@.rppd_config FOR EACH ROW EXECUTE PROCEDURE @extschema@.rppd_event();


CREATE table if not exists @extschema@.rppd_function (
    id serial primary key,
    code text not null,
    checksum varchar(64) not null,
    schema_table varchar(64) not null,
    topic varchar(64) not null default '', -- ''
    queue bool not null default true,
    max_connections int not null default 10,
    recovery_logs bool not null default true,
    capture_logs bool not null default false
--  sign, approve, cadence
);

comment on table @extschema@.rppd_function is 'Python function code, linked table and topic. If modify, than the cache will be refreshed.';
comment on column @extschema@.rppd_function.schema_table is 'The rppd_event() must trigger on the corresponding "schema_table" to fire the function, see topic to identify execution order';
comment on column @extschema@.rppd_function.topic is '"": queue/topic per table (see schema_table); ".{column}": queue per value of the column "id" in this table; "{any_name}": global queue i.e. multiple tables can share the same queue';
comment on column @extschema@.rppd_function.max_connections is 'max_connections concurrent events execution on the topic to make a queue';
comment on column @extschema@.rppd_function.queue is 'if queue, then perform consequence events execution for the same topic. Ex: if the "topic" is ".id" and not queue, then events for same row will execute in parallel';

CREATE TRIGGER @extschema@_rppd_function_event AFTER INSERT OR UPDATE OR DELETE ON
    @extschema@.rppd_function FOR EACH ROW EXECUTE PROCEDURE @extschema@.rppd_event();


-- function log
CREATE table if not exists @extschema@.rppd_function_log (
    id bigserial primary key,
    node_id int not null,
    fn_id int not null,
    trig_value bigint,
    trig_type int not null default 0,
    finished_at timestamp, -- not null default current_timestamp,
    took_sec int not null default 0,
    output_msg text,
    error_msg text
);

comment on table @extschema@.rppd_function_log is 'Python function code execution on app nodes';
comment on column @extschema@.rppd_function_log.node_id is 'The rppd_config.id as a reference to execution node';
comment on column @extschema@.rppd_function_log.fn_id is 'The rppd_function.id, but no FK to keep performance';
comment on column @extschema@.rppd_function_log.trig_value is 'The value of column to trigger if topic is ".{column}". Use stored value to load on restore queue on startup and continue. Must be column type int to continue otherwise will save null and not able to restore.';
comment on column @extschema@.rppd_function_log.trig_type is 'Type of event: Update = 0, Insert = 1, Delete = 2, Truncate = 3';
comment on column @extschema@.rppd_function_log.finished_at is 'The rppd_function finushed or NULL if it is not';
comment on column @extschema@.rppd_function_log.output_msg is 'The logged messages configured in rppd_function.capture_logs';
comment on column @extschema@.rppd_function_log.error_msg is 'The error indicator. Must be null if completed OK';


-- queue
CREATE table if not exists @extschema@.rppd_queue (
    id serial primary key,
    queue varchar(64) not null,
    schema_table varchar(64) not null,
    column_timestamp varchar(64) not null,
    cron varchar(64) not null,
    finished_at timestamp,
    started_at timestamp not null default current_timestamp
);

comment on table @extschema@.rppd_queue is 'Queue configuration';

comment on column @extschema@.rppd_queue.column_timestamp is 'The table field to check on periodic event fire';
comment on column @extschema@.rppd_queue.cron is 'time schedule';

CREATE TRIGGER @extschema@_rppd_queue_event AFTER INSERT OR UPDATE OR DELETE ON
    @extschema@.rppd_queue FOR EACH ROW EXECUTE PROCEDURE @extschema@.rppd_event();
