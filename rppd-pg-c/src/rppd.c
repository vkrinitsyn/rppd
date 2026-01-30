/*
 * rppd.c - RPPD PostgreSQL Extension
 *
 * PostgreSQL trigger extension for RPPD (Remote Python Procedure Daemon)
 * C implementation following https://www.postgresql.org/docs/current/xfunc-c.html
 */

#include "rppd.h"

#include "access/htup_details.h"
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* PostgreSQL module magic */
PG_MODULE_MAGIC;

/* Global configuration */
RppdConfig g_rppd_config = {
    .server_path = "",
    .loaded = false,
    .connected = false,
    .last_error = "",
    .mutex = PTHREAD_MUTEX_INITIALIZER
};

/* Function declarations */
void _PG_init(void);
void _PG_fini(void);

PG_FUNCTION_INFO_V1(rppd_event);
PG_FUNCTION_INFO_V1(rppd_info);
PG_FUNCTION_INFO_V1(rppd_info_cfg);
PG_FUNCTION_INFO_V1(rppd_info_node);
PG_FUNCTION_INFO_V1(rppd_info_node_cfg);
PG_FUNCTION_INFO_V1(rppd_info_id);
PG_FUNCTION_INFO_V1(rppd_info_id_cfg);
PG_FUNCTION_INFO_V1(rppd_info_uuid);
PG_FUNCTION_INFO_V1(rppd_info_uuid_cfg);

/*
 * Module initialization
 */
void
_PG_init(void)
{
    pthread_mutex_init(&g_rppd_config.mutex, NULL);
    elog(LOG, "rppd: extension loaded (version %s)", RPPD_VERSION);
}

/*
 * Module cleanup
 */
void
_PG_fini(void)
{
    rppd_grpc_disconnect();
    pthread_mutex_destroy(&g_rppd_config.mutex);
    elog(LOG, "rppd: extension unloaded");
}

/*
 * Get GUC variable as string
 */
char *
rppd_get_guc_string(const char *name)
{
    const char *val = GetConfigOption(name, true, false);
    if (val == NULL)
        return NULL;
    return pstrdup(val);
}

/*
 * Check for endless loop (stub for future use)
 */
bool
rppd_is_endless_loop(void)
{
    return false;
}

/*
 * Build SQL query to get host from config table
 */
static char *
build_host_query(const char *config_schema_table)
{
    StringInfoData sql;
    const char *schema_table;

    initStringInfo(&sql);

    if (config_schema_table == NULL || strlen(config_schema_table) == 0)
    {
        schema_table = "public." RPPD_CFG_TABLE;
    }
    else if (config_schema_table[0] == '.')
    {
        appendStringInfo(&sql, "public%s", config_schema_table);
        schema_table = sql.data;
    }
    else if (config_schema_table[strlen(config_schema_table) - 1] == '.')
    {
        appendStringInfo(&sql, "%s%s", config_schema_table, RPPD_CFG_TABLE);
        schema_table = sql.data;
    }
    else
    {
        schema_table = config_schema_table;
    }

    resetStringInfo(&sql);
    appendStringInfo(&sql, "SELECT host FROM %s WHERE master", schema_table);

    return sql.data;
}

/*
 * Connect to RPPD server
 */
static int
do_connect(const char *host)
{
    char path[RPPD_MAX_HOST_LEN];

    if (strstr(host, "://") != NULL)
    {
        snprintf(path, sizeof(path), "%s", host);
    }
    else
    {
        snprintf(path, sizeof(path), "http://%s", host);
    }

    return rppd_grpc_connect(path);
}

/*
 * Check if reconnection is needed
 */
static bool
needs_reconnect(const char *host)
{
    pthread_mutex_lock(&g_rppd_config.mutex);

    if (!g_rppd_config.connected)
    {
        pthread_mutex_unlock(&g_rppd_config.mutex);
        return true;
    }

    if (strcmp(g_rppd_config.server_path, host) != 0)
    {
        pthread_mutex_unlock(&g_rppd_config.mutex);
        return true;
    }

    pthread_mutex_unlock(&g_rppd_config.mutex);
    return false;
}

/*
 * Get column value as int32 from HeapTuple
 */
static bool
get_column_int32(HeapTuple tuple, TupleDesc tupdesc, const char *colname, int32 *value)
{
    int attnum;
    Datum datum;
    bool isnull;

    attnum = SPI_fnumber(tupdesc, colname);
    if (attnum == SPI_ERROR_NOATTRIBUTE)
        return false;

    datum = SPI_getbinval(tuple, tupdesc, attnum, &isnull);
    if (isnull)
        return false;

    *value = DatumGetInt32(datum);
    return true;
}

/*
 * Get column value as int64 from HeapTuple
 */
static bool
get_column_int64(HeapTuple tuple, TupleDesc tupdesc, const char *colname, int64 *value)
{
    int attnum;
    Datum datum;
    bool isnull;

    attnum = SPI_fnumber(tupdesc, colname);
    if (attnum == SPI_ERROR_NOATTRIBUTE)
        return false;

    datum = SPI_getbinval(tuple, tupdesc, attnum, &isnull);
    if (isnull)
        return false;

    *value = DatumGetInt64(datum);
    return true;
}

/*
 * Get column value as string from HeapTuple
 */
static char *
get_column_string(HeapTuple tuple, TupleDesc tupdesc, const char *colname)
{
    int attnum;
    Datum datum;
    bool isnull;

    attnum = SPI_fnumber(tupdesc, colname);
    if (attnum == SPI_ERROR_NOATTRIBUTE)
        return NULL;

    datum = SPI_getbinval(tuple, tupdesc, attnum, &isnull);
    if (isnull)
        return NULL;

    return TextDatumGetCString(datum);
}

/*
 * Get column value as bool from HeapTuple
 */
static bool
get_column_bool(HeapTuple tuple, TupleDesc tupdesc, const char *colname, bool *value)
{
    int attnum;
    Datum datum;
    bool isnull;

    attnum = SPI_fnumber(tupdesc, colname);
    if (attnum == SPI_ERROR_NOATTRIBUTE)
        return false;

    datum = SPI_getbinval(tuple, tupdesc, attnum, &isnull);
    if (isnull)
        return false;

    *value = DatumGetBool(datum);
    return true;
}

/*
 * Load host from database
 */
static char *
load_host_from_db(void)
{
    char *sql;
    char *host = NULL;
    int ret;

    sql = build_host_query(NULL);

    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT)
    {
        elog(WARNING, "rppd: SPI_connect failed: %d", ret);
        return NULL;
    }

    ret = SPI_execute(sql, true, 1);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        HeapTuple tuple = SPI_tuptable->vals[0];
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        host = get_column_string(tuple, tupdesc, "host");
        if (host != NULL)
            host = pstrdup(host);
    }

    SPI_finish();
    pfree(sql);

    return host;
}

/*
 * Make gRPC event call
 */
static int
call_event(const DbEventRequest *req, DbEventResponse *resp)
{
    return rppd_grpc_event(req, resp);
}

/*
 * Trigger function: rppd_event
 */
Datum
rppd_event(PG_FUNCTION_ARGS)
{
    TriggerData    *trigdata = (TriggerData *) fcinfo->context;
    HeapTuple       rettuple;
    HeapTuple       current_tuple;
    TupleDesc       tupdesc;
    Relation        rel;
    char           *table_name;
    char           *schema_name;
    char           *full_table_name;
    char           *guc_node;
    DbAction        event_type;
    DbEventRequest  event_req;
    DbEventResponse event_resp;
    PkColumn        pk;
    int32           id_value;
    int             ret;

    /* Verify this is being called as a trigger */
    if (!CALLED_AS_TRIGGER(fcinfo))
        elog(ERROR, "rppd_event: not called as trigger");

    /* Get trigger context */
    rel = trigdata->tg_relation;
    tupdesc = RelationGetDescr(rel);

    /* Get current tuple (new for INSERT/UPDATE, old for DELETE) */
    if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
    {
        current_tuple = trigdata->tg_trigtuple;
        rettuple = trigdata->tg_trigtuple;
    }
    else
    {
        current_tuple = trigdata->tg_newtuple ? trigdata->tg_newtuple : trigdata->tg_trigtuple;
        rettuple = current_tuple;
    }

    /* Check if rppd.node=local - if so, skip processing */
    guc_node = rppd_get_guc_string(RPPD_NODE_GUC);
    if (guc_node != NULL && strcmp(guc_node, RPPD_LOCAL) == 0)
    {
        pfree(guc_node);
        return PointerGetDatum(rettuple);
    }
    if (guc_node != NULL)
        pfree(guc_node);

    /* Check for endless loop */
    if (rppd_is_endless_loop())
        return PointerGetDatum(rettuple);

    /* Get table info */
    table_name = RelationGetRelationName(rel);
    schema_name = get_namespace_name(RelationGetNamespace(rel));
    full_table_name = psprintf("%s.%s", schema_name, table_name);

    /* Determine event type */
    if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
        event_type = DB_ACTION_UPDATE;
    else if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
        event_type = DB_ACTION_INSERT;
    else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
        event_type = DB_ACTION_DELETE;
    else
        event_type = DB_ACTION_TRUNCATE;

    /* Try to get "id" column as PK */
    memset(&pk, 0, sizeof(pk));
    pk.column_name = "id";
    pk.column_type = PK_TYPE_INT;
    if (get_column_int32(current_tuple, tupdesc, "id", &id_value))
    {
        pk.has_value = true;
        pk.value.int_value = id_value;
    }

    /* Check if we need to load/reconnect */
    if (!g_rppd_config.loaded || strcmp(table_name, RPPD_CFG_TABLE) == 0)
    {
        char *host = NULL;
        bool is_master = false;

        /* If this is the config table, get host from the row */
        if (strcmp(table_name, RPPD_CFG_TABLE) == 0)
        {
            get_column_bool(current_tuple, tupdesc, "master", &is_master);
            if (is_master)
            {
                host = get_column_string(current_tuple, tupdesc, "host");
                if (host != NULL)
                {
                    /* Add default port if not present */
                    if (strchr(host, ':') == NULL)
                    {
                        char *host_with_port = psprintf("%s:8881", host);
                        pfree(host);
                        host = host_with_port;
                    }
                }
            }
        }

        /* Load from DB if not already loaded and not from config table */
        if (host == NULL && !g_rppd_config.loaded)
        {
            host = load_host_from_db();
        }

        /* Connect if we have a host and need to reconnect */
        if (host != NULL && needs_reconnect(host))
        {
            elog(NOTICE, "rppd: connecting to RPPD server at %s", host);
            ret = do_connect(host);
            if (ret != 0)
            {
                elog(WARNING, "rppd: connection failed to %s: %s",
                     host, rppd_grpc_last_error());
                pfree(host);
                pfree(full_table_name);
                ereport(ERROR,
                        (errcode(ERRCODE_CONNECTION_FAILURE),
                         errmsg("rppd: failed to connect to server")));
            }

            pthread_mutex_lock(&g_rppd_config.mutex);
            strncpy(g_rppd_config.server_path, host, RPPD_MAX_HOST_LEN - 1);
            g_rppd_config.loaded = true;
            g_rppd_config.connected = true;
            pthread_mutex_unlock(&g_rppd_config.mutex);
        }

        if (host != NULL)
            pfree(host);
    }

    /* Build event request */
    memset(&event_req, 0, sizeof(event_req));
    event_req.table_name = full_table_name;
    event_req.event_type = event_type;
    event_req.id_value = false;
    event_req.has_caller = false;
    event_req.pks = &pk;
    event_req.pks_count = pk.has_value ? 1 : 0;

    /* Make the call */
    memset(&event_resp, 0, sizeof(event_resp));
    ret = call_event(&event_req, &event_resp);

    if (ret != 0)
    {
        const char *error = rppd_grpc_last_error();
        char *host_copy;

        pthread_mutex_lock(&g_rppd_config.mutex);
        host_copy = pstrdup(g_rppd_config.server_path);
        g_rppd_config.connected = false;
        strncpy(g_rppd_config.last_error, error, RPPD_MAX_ERROR_LEN - 1);
        pthread_mutex_unlock(&g_rppd_config.mutex);

        elog(WARNING, "rppd: event call failed to [%s]: %s (will retry on next call)",
             host_copy, error);
        pfree(host_copy);
        pfree(full_table_name);

        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                 errmsg("rppd: event call failed")));
    }

    /* Handle repeat_with columns if present */
    if (event_resp.repeat_with_count > 0)
    {
        int i;
        PkColumn *all_pks;
        int total_pks;
        DbEventRequest repeat_req;
        DbEventResponse repeat_resp;

        total_pks = event_req.pks_count + event_resp.repeat_with_count;
        all_pks = palloc(sizeof(PkColumn) * total_pks);

        /* Copy original PKs */
        for (i = 0; i < event_req.pks_count; i++)
            all_pks[i] = event_req.pks[i];

        /* Add repeat_with columns with values from current tuple */
        for (i = 0; i < event_resp.repeat_with_count; i++)
        {
            PkColumn *col = &event_resp.repeat_with[i];
            PkColumn *new_pk = &all_pks[event_req.pks_count + i];

            new_pk->column_name = col->column_name;
            new_pk->column_type = col->column_type;
            new_pk->has_value = false;

            if (col->column_type == PK_TYPE_BIGINT)
            {
                int64 val;
                if (get_column_int64(current_tuple, tupdesc, col->column_name, &val))
                {
                    new_pk->has_value = true;
                    new_pk->value.bigint_value = val;
                }
            }
            else
            {
                int32 val;
                if (get_column_int32(current_tuple, tupdesc, col->column_name, &val))
                {
                    new_pk->has_value = true;
                    new_pk->value.int_value = val;
                }
            }
        }

        /* Make second call with id_value=true */
        memset(&repeat_req, 0, sizeof(repeat_req));
        repeat_req.table_name = full_table_name;
        repeat_req.event_type = event_type;
        repeat_req.id_value = true;
        repeat_req.has_caller = false;
        repeat_req.pks = all_pks;
        repeat_req.pks_count = total_pks;

        memset(&repeat_resp, 0, sizeof(repeat_resp));
        call_event(&repeat_req, &repeat_resp);

        rppd_pb_free_event_response(&repeat_resp);
        pfree(all_pks);
    }

    rppd_pb_free_event_response(&event_resp);
    pfree(full_table_name);

    return PointerGetDatum(rettuple);
}

/*
 * Helper to format JSON response
 */
static char *
wrap_json_error(const char *error)
{
    return psprintf("{ \"error\":\"%s\" }", error);
}

/*
 * Helper to format status response as JSON
 */
static char *
format_status_response(const StatusResponse *resp)
{
    StringInfoData json;

    initStringInfo(&json);
    appendStringInfo(&json, "{\"node_id\":%d,\"is_master\":%s,\"queued\":%d,\"in_proc\":%d,\"pool\":%d",
                     resp->node_id,
                     resp->is_master ? "true" : "false",
                     resp->queued,
                     resp->in_proc,
                     resp->pool);

    if (resp->has_status)
    {
        appendStringInfo(&json, ",\"status\":{");
        if (resp->status.has_queue_pos)
            appendStringInfo(&json, "\"queue_pos\":%u", resp->status.queue_pos);
        else if (resp->status.has_in_proc_sec)
            appendStringInfo(&json, "\"in_proc_sec\":%u", resp->status.in_proc_sec);
        else if (resp->status.has_remote_host)
            appendStringInfo(&json, "\"remote_host\":%d", resp->status.remote_host);
        appendStringInfoChar(&json, '}');
    }

    if (resp->uuids_count > 0)
    {
        int i;
        appendStringInfo(&json, ",\"uuids\":[");
        for (i = 0; i < resp->uuids_count; i++)
        {
            if (i > 0)
                appendStringInfoChar(&json, ',');
            appendStringInfo(&json, "\"%s\"", resp->uuids[i]);
        }
        appendStringInfoChar(&json, ']');
    }

    appendStringInfoChar(&json, '}');

    return json.data;
}

/*
 * Implementation of rppd_info functions
 */
static char *
rppd_info_impl(const StatusRequest *req)
{
    StatusResponse resp;
    char *host;
    char *result;
    int ret;

    /* Check if connected, if not load host from DB */
    pthread_mutex_lock(&g_rppd_config.mutex);
    if (!g_rppd_config.connected)
    {
        pthread_mutex_unlock(&g_rppd_config.mutex);

        host = load_host_from_db();
        if (host == NULL)
        {
            return wrap_json_error("Failed to load host from database");
        }

        ret = do_connect(host);
        if (ret != 0)
        {
            result = psprintf("{ \"error\":\"%s\", \"server\":\"%s\" }",
                              rppd_grpc_last_error(), host);
            pfree(host);
            return result;
        }

        pthread_mutex_lock(&g_rppd_config.mutex);
        strncpy(g_rppd_config.server_path, host, RPPD_MAX_HOST_LEN - 1);
        g_rppd_config.connected = true;
        pthread_mutex_unlock(&g_rppd_config.mutex);

        pfree(host);
    }
    else
    {
        pthread_mutex_unlock(&g_rppd_config.mutex);
    }

    /* Make status call */
    memset(&resp, 0, sizeof(resp));
    ret = rppd_grpc_status(req, &resp);

    if (ret != 0)
    {
        return wrap_json_error(rppd_grpc_last_error());
    }

    result = format_status_response(&resp);
    rppd_pb_free_status_response(&resp);

    return result;
}

/*
 * rppd_info() - basic status
 */
Datum
rppd_info(PG_FUNCTION_ARGS)
{
    StatusRequest req;
    char *result;

    memset(&req, 0, sizeof(req));
    req.config_schema_table = "";
    req.node_id = -1;

    result = rppd_info_impl(&req);
    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * rppd_info(cfg text) - status with config table
 */
Datum
rppd_info_cfg(PG_FUNCTION_ARGS)
{
    StatusRequest req;
    char *result;
    text *cfg_text = PG_GETARG_TEXT_PP(0);

    memset(&req, 0, sizeof(req));
    req.config_schema_table = text_to_cstring(cfg_text);
    req.node_id = -1;

    result = rppd_info_impl(&req);
    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * rppd_info(node_id int) - status for specific node
 */
Datum
rppd_info_node(PG_FUNCTION_ARGS)
{
    StatusRequest req;
    char *result;

    memset(&req, 0, sizeof(req));
    req.config_schema_table = "";
    req.node_id = PG_GETARG_INT32(0);

    result = rppd_info_impl(&req);
    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * rppd_info(cfg text, node_id int) - status for specific node with config
 */
Datum
rppd_info_node_cfg(PG_FUNCTION_ARGS)
{
    StatusRequest req;
    char *result;
    text *cfg_text = PG_GETARG_TEXT_PP(0);

    memset(&req, 0, sizeof(req));
    req.config_schema_table = text_to_cstring(cfg_text);
    req.node_id = PG_GETARG_INT32(1);

    result = rppd_info_impl(&req);
    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * rppd_info(node_id int, fn_log_id bigint) - status with function log ID
 */
Datum
rppd_info_id(PG_FUNCTION_ARGS)
{
    StatusRequest req;
    char *result;

    memset(&req, 0, sizeof(req));
    req.config_schema_table = "";
    req.node_id = PG_GETARG_INT32(0);
    req.has_fn_log_id = true;
    req.fn_log_id = PG_GETARG_INT64(1);

    result = rppd_info_impl(&req);
    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * rppd_info(cfg text, node_id int, fn_log_id bigint)
 */
Datum
rppd_info_id_cfg(PG_FUNCTION_ARGS)
{
    StatusRequest req;
    char *result;
    text *cfg_text = PG_GETARG_TEXT_PP(0);

    memset(&req, 0, sizeof(req));
    req.config_schema_table = text_to_cstring(cfg_text);
    req.node_id = PG_GETARG_INT32(1);
    req.has_fn_log_id = true;
    req.fn_log_id = PG_GETARG_INT64(2);

    result = rppd_info_impl(&req);
    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * rppd_info(node_id int, fn_log_uuid text) - status with function UUID
 */
Datum
rppd_info_uuid(PG_FUNCTION_ARGS)
{
    StatusRequest req;
    char *result;
    text *uuid_text = PG_GETARG_TEXT_PP(1);

    memset(&req, 0, sizeof(req));
    req.config_schema_table = "";
    req.node_id = PG_GETARG_INT32(0);
    req.has_uuid = true;
    req.uuid = text_to_cstring(uuid_text);

    result = rppd_info_impl(&req);
    PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * rppd_info(cfg text, node_id int, fn_log_uuid text)
 */
Datum
rppd_info_uuid_cfg(PG_FUNCTION_ARGS)
{
    StatusRequest req;
    char *result;
    text *cfg_text = PG_GETARG_TEXT_PP(0);
    text *uuid_text = PG_GETARG_TEXT_PP(2);

    memset(&req, 0, sizeof(req));
    req.config_schema_table = text_to_cstring(cfg_text);
    req.node_id = PG_GETARG_INT32(1);
    req.has_uuid = true;
    req.uuid = text_to_cstring(uuid_text);

    result = rppd_info_impl(&req);
    PG_RETURN_TEXT_P(cstring_to_text(result));
}
