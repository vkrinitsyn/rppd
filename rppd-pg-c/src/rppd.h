/*
 * rppd.h - RPPD PostgreSQL Extension Header
 *
 * PostgreSQL trigger extension for RPPD (Remote Python Procedure Daemon)
 */

#ifndef RPPD_H
#define RPPD_H

#include "postgres.h"
#include "fmgr.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/guc.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"

#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>

/* Constants */
#define RPPD_VERSION "0.2.2"
#define RPPD_TIMEOUT_MS 100
#define RPPD_NODE_GUC "rppd.node"
#define RPPD_LOCAL "local"
#define RPPD_CFG_TABLE "rppd_config"
#define RPPD_MAX_HOST_LEN 256
#define RPPD_MAX_ERROR_LEN 512

/* DB Action types (matches proto enum) */
typedef enum {
    DB_ACTION_UPDATE = 0,
    DB_ACTION_INSERT = 1,
    DB_ACTION_DELETE = 2,
    DB_ACTION_TRUNCATE = 3,
    DB_ACTION_DUAL = 4
} DbAction;

/* PK Column types (matches proto enum) */
typedef enum {
    PK_TYPE_INT = 0,
    PK_TYPE_BIGINT = 1,
    PK_TYPE_STRING = 2
} PkColumnType;

/* Primary Key Column structure */
typedef struct {
    char *column_name;
    PkColumnType column_type;
    bool has_value;
    union {
        int32_t int_value;
        int64_t bigint_value;
        char *string_value;
    } value;
} PkColumn;

/* DB Event Request */
typedef struct {
    char *table_name;
    DbAction event_type;
    bool id_value;
    bool has_caller;
    int32_t call_by;
    PkColumn *pks;
    int pks_count;
} DbEventRequest;

/* DB Event Response */
typedef struct {
    bool saved;
    PkColumn *repeat_with;
    int repeat_with_count;
} DbEventResponse;

/* Status Request */
typedef struct {
    char *config_schema_table;
    int32_t node_id;
    bool has_fn_log_id;
    int64_t fn_log_id;
    bool has_uuid;
    char *uuid;
} StatusRequest;

/* Function Status */
typedef struct {
    bool has_queue_pos;
    uint32_t queue_pos;
    bool has_in_proc_sec;
    uint32_t in_proc_sec;
    bool has_remote_host;
    int32_t remote_host;
} FnStatus;

/* Status Response */
typedef struct {
    int32_t node_id;
    bool is_master;
    int32_t queued;
    int32_t in_proc;
    int32_t pool;
    bool has_status;
    FnStatus status;
    char **uuids;
    int uuids_count;
} StatusResponse;

/* Connection configuration */
typedef struct {
    char server_path[RPPD_MAX_HOST_LEN];
    bool loaded;
    bool connected;
    char last_error[RPPD_MAX_ERROR_LEN];
    pthread_mutex_t mutex;
} RppdConfig;

/* Global config */
extern RppdConfig g_rppd_config;

/* grpc_client.h functions */
int rppd_grpc_connect(const char *host);
void rppd_grpc_disconnect(void);
int rppd_grpc_event(const DbEventRequest *req, DbEventResponse *resp);
int rppd_grpc_status(const StatusRequest *req, StatusResponse *resp);
const char *rppd_grpc_last_error(void);

/* protobuf.h functions */
size_t rppd_pb_encode_event_request(const DbEventRequest *req, uint8_t **out);
int rppd_pb_decode_event_response(const uint8_t *data, size_t len, DbEventResponse *resp);
size_t rppd_pb_encode_status_request(const StatusRequest *req, uint8_t **out);
int rppd_pb_decode_status_response(const uint8_t *data, size_t len, StatusResponse *resp);
void rppd_pb_free_event_response(DbEventResponse *resp);
void rppd_pb_free_status_response(StatusResponse *resp);

/* Utility functions */
char *rppd_get_guc_string(const char *name);
bool rppd_is_endless_loop(void);

#endif /* RPPD_H */
