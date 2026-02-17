/*
 * protobuf.c - Manual Protocol Buffer Encoding/Decoding
 *
 * Implements protobuf wire format for RPPD messages
 * Reference: https://developers.google.com/protocol-buffers/docs/encoding
 */

#include "rppd.h"

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Wire types */
#define WIRE_VARINT 0
#define WIRE_64BIT 1
#define WIRE_LENGTH 2
#define WIRE_32BIT 5

/* Buffer for building messages */
typedef struct {
    uint8_t *data;
    size_t size;
    size_t capacity;
} EncodeBuffer;

/* Buffer for reading messages */
typedef struct {
    const uint8_t *data;
    size_t size;
    size_t pos;
} DecodeBuffer;

/*
 * Encoding helpers
 */

static int
buf_ensure(EncodeBuffer *buf, size_t needed)
{
    if (buf->size + needed > buf->capacity)
    {
        uint8_t *new_data;
        size_t new_cap = buf->capacity * 2;
        if (new_cap < buf->size + needed)
            new_cap = buf->size + needed + 256;

        new_data = realloc(buf->data, new_cap);
        if (new_data == NULL)
            return -1;

        buf->data = new_data;
        buf->capacity = new_cap;
    }
    return 0;
}

static int
buf_write_byte(EncodeBuffer *buf, uint8_t b)
{
    if (buf_ensure(buf, 1) != 0)
        return -1;
    buf->data[buf->size++] = b;
    return 0;
}

static int
buf_write_varint(EncodeBuffer *buf, uint64_t val)
{
    do {
        uint8_t b = val & 0x7F;
        val >>= 7;
        if (val != 0)
            b |= 0x80;
        if (buf_write_byte(buf, b) != 0)
            return -1;
    } while (val != 0);
    return 0;
}

static int __attribute__((unused))
buf_write_svarint(EncodeBuffer *buf, int64_t val)
{
    /* ZigZag encoding */
    uint64_t uval = (uint64_t)((val << 1) ^ (val >> 63));
    return buf_write_varint(buf, uval);
}

static int
buf_write_tag(EncodeBuffer *buf, int field, int wire_type)
{
    return buf_write_varint(buf, (uint64_t)((field << 3) | wire_type));
}

static int
buf_write_bytes(EncodeBuffer *buf, const uint8_t *data, size_t len)
{
    if (buf_ensure(buf, len) != 0)
        return -1;
    memcpy(buf->data + buf->size, data, len);
    buf->size += len;
    return 0;
}

static int
buf_write_string(EncodeBuffer *buf, int field, const char *str)
{
    size_t len;
    if (str == NULL)
        return 0;

    len = strlen(str);
    if (len == 0)
        return 0;

    if (buf_write_tag(buf, field, WIRE_LENGTH) != 0)
        return -1;
    if (buf_write_varint(buf, len) != 0)
        return -1;
    return buf_write_bytes(buf, (const uint8_t *)str, len);
}

static int
buf_write_int32(EncodeBuffer *buf, int field, int32_t val)
{
    if (val == 0)
        return 0;
    if (buf_write_tag(buf, field, WIRE_VARINT) != 0)
        return -1;
    return buf_write_varint(buf, (uint64_t)(uint32_t)val);
}

static int
buf_write_int64(EncodeBuffer *buf, int field, int64_t val)
{
    if (val == 0)
        return 0;
    if (buf_write_tag(buf, field, WIRE_VARINT) != 0)
        return -1;
    return buf_write_varint(buf, (uint64_t)val);
}

static int
buf_write_bool(EncodeBuffer *buf, int field, bool val)
{
    if (!val)
        return 0;
    if (buf_write_tag(buf, field, WIRE_VARINT) != 0)
        return -1;
    return buf_write_byte(buf, 1);
}

static int
buf_write_enum(EncodeBuffer *buf, int field, int val)
{
    return buf_write_int32(buf, field, val);
}

static int
buf_write_submessage(EncodeBuffer *buf, int field, const uint8_t *data, size_t len)
{
    if (buf_write_tag(buf, field, WIRE_LENGTH) != 0)
        return -1;
    if (buf_write_varint(buf, len) != 0)
        return -1;
    return buf_write_bytes(buf, data, len);
}

/*
 * Decoding helpers
 */

static int
dec_read_byte(DecodeBuffer *buf, uint8_t *b)
{
    if (buf->pos >= buf->size)
        return -1;
    *b = buf->data[buf->pos++];
    return 0;
}

static int
dec_read_varint(DecodeBuffer *buf, uint64_t *val)
{
    uint64_t result = 0;
    int shift = 0;
    uint8_t b;

    do {
        if (dec_read_byte(buf, &b) != 0)
            return -1;
        result |= (uint64_t)(b & 0x7F) << shift;
        shift += 7;
    } while (b & 0x80);

    *val = result;
    return 0;
}

static int __attribute__((unused))
dec_read_svarint(DecodeBuffer *buf, int64_t *val)
{
    uint64_t uval;
    if (dec_read_varint(buf, &uval) != 0)
        return -1;
    /* ZigZag decode */
    *val = (int64_t)((uval >> 1) ^ -(int64_t)(uval & 1));
    return 0;
}

static int
dec_read_tag(DecodeBuffer *buf, int *field, int *wire_type)
{
    uint64_t tag;
    if (dec_read_varint(buf, &tag) != 0)
        return -1;
    *field = (int)(tag >> 3);
    *wire_type = (int)(tag & 0x07);
    return 0;
}

static int
dec_skip_field(DecodeBuffer *buf, int wire_type)
{
    uint64_t val;

    switch (wire_type)
    {
        case WIRE_VARINT:
            return dec_read_varint(buf, &val);
        case WIRE_64BIT:
            if (buf->pos + 8 > buf->size)
                return -1;
            buf->pos += 8;
            return 0;
        case WIRE_LENGTH:
            if (dec_read_varint(buf, &val) != 0)
                return -1;
            if (buf->pos + val > buf->size)
                return -1;
            buf->pos += val;
            return 0;
        case WIRE_32BIT:
            if (buf->pos + 4 > buf->size)
                return -1;
            buf->pos += 4;
            return 0;
        default:
            return -1;
    }
}

static char *
dec_read_string(DecodeBuffer *buf)
{
    uint64_t len;
    char *str;

    if (dec_read_varint(buf, &len) != 0)
        return NULL;
    if (buf->pos + len > buf->size)
        return NULL;

    str = malloc(len + 1);
    if (str == NULL)
        return NULL;

    memcpy(str, buf->data + buf->pos, len);
    str[len] = '\0';
    buf->pos += len;

    return str;
}

static int
dec_read_bytes(DecodeBuffer *buf, uint8_t **data, size_t *len)
{
    uint64_t length;

    if (dec_read_varint(buf, &length) != 0)
        return -1;
    if (buf->pos + length > buf->size)
        return -1;

    *data = (uint8_t *)(buf->data + buf->pos);
    *len = (size_t)length;
    buf->pos += length;

    return 0;
}

/*
 * Encode PkColumn submessage
 */
static size_t
encode_pk_column(const PkColumn *col, uint8_t **out)
{
    EncodeBuffer buf = {0};

    buf.capacity = 64;
    buf.data = malloc(buf.capacity);
    if (buf.data == NULL)
        return 0;

    /* field 1: column_name */
    buf_write_string(&buf, 1, col->column_name);

    /* field 2: column_type (enum) */
    buf_write_enum(&buf, 2, (int)col->column_type);

    /* field 5/6/7: pk_value oneof */
    if (col->has_value)
    {
        switch (col->column_type)
        {
            case PK_TYPE_INT:
                buf_write_int32(&buf, 5, col->value.int_value);
                break;
            case PK_TYPE_BIGINT:
                buf_write_int64(&buf, 6, col->value.bigint_value);
                break;
            case PK_TYPE_STRING:
                buf_write_string(&buf, 7, col->value.string_value);
                break;
        }
    }

    *out = buf.data;
    return buf.size;
}

/*
 * Decode PkColumn submessage
 */
static int
decode_pk_column(const uint8_t *data, size_t len, PkColumn *col)
{
    DecodeBuffer buf = {data, len, 0};
    int field, wire_type;

    memset(col, 0, sizeof(*col));

    while (buf.pos < buf.size)
    {
        if (dec_read_tag(&buf, &field, &wire_type) != 0)
            return -1;

        switch (field)
        {
            case 1: /* column_name */
                col->column_name = dec_read_string(&buf);
                break;
            case 2: /* column_type */
                {
                    uint64_t val;
                    if (dec_read_varint(&buf, &val) != 0)
                        return -1;
                    col->column_type = (PkColumnType)val;
                }
                break;
            case 5: /* int_value */
                {
                    uint64_t val;
                    if (dec_read_varint(&buf, &val) != 0)
                        return -1;
                    col->has_value = true;
                    col->value.int_value = (int32_t)val;
                }
                break;
            case 6: /* bigint_value */
                {
                    uint64_t val;
                    if (dec_read_varint(&buf, &val) != 0)
                        return -1;
                    col->has_value = true;
                    col->value.bigint_value = (int64_t)val;
                }
                break;
            case 7: /* string_value */
                col->has_value = true;
                col->value.string_value = dec_read_string(&buf);
                break;
            default:
                if (dec_skip_field(&buf, wire_type) != 0)
                    return -1;
        }
    }

    return 0;
}

/*
 * Encode DbEventRequest
 */
size_t
rppd_pb_encode_event_request(const DbEventRequest *req, uint8_t **out)
{
    EncodeBuffer buf = {0};
    int i;

    buf.capacity = 256;
    buf.data = malloc(buf.capacity);
    if (buf.data == NULL)
    {
        *out = NULL;
        return 0;
    }

    /* field 1: table_name */
    buf_write_string(&buf, 1, req->table_name);

    /* field 2: event_type (enum) */
    buf_write_enum(&buf, 2, (int)req->event_type);

    /* field 3: id_value */
    buf_write_bool(&buf, 3, req->id_value);

    /* field 4: call_by (oneof optional_caller) */
    if (req->has_caller)
        buf_write_int32(&buf, 4, req->call_by);

    /* field 5: pks (repeated) */
    for (i = 0; i < req->pks_count; i++)
    {
        uint8_t *pk_data = NULL;
        size_t pk_len;

        pk_len = encode_pk_column(&req->pks[i], &pk_data);
        if (pk_data != NULL)
        {
            buf_write_submessage(&buf, 5, pk_data, pk_len);
            free(pk_data);
        }
    }

    *out = buf.data;
    return buf.size;
}

/*
 * Decode DbEventResponse
 */
int
rppd_pb_decode_event_response(const uint8_t *data, size_t len, DbEventResponse *resp)
{
    DecodeBuffer buf = {data, len, 0};
    int field, wire_type;
    PkColumn *repeat_with = NULL;
    int repeat_count = 0;
    int repeat_capacity = 0;

    memset(resp, 0, sizeof(*resp));

    while (buf.pos < buf.size)
    {
        if (dec_read_tag(&buf, &field, &wire_type) != 0)
            return -1;

        switch (field)
        {
            case 1: /* saved */
                {
                    uint64_t val;
                    if (dec_read_varint(&buf, &val) != 0)
                        return -1;
                    resp->saved = (val != 0);
                }
                break;
            case 2: /* repeat_with */
                {
                    uint8_t *sub_data;
                    size_t sub_len;

                    if (dec_read_bytes(&buf, &sub_data, &sub_len) != 0)
                        return -1;

                    /* Grow array if needed */
                    if (repeat_count >= repeat_capacity)
                    {
                        int new_cap = repeat_capacity == 0 ? 4 : repeat_capacity * 2;
                        PkColumn *new_arr = realloc(repeat_with, sizeof(PkColumn) * new_cap);
                        if (new_arr == NULL)
                            return -1;
                        repeat_with = new_arr;
                        repeat_capacity = new_cap;
                    }

                    if (decode_pk_column(sub_data, sub_len, &repeat_with[repeat_count]) != 0)
                        return -1;
                    repeat_count++;
                }
                break;
            default:
                if (dec_skip_field(&buf, wire_type) != 0)
                    return -1;
        }
    }

    resp->repeat_with = repeat_with;
    resp->repeat_with_count = repeat_count;

    return 0;
}

/*
 * Encode StatusRequest
 */
size_t
rppd_pb_encode_status_request(const StatusRequest *req, uint8_t **out)
{
    EncodeBuffer buf = {0};

    buf.capacity = 128;
    buf.data = malloc(buf.capacity);
    if (buf.data == NULL)
    {
        *out = NULL;
        return 0;
    }

    /* field 1: config_schema_table */
    buf_write_string(&buf, 1, req->config_schema_table);

    /* field 2: node_id */
    buf_write_int32(&buf, 2, req->node_id);

    /* field 3: uuid (oneof fn_log) */
    if (req->has_uuid)
        buf_write_string(&buf, 3, req->uuid);

    /* field 4: fn_log_id (oneof fn_log) */
    if (req->has_fn_log_id)
        buf_write_int64(&buf, 4, req->fn_log_id);

    *out = buf.data;
    return buf.size;
}

/*
 * Decode FnStatus submessage
 */
static int
decode_fn_status(const uint8_t *data, size_t len, FnStatus *status)
{
    DecodeBuffer buf = {data, len, 0};
    int field, wire_type;

    memset(status, 0, sizeof(*status));

    while (buf.pos < buf.size)
    {
        if (dec_read_tag(&buf, &field, &wire_type) != 0)
            return -1;

        switch (field)
        {
            case 1: /* queue_pos */
                {
                    uint64_t val;
                    if (dec_read_varint(&buf, &val) != 0)
                        return -1;
                    status->has_queue_pos = true;
                    status->queue_pos = (uint32_t)val;
                }
                break;
            case 2: /* in_proc_sec */
                {
                    uint64_t val;
                    if (dec_read_varint(&buf, &val) != 0)
                        return -1;
                    status->has_in_proc_sec = true;
                    status->in_proc_sec = (uint32_t)val;
                }
                break;
            case 3: /* remote_host */
                {
                    uint64_t val;
                    if (dec_read_varint(&buf, &val) != 0)
                        return -1;
                    status->has_remote_host = true;
                    status->remote_host = (int32_t)val;
                }
                break;
            default:
                if (dec_skip_field(&buf, wire_type) != 0)
                    return -1;
        }
    }

    return 0;
}

/*
 * Decode StatusFnsResponse (uuid list)
 */
static int
decode_status_fns_response(const uint8_t *data, size_t len, char ***uuids, int *count)
{
    DecodeBuffer buf = {data, len, 0};
    int field, wire_type;
    char **arr = NULL;
    int arr_count = 0;
    int arr_capacity = 0;

    while (buf.pos < buf.size)
    {
        if (dec_read_tag(&buf, &field, &wire_type) != 0)
            return -1;

        if (field == 1) /* uuid */
        {
            char *uuid = dec_read_string(&buf);
            if (uuid == NULL)
                return -1;

            if (arr_count >= arr_capacity)
            {
                int new_cap = arr_capacity == 0 ? 4 : arr_capacity * 2;
                char **new_arr = realloc(arr, sizeof(char *) * new_cap);
                if (new_arr == NULL)
                {
                    free(uuid);
                    return -1;
                }
                arr = new_arr;
                arr_capacity = new_cap;
            }

            arr[arr_count++] = uuid;
        }
        else
        {
            if (dec_skip_field(&buf, wire_type) != 0)
                return -1;
        }
    }

    *uuids = arr;
    *count = arr_count;
    return 0;
}

/*
 * Decode StatusResponse
 */
int
rppd_pb_decode_status_response(const uint8_t *data, size_t len, StatusResponse *resp)
{
    DecodeBuffer buf = {data, len, 0};
    int field, wire_type;

    memset(resp, 0, sizeof(*resp));

    while (buf.pos < buf.size)
    {
        if (dec_read_tag(&buf, &field, &wire_type) != 0)
            return -1;

        switch (field)
        {
            case 1: /* node_id */
                {
                    uint64_t val;
                    if (dec_read_varint(&buf, &val) != 0)
                        return -1;
                    resp->node_id = (int32_t)val;
                }
                break;
            case 2: /* is_master */
                {
                    uint64_t val;
                    if (dec_read_varint(&buf, &val) != 0)
                        return -1;
                    resp->is_master = (val != 0);
                }
                break;
            case 3: /* queued */
                {
                    uint64_t val;
                    if (dec_read_varint(&buf, &val) != 0)
                        return -1;
                    resp->queued = (int32_t)val;
                }
                break;
            case 4: /* in_proc */
                {
                    uint64_t val;
                    if (dec_read_varint(&buf, &val) != 0)
                        return -1;
                    resp->in_proc = (int32_t)val;
                }
                break;
            case 5: /* pool */
                {
                    uint64_t val;
                    if (dec_read_varint(&buf, &val) != 0)
                        return -1;
                    resp->pool = (int32_t)val;
                }
                break;
            case 6: /* status (oneof fn_log) */
                {
                    uint8_t *sub_data;
                    size_t sub_len;

                    if (dec_read_bytes(&buf, &sub_data, &sub_len) != 0)
                        return -1;

                    resp->has_status = true;
                    if (decode_fn_status(sub_data, sub_len, &resp->status) != 0)
                        return -1;
                }
                break;
            case 7: /* uuid (oneof fn_log) */
                {
                    uint8_t *sub_data;
                    size_t sub_len;

                    if (dec_read_bytes(&buf, &sub_data, &sub_len) != 0)
                        return -1;

                    if (decode_status_fns_response(sub_data, sub_len,
                                                   &resp->uuids, &resp->uuids_count) != 0)
                        return -1;
                }
                break;
            default:
                if (dec_skip_field(&buf, wire_type) != 0)
                    return -1;
        }
    }

    return 0;
}

/*
 * Free DbEventResponse
 */
void
rppd_pb_free_event_response(DbEventResponse *resp)
{
    int i;

    if (resp->repeat_with != NULL)
    {
        for (i = 0; i < resp->repeat_with_count; i++)
        {
            if (resp->repeat_with[i].column_name != NULL)
                free(resp->repeat_with[i].column_name);
            if (resp->repeat_with[i].column_type == PK_TYPE_STRING &&
                resp->repeat_with[i].value.string_value != NULL)
                free(resp->repeat_with[i].value.string_value);
        }
        free(resp->repeat_with);
    }

    memset(resp, 0, sizeof(*resp));
}

/*
 * Free StatusResponse
 */
void
rppd_pb_free_status_response(StatusResponse *resp)
{
    int i;

    if (resp->uuids != NULL)
    {
        for (i = 0; i < resp->uuids_count; i++)
        {
            if (resp->uuids[i] != NULL)
                free(resp->uuids[i]);
        }
        free(resp->uuids);
    }

    memset(resp, 0, sizeof(*resp));
}
