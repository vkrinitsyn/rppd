/*
 * grpc_client.c - gRPC Client Implementation using libcurl HTTP/2
 *
 * Implements gRPC-Web protocol over HTTP/2 using libcurl
 * Reference: https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md
 */

#include "rppd.h"

#include <curl/curl.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* gRPC frame header size */
#define GRPC_HEADER_SIZE 5

/* Response buffer */
typedef struct {
    uint8_t *data;
    size_t size;
    size_t capacity;
} ResponseBuffer;

/* Global state */
static CURL *g_curl = NULL;
static char g_host[RPPD_MAX_HOST_LEN] = "";
static char g_last_error[RPPD_MAX_ERROR_LEN] = "";
static pthread_mutex_t g_curl_mutex = PTHREAD_MUTEX_INITIALIZER;

/*
 * libcurl write callback
 */
static size_t
write_callback(void *contents, size_t size, size_t nmemb, void *userp)
{
    size_t realsize = size * nmemb;
    ResponseBuffer *buf = (ResponseBuffer *)userp;

    if (buf->size + realsize > buf->capacity)
    {
        size_t new_capacity = buf->capacity * 2;
        if (new_capacity < buf->size + realsize)
            new_capacity = buf->size + realsize + 1024;

        uint8_t *new_data = realloc(buf->data, new_capacity);
        if (new_data == NULL)
            return 0;

        buf->data = new_data;
        buf->capacity = new_capacity;
    }

    memcpy(buf->data + buf->size, contents, realsize);
    buf->size += realsize;

    return realsize;
}

/*
 * Initialize curl handle
 */
static int
init_curl(void)
{
    if (g_curl != NULL)
        return 0;

    curl_global_init(CURL_GLOBAL_ALL);
    g_curl = curl_easy_init();

    if (g_curl == NULL)
    {
        snprintf(g_last_error, sizeof(g_last_error), "Failed to initialize libcurl");
        return -1;
    }

    return 0;
}

/*
 * Build gRPC frame with length-prefixed message
 * Format: 1 byte compressed flag + 4 bytes big-endian length + message
 */
static uint8_t *
build_grpc_frame(const uint8_t *msg, size_t msg_len, size_t *frame_len)
{
    uint8_t *frame;
    size_t total_len = GRPC_HEADER_SIZE + msg_len;

    frame = malloc(total_len);
    if (frame == NULL)
        return NULL;

    /* Compression flag (0 = not compressed) */
    frame[0] = 0;

    /* Message length in big-endian */
    frame[1] = (msg_len >> 24) & 0xFF;
    frame[2] = (msg_len >> 16) & 0xFF;
    frame[3] = (msg_len >> 8) & 0xFF;
    frame[4] = msg_len & 0xFF;

    /* Copy message */
    if (msg_len > 0)
        memcpy(frame + GRPC_HEADER_SIZE, msg, msg_len);

    *frame_len = total_len;
    return frame;
}

/*
 * Parse gRPC frame and extract message
 */
static int
parse_grpc_frame(const uint8_t *frame, size_t frame_len, uint8_t **msg, size_t *msg_len)
{
    uint32_t len;

    if (frame_len < GRPC_HEADER_SIZE)
    {
        snprintf(g_last_error, sizeof(g_last_error), "Invalid gRPC frame: too short");
        return -1;
    }

    /* Check compression flag */
    if (frame[0] != 0)
    {
        snprintf(g_last_error, sizeof(g_last_error), "Compressed gRPC messages not supported");
        return -1;
    }

    /* Parse message length (big-endian) */
    len = ((uint32_t)frame[1] << 24) |
          ((uint32_t)frame[2] << 16) |
          ((uint32_t)frame[3] << 8) |
          (uint32_t)frame[4];

    if (frame_len < GRPC_HEADER_SIZE + len)
    {
        snprintf(g_last_error, sizeof(g_last_error), "Invalid gRPC frame: incomplete message");
        return -1;
    }

    *msg = (uint8_t *)(frame + GRPC_HEADER_SIZE);
    *msg_len = len;

    return 0;
}

/*
 * Make gRPC call
 */
static int
grpc_call(const char *method, const uint8_t *req_data, size_t req_len,
          uint8_t **resp_data, size_t *resp_len)
{
    CURLcode res;
    struct curl_slist *headers = NULL;
    ResponseBuffer response = {0};
    char url[512];
    uint8_t *frame;
    size_t frame_len;
    long http_code;

    pthread_mutex_lock(&g_curl_mutex);

    if (g_curl == NULL || strlen(g_host) == 0)
    {
        snprintf(g_last_error, sizeof(g_last_error), "Not connected");
        pthread_mutex_unlock(&g_curl_mutex);
        return -1;
    }

    /* Build URL */
    snprintf(url, sizeof(url), "%s%s", g_host, method);

    /* Initialize response buffer */
    response.capacity = 4096;
    response.data = malloc(response.capacity);
    if (response.data == NULL)
    {
        snprintf(g_last_error, sizeof(g_last_error), "Memory allocation failed");
        pthread_mutex_unlock(&g_curl_mutex);
        return -1;
    }
    response.size = 0;

    /* Build gRPC frame */
    frame = build_grpc_frame(req_data, req_len, &frame_len);
    if (frame == NULL)
    {
        free(response.data);
        snprintf(g_last_error, sizeof(g_last_error), "Failed to build gRPC frame");
        pthread_mutex_unlock(&g_curl_mutex);
        return -1;
    }

    /* Set headers for gRPC */
    headers = curl_slist_append(headers, "Content-Type: application/grpc");
    headers = curl_slist_append(headers, "TE: trailers");
    headers = curl_slist_append(headers, "grpc-encoding: identity");

    /* Configure curl */
    curl_easy_reset(g_curl);
    curl_easy_setopt(g_curl, CURLOPT_URL, url);
    curl_easy_setopt(g_curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
    curl_easy_setopt(g_curl, CURLOPT_POST, 1L);
    curl_easy_setopt(g_curl, CURLOPT_POSTFIELDS, frame);
    curl_easy_setopt(g_curl, CURLOPT_POSTFIELDSIZE, (long)frame_len);
    curl_easy_setopt(g_curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(g_curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(g_curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(g_curl, CURLOPT_TIMEOUT_MS, (long)RPPD_TIMEOUT_MS);
    curl_easy_setopt(g_curl, CURLOPT_CONNECTTIMEOUT_MS, (long)RPPD_TIMEOUT_MS);

    /* Perform request */
    res = curl_easy_perform(g_curl);

    /* Cleanup */
    curl_slist_free_all(headers);
    free(frame);

    if (res != CURLE_OK)
    {
        snprintf(g_last_error, sizeof(g_last_error), "curl error: %s",
                 curl_easy_strerror(res));
        free(response.data);
        pthread_mutex_unlock(&g_curl_mutex);
        return -1;
    }

    /* Check HTTP status */
    curl_easy_getinfo(g_curl, CURLINFO_RESPONSE_CODE, &http_code);
    if (http_code != 200)
    {
        snprintf(g_last_error, sizeof(g_last_error), "HTTP error: %ld", http_code);
        free(response.data);
        pthread_mutex_unlock(&g_curl_mutex);
        return -1;
    }

    /* Parse response frame */
    if (parse_grpc_frame(response.data, response.size, resp_data, resp_len) != 0)
    {
        free(response.data);
        pthread_mutex_unlock(&g_curl_mutex);
        return -1;
    }

    /* Caller needs to copy the data as we'll free the buffer */
    uint8_t *resp_copy = malloc(*resp_len);
    if (resp_copy == NULL)
    {
        snprintf(g_last_error, sizeof(g_last_error), "Memory allocation failed");
        free(response.data);
        pthread_mutex_unlock(&g_curl_mutex);
        return -1;
    }
    memcpy(resp_copy, *resp_data, *resp_len);
    *resp_data = resp_copy;

    free(response.data);
    pthread_mutex_unlock(&g_curl_mutex);

    return 0;
}

/*
 * Connect to gRPC server
 */
int
rppd_grpc_connect(const char *host)
{
    pthread_mutex_lock(&g_curl_mutex);

    if (init_curl() != 0)
    {
        pthread_mutex_unlock(&g_curl_mutex);
        return -1;
    }

    strncpy(g_host, host, RPPD_MAX_HOST_LEN - 1);
    g_host[RPPD_MAX_HOST_LEN - 1] = '\0';

    pthread_mutex_unlock(&g_curl_mutex);

    /* Test connection with a simple status call */
    /* For now, just assume success - actual test will happen on first call */

    return 0;
}

/*
 * Disconnect from gRPC server
 */
void
rppd_grpc_disconnect(void)
{
    pthread_mutex_lock(&g_curl_mutex);

    if (g_curl != NULL)
    {
        curl_easy_cleanup(g_curl);
        g_curl = NULL;
    }

    g_host[0] = '\0';
    curl_global_cleanup();

    pthread_mutex_unlock(&g_curl_mutex);
}

/*
 * Get last error message
 */
const char *
rppd_grpc_last_error(void)
{
    return g_last_error;
}

/*
 * Make event RPC call
 */
int
rppd_grpc_event(const DbEventRequest *req, DbEventResponse *resp)
{
    uint8_t *req_data;
    size_t req_len;
    uint8_t *resp_data = NULL;
    size_t resp_len;
    int ret;

    /* Encode request */
    req_len = rppd_pb_encode_event_request(req, &req_data);
    if (req_data == NULL)
    {
        snprintf(g_last_error, sizeof(g_last_error), "Failed to encode request");
        return -1;
    }

    /* Make call */
    ret = grpc_call("/rppg.RppdTrigger/event", req_data, req_len, &resp_data, &resp_len);
    free(req_data);

    if (ret != 0)
        return ret;

    /* Decode response */
    ret = rppd_pb_decode_event_response(resp_data, resp_len, resp);
    free(resp_data);

    return ret;
}

/*
 * Make status RPC call
 */
int
rppd_grpc_status(const StatusRequest *req, StatusResponse *resp)
{
    uint8_t *req_data;
    size_t req_len;
    uint8_t *resp_data = NULL;
    size_t resp_len;
    int ret;

    /* Encode request */
    req_len = rppd_pb_encode_status_request(req, &req_data);
    if (req_data == NULL)
    {
        snprintf(g_last_error, sizeof(g_last_error), "Failed to encode request");
        return -1;
    }

    /* Make call */
    ret = grpc_call("/rppg.RppdTrigger/status", req_data, req_len, &resp_data, &resp_len);
    free(req_data);

    if (ret != 0)
        return ret;

    /* Decode response */
    ret = rppd_pb_decode_status_response(resp_data, resp_len, resp);
    free(resp_data);

    return ret;
}
