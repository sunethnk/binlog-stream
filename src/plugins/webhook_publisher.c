// webhook_publisher.c
// HTTP/REST API Publisher Plugin
// Sends CDC events to HTTP webhooks
//
// Build: gcc -shared -fPIC -o webhook_publisher.so webhook_publisher.c -I. -lcurl

#include "publisher_api.h"
#include <curl/curl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Plugin private data
typedef struct {
    CURL *curl;
    char webhook_url[512];
    char auth_header[256];
    int timeout_seconds;
    int retry_count;
    uint64_t events_sent;
    uint64_t events_failed;
} webhook_publisher_data_t;

// Callback for curl response (we ignore the response body for now)
static size_t webhook_response_callback(void *contents, size_t size, size_t nmemb, void *userp) {
    (void)contents;
    (void)userp;
    return size * nmemb;
}

static const char* get_name(void) {
    return "webhook_publisher";
}

static const char* get_version(void) {
    return "1.0.0";
}

static int get_api_version(void) {
    return PUBLISHER_API_VERSION;
}

static int init(const publisher_config_t *config, void **plugin_data) {
    PLUGIN_LOG_INFO("Initializing webhook publisher");
    
    webhook_publisher_data_t *data = calloc(1, sizeof(webhook_publisher_data_t));
    if (!data) {
        PLUGIN_LOG_ERROR("Failed to allocate publisher data");
        return -1;
    }
    
    // Get webhook URL (required)
    const char *url = PLUGIN_GET_CONFIG(config, "webhook_url");
    if (!url) {
        PLUGIN_LOG_ERROR("Missing required config: webhook_url");
        free(data);
        return -1;
    }
    strncpy(data->webhook_url, url, sizeof(data->webhook_url) - 1);
    
    // Get optional auth token
    const char *auth_token = PLUGIN_GET_CONFIG(config, "auth_token");
    if (auth_token) {
        snprintf(data->auth_header, sizeof(data->auth_header), 
                "Authorization: Bearer %s", auth_token);
    }
    
    // Get optional settings
    data->timeout_seconds = PLUGIN_GET_CONFIG_INT(config, "timeout_seconds", 10);
    data->retry_count = PLUGIN_GET_CONFIG_INT(config, "retry_count", 3);
    
    *plugin_data = data;
    
    PLUGIN_LOG_INFO("Webhook publisher configured: url=%s, timeout=%ds, retries=%d",
                   data->webhook_url, data->timeout_seconds, data->retry_count);
    
    return 0;
}

static int start(void *plugin_data) {
    webhook_publisher_data_t *data = (webhook_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Starting webhook publisher");
    
    // Initialize curl
    curl_global_init(CURL_GLOBAL_DEFAULT);
    data->curl = curl_easy_init();
    
    if (!data->curl) {
        PLUGIN_LOG_ERROR("Failed to initialize CURL");
        return -1;
    }
    
    // Set curl options
    curl_easy_setopt(data->curl, CURLOPT_TIMEOUT, data->timeout_seconds);
    curl_easy_setopt(data->curl, CURLOPT_WRITEFUNCTION, webhook_response_callback);
    curl_easy_setopt(data->curl, CURLOPT_NOSIGNAL, 1L);
    
    PLUGIN_LOG_INFO("Webhook publisher started: %s", data->webhook_url);
    return 0;
}

static int publish(void *plugin_data, const cdc_event_t *event) {
    webhook_publisher_data_t *data = (webhook_publisher_data_t*)plugin_data;
    
    if (!data->curl || !event || !event->json) {
        return -1;
    }
    
    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    
    if (data->auth_header[0] != '\0') {
        headers = curl_slist_append(headers, data->auth_header);
    }
    
    // Set request options
    curl_easy_setopt(data->curl, CURLOPT_URL, data->webhook_url);
    curl_easy_setopt(data->curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(data->curl, CURLOPT_POSTFIELDS, event->json);
    
    // Try sending with retries
    int attempt = 0;
    CURLcode res = CURLE_FAILED_INIT;
    
    for (attempt = 0; attempt <= data->retry_count; attempt++) {
        res = curl_easy_perform(data->curl);
        
        if (res == CURLE_OK) {
            long http_code = 0;
            curl_easy_getinfo(data->curl, CURLINFO_RESPONSE_CODE, &http_code);
            
            if (http_code >= 200 && http_code < 300) {
                data->events_sent++;
                PLUGIN_LOG_TRACE("Webhook sent: db=%s, table=%s, http_code=%ld",
                               event->db, event->table, http_code);
                curl_slist_free_all(headers);
                return 0;
            } else {
                PLUGIN_LOG_WARN("Webhook returned HTTP %ld (attempt %d/%d)",
                              http_code, attempt + 1, data->retry_count + 1);
            }
        } else {
            PLUGIN_LOG_WARN("Webhook failed: %s (attempt %d/%d)",
                          curl_easy_strerror(res), attempt + 1, data->retry_count + 1);
        }
        
        // Wait before retry (exponential backoff)
        if (attempt < data->retry_count) {
            usleep(100000 * (1 << attempt)); // 100ms, 200ms, 400ms, etc.
        }
    }
    
    curl_slist_free_all(headers);
    data->events_failed++;
    PLUGIN_LOG_ERROR("Webhook failed after %d attempts: db=%s, table=%s",
                    attempt, event->db, event->table);
    
    return -1;
}

static int stop(void *plugin_data) {
    webhook_publisher_data_t *data = (webhook_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Stopping webhook publisher: %s (sent=%llu, failed=%llu)",
                   data->webhook_url, data->events_sent, data->events_failed);
    
    if (data->curl) {
        curl_easy_cleanup(data->curl);
        data->curl = NULL;
    }
    
    curl_global_cleanup();
    
    return 0;
}

static void cleanup(void *plugin_data) {
    webhook_publisher_data_t *data = (webhook_publisher_data_t*)plugin_data;
    
    if (data) {
        if (data->curl) {
            curl_easy_cleanup(data->curl);
        }
        free(data);
    }
    
    PLUGIN_LOG_INFO("Webhook publisher cleaned up");
}

static int health_check(void *plugin_data) {
    webhook_publisher_data_t *data = (webhook_publisher_data_t*)plugin_data;
    return (data && data->curl) ? 0 : -1;
}

static const publisher_callbacks_t callbacks = {
    .get_name = get_name,
    .get_version = get_version,
    .get_api_version = get_api_version,
    .init = init,
    .start = start,
    .stop = stop,
    .cleanup = cleanup,
    .publish = publish,
    .publish_batch = NULL,
    .health_check = health_check,
};

PUBLISHER_PLUGIN_DEFINE(webhook_publisher) {
    publisher_plugin_t *p = malloc(sizeof(publisher_plugin_t));
    if (!p) return -1;
    
    p->callbacks = &callbacks;
    p->plugin_data = NULL;
    
    *plugin = p;
    return 0;
}