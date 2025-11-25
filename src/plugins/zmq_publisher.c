// zmq_publisher.c
// Example ZeroMQ Publisher Plugin
//
// Build: gcc -shared -fPIC -o zmq_publisher.so zmq_publisher.c -I. -lzmq

#include "publisher_api.h"
#include <zmq.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

// Plugin private data
typedef struct {
    void *zmq_context;
    void *zmq_socket;
    char endpoint[256];
    int send_timeout_ms;
    int subscriber_filtering;
    uint64_t messages_sent;
    uint64_t send_failures;
} zmq_publisher_data_t;

// Plugin metadata
static const char* get_name(void) {
    return "zmq_publisher";
}

static const char* get_version(void) {
    return "1.0.0";
}

static int get_api_version(void) {
    return PUBLISHER_API_VERSION;
}

// Initialize publisher
static int init(const publisher_config_t *config, void **plugin_data) {
    PLUGIN_LOG_INFO("Initializing ZMQ publisher");
    
    zmq_publisher_data_t *data = calloc(1, sizeof(zmq_publisher_data_t));
    if (!data) {
        PLUGIN_LOG_ERROR("Failed to allocate publisher data");
        return -1;
    }
    
    // Get endpoint from config
    const char *endpoint = PLUGIN_GET_CONFIG(config, "endpoint");
    if (!endpoint) {
        PLUGIN_LOG_ERROR("Missing required config: endpoint");
        free(data);
        return -1;
    }
    
    strncpy(data->endpoint, endpoint, sizeof(data->endpoint) - 1);
    
    // Get optional timeout
    data->send_timeout_ms = PLUGIN_GET_CONFIG_INT(config, "send_timeout_ms", 1000);
    data->subscriber_filtering = PLUGIN_GET_CONFIG_BOOL(config, "subscriber_filtering", 0);
    *plugin_data = data;
    
    PLUGIN_LOG_INFO("ZMQ publisher configured: endpoint=%s, timeout=%dms",
                   data->endpoint, data->send_timeout_ms);
    
    return 0;
}

// Start publisher
static int start(void *plugin_data) {
    zmq_publisher_data_t *data = (zmq_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Starting ZMQ publisher: %s", data->endpoint);
    
    // Create ZMQ context
    data->zmq_context = zmq_ctx_new();
    if (!data->zmq_context) {
        PLUGIN_LOG_ERROR("Failed to create ZMQ context");
        return -1;
    }
    
    // Create PUB socket
    data->zmq_socket = zmq_socket(data->zmq_context, ZMQ_PUB);
    if (!data->zmq_socket) {
        PLUGIN_LOG_ERROR("Failed to create ZMQ socket");
        zmq_ctx_destroy(data->zmq_context);
        data->zmq_context = NULL;
        return -1;
    }
    
    // Set socket options
    int timeout = data->send_timeout_ms;
    zmq_setsockopt(data->zmq_socket, ZMQ_SNDTIMEO, &timeout, sizeof(timeout));
    
    // Bind socket
    if (zmq_bind(data->zmq_socket, data->endpoint) != 0) {
        PLUGIN_LOG_ERROR("Failed to bind ZMQ socket to %s: %s",
                        data->endpoint, zmq_strerror(errno));
        zmq_close(data->zmq_socket);
        zmq_ctx_destroy(data->zmq_context);
        data->zmq_socket = NULL;
        data->zmq_context = NULL;
        return -1;
    }
    
    PLUGIN_LOG_INFO("ZMQ publisher started: %s", data->endpoint);
    return 0;
}

// Publish event
static int publish(void *plugin_data, const cdc_event_t *event) {
    zmq_publisher_data_t *data = (zmq_publisher_data_t*)plugin_data;
    
    if (!data->zmq_socket || !event || !event->json) {
        return -1;
    }
    int rc = -1;
    char topic[256];
    // Build topic: "db.table"
    if(data->subscriber_filtering){
        snprintf(topic, sizeof(topic), "%s.%s",
                event->db ? event->db : "unknown",
                event->table ? event->table : "unknown");

        // Send multi-part message: [topic, json]
        rc = zmq_send(data->zmq_socket, topic, strlen(topic), ZMQ_SNDMORE);
        if (rc < 0) {
            data->send_failures++;
            PLUGIN_LOG_WARN("ZMQ send topic failed: %s", zmq_strerror(errno));
            return -1;
        }
    } else {
        topic[0] = '\0';
    }
    
    rc = zmq_send(data->zmq_socket, event->json, strlen(event->json), 0);
    if (rc < 0) {
        data->send_failures++;
        PLUGIN_LOG_WARN("ZMQ send message failed: %s", zmq_strerror(errno));
        return -1;
    }
    
    data->messages_sent++;
    
    PLUGIN_LOG_TRACE("Published to ZMQ: topic=%s, txn=%s",
                    strlen(topic) > 0 ? topic : "none", event->txn ? event->txn : "");
    
    return 0;
}

// Batch publish (optional optimization)
static int publish_batch(void *plugin_data, const cdc_event_t **events, int count) {
    int success = 0;
    
    for (int i = 0; i < count; i++) {
        if (publish(plugin_data, events[i]) == 0) {
            success++;
        }
    }
    
    return success == count ? 0 : -1;
}

// Stop publisher
static int stop(void *plugin_data) {
    zmq_publisher_data_t *data = (zmq_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Stopping ZMQ publisher: %s (sent=%llu, failures=%llu)",
                   data->endpoint, data->messages_sent, data->send_failures);
    
    if (data->zmq_socket) {
        zmq_close(data->zmq_socket);
        data->zmq_socket = NULL;
    }
    
    if (data->zmq_context) {
        zmq_ctx_destroy(data->zmq_context);
        data->zmq_context = NULL;
    }
    
    return 0;
}

// Cleanup
static void cleanup(void *plugin_data) {
    zmq_publisher_data_t *data = (zmq_publisher_data_t*)plugin_data;
    
    if (data) {
        if (data->zmq_socket) {
            zmq_close(data->zmq_socket);
        }
        if (data->zmq_context) {
            zmq_ctx_destroy(data->zmq_context);
        }
        free(data);
    }
    
    PLUGIN_LOG_INFO("ZMQ publisher cleaned up");
}

// Health check
static int health_check(void *plugin_data) {
    zmq_publisher_data_t *data = (zmq_publisher_data_t*)plugin_data;
    return (data && data->zmq_socket) ? 0 : -1;
}

// Plugin callbacks
static const publisher_callbacks_t callbacks = {
    .get_name = get_name,
    .get_version = get_version,
    .get_api_version = get_api_version,
    .init = init,
    .start = start,
    .stop = stop,
    .cleanup = cleanup,
    .publish = publish,
    .publish_batch = publish_batch,
    .health_check = health_check,
};

// Plugin entry point
PUBLISHER_PLUGIN_DEFINE(zmq_publisher) {
    publisher_plugin_t *p = malloc(sizeof(publisher_plugin_t));
    if (!p) return -1;
    
    p->callbacks = &callbacks;
    p->plugin_data = NULL;
    
    *plugin = p;
    return 0;
}
