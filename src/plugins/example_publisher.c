// example_publisher.c
// Example Publisher Plugin
//
// Build: gcc -shared -fPIC -o example_publisher.so example_publisher.c -I.

#include "publisher_api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Plugin private data
typedef struct {
    const char *example_data;
    uint64_t events_written;
} example_publisher_data_t;

// Plugin metadata
static const char* get_name(void) {
    return "example_publisher";
}

static const char* get_version(void) {
    return "1.0.0";
}

static int get_api_version(void) {
    return PUBLISHER_API_VERSION;
}

// Initialize publisher
static int init(const publisher_config_t *config, void **plugin_data) {
    PLUGIN_LOG_INFO("Initializing example publisher");
    
    example_publisher_data_t *data = calloc(1, sizeof(example_publisher_data_t));
    if (!data) {
        PLUGIN_LOG_ERROR("Failed to allocate publisher data");
        return -1;
    }
    
    data->example_data = PLUGIN_GET_CONFIG(config, "example_data");
    
    *plugin_data = data;
        
    PLUGIN_LOG_INFO("Example publisher configured: example_data=%s",
                   data->example_data);
    
    return 0;
}

// Start publisher
static int start(void *plugin_data) {
    example_publisher_data_t *data = (example_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Starting example publisher: %s", data->example_data);
    

    PLUGIN_LOG_INFO("Example publisher started: %s", data->example_data);
    return 0;
}

// Publish event
static int publish(void *plugin_data, const cdc_event_t *event) {
    example_publisher_data_t *data = (example_publisher_data_t*)plugin_data;
    
    if (!data || !data->example_data) {
        return -1;
    }
    
    // Write JSON to file
    printf("############### EXAMPLE PLUGIN ###############\n");
    printf("%s\n", event->json);
    printf("############### EXAMPLE PLUGIN ###############\n");
    
    data->events_written++;
    PLUGIN_LOG_TRACE("Published event to example: txn=%s, db=%s, table=%s",
                    event->txn ? event->txn : "",
                    event->db ? event->db : "",
                    event->table ? event->table : "");
    
    return 0;
}

// Stop publisher
static int stop(void *plugin_data) {
    example_publisher_data_t *data = (example_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Stopping example publisher: %s (events_written=%llu)",
                   data->example_data, data->events_written);
        
    return 0;
}

// Cleanup
static void cleanup(void *plugin_data) {
    example_publisher_data_t *data = (example_publisher_data_t*)plugin_data;
    
    if (data) {
        free(data);
    }
    
    PLUGIN_LOG_INFO("Example publisher cleaned up");
}

// Health check
static int health_check(void *plugin_data) {
    example_publisher_data_t *data = (example_publisher_data_t*)plugin_data;
    return (data && data->example_data) ? 0 : -1;
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
    .publish_batch = NULL,  // Not implemented
    .health_check = health_check,
};

// Plugin entry point
PUBLISHER_PLUGIN_DEFINE(file_publisher) {
    publisher_plugin_t *p = malloc(sizeof(publisher_plugin_t));
    if (!p) return -1;
    
    p->callbacks = &callbacks;
    p->plugin_data = NULL;
    
    *plugin = p;
    return 0;
}
