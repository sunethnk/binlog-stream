// file_publisher.c
// Example File Publisher Plugin
//
// Build: gcc -shared -fPIC -o file_publisher.so file_publisher.c -I.

#include "publisher_api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Plugin private data
typedef struct {
    FILE *fp;
    char file_path[512];
    int flush_every_event;
    uint64_t events_written;
} file_publisher_data_t;

// Plugin metadata
static const char* get_name(void) {
    return "file_publisher";
}

static const char* get_version(void) {
    return "1.0.0";
}

static int get_api_version(void) {
    return PUBLISHER_API_VERSION;
}

// Initialize publisher
static int init(const publisher_config_t *config, void **plugin_data) {
    PLUGIN_LOG_INFO("Initializing file publisher");
    
    file_publisher_data_t *data = calloc(1, sizeof(file_publisher_data_t));
    if (!data) {
        PLUGIN_LOG_ERROR("Failed to allocate publisher data");
        return -1;
    }
    
    // Get file path from config
    const char *file_path = PLUGIN_GET_CONFIG(config, "file_path");
    if (!file_path) {
        PLUGIN_LOG_ERROR("Missing required config: file_path");
        free(data);
        return -1;
    }
    
    strncpy(data->file_path, file_path, sizeof(data->file_path) - 1);
    
    // Get optional flush setting
    data->flush_every_event = PLUGIN_GET_CONFIG_INT(config, "flush_every_event", 1);
    
    *plugin_data = data;
    
    PLUGIN_LOG_INFO("File publisher configured: path=%s, flush=%d",
                   data->file_path, data->flush_every_event);
    
    return 0;
}

// Start publisher
static int start(void *plugin_data) {
    file_publisher_data_t *data = (file_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Starting file publisher: %s", data->file_path);
    
    // Open file in append mode
    data->fp = fopen(data->file_path, "a");
    if (!data->fp) {
        PLUGIN_LOG_ERROR("Failed to open file: %s", data->file_path);
        return -1;
    }
    
    PLUGIN_LOG_INFO("File publisher started: %s", data->file_path);
    return 0;
}

// Publish event
static int publish(void *plugin_data, const cdc_event_t *event) {
    file_publisher_data_t *data = (file_publisher_data_t*)plugin_data;
    
    if (!data->fp || !event || !event->json) {
        return -1;
    }
    
    // Write JSON to file
    fprintf(data->fp, "%s\n", event->json);
    
    if (data->flush_every_event) {
        fflush(data->fp);
    }
    
    data->events_written++;
    PLUGIN_LOG_TRACE("Published event to file: txn=%s, db=%s, table=%s",
                    event->txn ? event->txn : "",
                    event->db ? event->db : "",
                    event->table ? event->table : "");
    
    return 0;
}

// Stop publisher
static int stop(void *plugin_data) {
    file_publisher_data_t *data = (file_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Stopping file publisher: %s (events_written=%llu)",
                   data->file_path, data->events_written);
    
    if (data->fp) {
        fflush(data->fp);
        fclose(data->fp);
        data->fp = NULL;
    }
    
    return 0;
}

// Cleanup
static void cleanup(void *plugin_data) {
    file_publisher_data_t *data = (file_publisher_data_t*)plugin_data;
    
    if (data) {
        if (data->fp) {
            fclose(data->fp);
        }
        free(data);
    }
    
    PLUGIN_LOG_INFO("File publisher cleaned up");
}

// Health check
static int health_check(void *plugin_data) {
    file_publisher_data_t *data = (file_publisher_data_t*)plugin_data;
    return (data && data->fp) ? 0 : -1;
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
