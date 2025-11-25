// syslog_publisher.c
// Syslog Publisher Plugin - Sends CDC events to syslog
//
// Build: gcc -shared -fPIC -o syslog_publisher.so syslog_publisher.c -I.

#include "publisher_api.h"
#include <syslog.h>
#include <stdlib.h>
#include <string.h>
#include <json-c/json.h>
#include <stdio.h>

// Plugin private data
typedef struct {
    char ident[64];
    int facility;
    int priority;
    int include_pid;
    int format_compact;  // 0 = full JSON, 1 = compact summary
    uint64_t events_logged;
} syslog_publisher_data_t;

static const char* get_name(void) {
    return "syslog_publisher";
}

static const char* get_version(void) {
    return "1.0.0";
}

static int get_api_version(void) {
    return PUBLISHER_API_VERSION;
}

// Parse facility string to syslog constant
static int parse_facility(const char *facility_str) {
    if (!facility_str) return LOG_LOCAL0;
    
    if (strcmp(facility_str, "LOG_USER") == 0) return LOG_USER;
    if (strcmp(facility_str, "LOG_DAEMON") == 0) return LOG_DAEMON;
    if (strcmp(facility_str, "LOG_LOCAL0") == 0) return LOG_LOCAL0;
    if (strcmp(facility_str, "LOG_LOCAL1") == 0) return LOG_LOCAL1;
    if (strcmp(facility_str, "LOG_LOCAL2") == 0) return LOG_LOCAL2;
    if (strcmp(facility_str, "LOG_LOCAL3") == 0) return LOG_LOCAL3;
    if (strcmp(facility_str, "LOG_LOCAL4") == 0) return LOG_LOCAL4;
    if (strcmp(facility_str, "LOG_LOCAL5") == 0) return LOG_LOCAL5;
    if (strcmp(facility_str, "LOG_LOCAL6") == 0) return LOG_LOCAL6;
    if (strcmp(facility_str, "LOG_LOCAL7") == 0) return LOG_LOCAL7;
    
    return LOG_LOCAL0;
}

// Parse priority string to syslog constant
static int parse_priority(const char *priority_str) {
    if (!priority_str) return LOG_INFO;
    
    if (strcmp(priority_str, "LOG_EMERG") == 0) return LOG_EMERG;
    if (strcmp(priority_str, "LOG_ALERT") == 0) return LOG_ALERT;
    if (strcmp(priority_str, "LOG_CRIT") == 0) return LOG_CRIT;
    if (strcmp(priority_str, "LOG_ERR") == 0) return LOG_ERR;
    if (strcmp(priority_str, "LOG_WARNING") == 0) return LOG_WARNING;
    if (strcmp(priority_str, "LOG_NOTICE") == 0) return LOG_NOTICE;
    if (strcmp(priority_str, "LOG_INFO") == 0) return LOG_INFO;
    if (strcmp(priority_str, "LOG_DEBUG") == 0) return LOG_DEBUG;
    
    return LOG_INFO;
}

static int init(const publisher_config_t *config, void **plugin_data) {
    PLUGIN_LOG_INFO("Initializing syslog publisher");
    
    syslog_publisher_data_t *data = calloc(1, sizeof(syslog_publisher_data_t));
    if (!data) {
        PLUGIN_LOG_ERROR("Failed to allocate publisher data");
        return -1;
    }
    
    // Get ident (program name for syslog)
    const char *ident = PLUGIN_GET_CONFIG(config, "ident");
    if (!ident) ident = "binlog_cdc";
    strncpy(data->ident, ident, sizeof(data->ident) - 1);
    
    // Get facility
    const char *facility_str = PLUGIN_GET_CONFIG(config, "facility");
    data->facility = parse_facility(facility_str);
    
    // Get priority
    const char *priority_str = PLUGIN_GET_CONFIG(config, "priority");
    data->priority = parse_priority(priority_str);
    
    // Get options
    data->include_pid = PLUGIN_GET_CONFIG_BOOL(config, "include_pid", 1);
    data->format_compact = PLUGIN_GET_CONFIG_BOOL(config, "format_compact", 0);
    
    *plugin_data = data;
    
    PLUGIN_LOG_INFO("Syslog publisher configured: ident=%s, format=%s",
                   data->ident, data->format_compact ? "compact" : "full");
    
    return 0;
}

static int start(void *plugin_data) {
    syslog_publisher_data_t *data = (syslog_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Starting syslog publisher");
    
    // Open syslog connection
    int options = LOG_CONS | LOG_NDELAY;
    if (data->include_pid) {
        options |= LOG_PID;
    }
    
    openlog(data->ident, options, data->facility);
    
    PLUGIN_LOG_INFO("Syslog publisher started: %s", data->ident);
    return 0;
}

// Format compact summary from JSON
static char* format_compact(const cdc_event_t *event) {
    static char buffer[512];
    
    // Parse JSON to extract type
    json_object *root = json_tokener_parse(event->json);
    if (!root) {
        snprintf(buffer, sizeof(buffer), "CDC event db=%s table=%s",
                event->db ? event->db : "?", 
                event->table ? event->table : "?");
        return buffer;
    }
    
    json_object *type_obj = NULL;
    const char *event_type = "UNKNOWN";
    if (json_object_object_get_ex(root, "type", &type_obj)) {
        event_type = json_object_get_string(type_obj);
    }
    
    // Get row count if available
    json_object *rows_obj = NULL;
    int row_count = 0;
    if (json_object_object_get_ex(root, "rows", &rows_obj)) {
        row_count = json_object_array_length(rows_obj);
    }
    
    snprintf(buffer, sizeof(buffer), 
            "CDC: %s db=%s table=%s rows=%d txn=%s",
            event_type,
            event->db ? event->db : "?",
            event->table ? event->table : "?",
            row_count,
            event->txn ? event->txn : "none");
    
    json_object_put(root);
    return buffer;
}

static int publish(void *plugin_data, const cdc_event_t *event) {
    syslog_publisher_data_t *data = (syslog_publisher_data_t*)plugin_data;
    
    if (!event || !event->json) {
        return -1;
    }
    
    // Format message
    const char *message;
    char *compact_msg = NULL;
    
    if (data->format_compact) {
        compact_msg = format_compact(event);
        message = compact_msg;
    } else {
        message = event->json;
    }
    
    // Log to syslog
    syslog(data->priority, "%s", message);
    
    data->events_logged++;
    
    PLUGIN_LOG_TRACE("Logged to syslog: db=%s, table=%s",
                    event->db, event->table);
    
    return 0;
}

static int stop(void *plugin_data) {
    syslog_publisher_data_t *data = (syslog_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Stopping syslog publisher (logged=%llu)", data->events_logged);
    
    closelog();
    
    return 0;
}

static void cleanup(void *plugin_data) {
    syslog_publisher_data_t *data = (syslog_publisher_data_t*)plugin_data;
    
    if (data) {
        free(data);
    }
    
    PLUGIN_LOG_INFO("Syslog publisher cleaned up");
}

static int health_check(void *plugin_data) {
    syslog_publisher_data_t *data = (syslog_publisher_data_t*)plugin_data;
    return (data != NULL) ? 0 : -1;
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

PUBLISHER_PLUGIN_DEFINE(syslog_publisher) {
    publisher_plugin_t *p = malloc(sizeof(publisher_plugin_t));
    if (!p) return -1;
    
    p->callbacks = &callbacks;
    p->plugin_data = NULL;
    
    *plugin = p;
    return 0;
}