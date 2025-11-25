// publisher_api.h
// Publisher Plugin API for MySQL/MariaDB Binlog Streamer
//
// Custom publishers should implement this interface and compile as shared libraries (.so)
//
// Example compilation:
//   gcc -shared -fPIC -o my_publisher.so my_publisher.c -I.

#ifndef PUBLISHER_API_H
#define PUBLISHER_API_H

#include <stddef.h>
#include <stdint.h>

// API version for compatibility checking
#define PUBLISHER_API_VERSION 1

// Forward declarations
typedef struct publisher_plugin publisher_plugin_t;
typedef struct cdc_event cdc_event_t;
typedef struct publisher_config publisher_config_t;

// CDC Event structure passed to publishers
struct cdc_event {
    const char *db;           // Database name
    const char *table;        // Table name
    const char *json;         // JSON representation of the event
    const char *txn;          // Transaction ID
    uint64_t position;        // Binlog position
    const char *binlog_file;  // Binlog file name
};

// Publisher configuration from JSON
struct publisher_config {
    const char *name;         // Publisher name
    int active;               // Is publisher active?
    uint64_t max_q_depth;
    // Database filtering
    char **databases;         // Array of database names to publish
    int db_count;             // Number of databases in filter
    
    // Custom configuration (publisher-specific key-value pairs)
    char **config_keys;       // Array of configuration keys
    char **config_values;     // Array of configuration values
    int config_count;         // Number of config items
};

// Publisher plugin callbacks
typedef struct publisher_callbacks {
    // Get plugin metadata
    const char* (*get_name)(void);
    const char* (*get_version)(void);
    int (*get_api_version)(void);
    
    // Lifecycle callbacks
    int (*init)(const publisher_config_t *config, void **plugin_data);
    int (*start)(void *plugin_data);
    int (*stop)(void *plugin_data);
    void (*cleanup)(void *plugin_data);
    
    // Event processing
    int (*publish)(void *plugin_data, const cdc_event_t *event);
    
    // Optional: batch processing
    int (*publish_batch)(void *plugin_data, const cdc_event_t **events, int count);
    
    // Optional: health check
    int (*health_check)(void *plugin_data);
    
} publisher_callbacks_t;

// Plugin descriptor - must be exported by each plugin
typedef struct publisher_plugin {
    const publisher_callbacks_t *callbacks;
    void *plugin_data;  // Private plugin data
} publisher_plugin_t;

// Main entry point - each plugin must export this function
// Symbol name: "publisher_plugin_init"
typedef int (*publisher_plugin_init_fn)(publisher_plugin_t **plugin);

// Helper macro for defining plugin init function
#define PUBLISHER_PLUGIN_DEFINE(name) \
    int publisher_plugin_init(publisher_plugin_t **plugin)

// Configuration helper functions (provided by core)
typedef struct publisher_api_helpers {
    // Logging functions
    void (*log_error)(const char *fmt, ...);
    void (*log_warn)(const char *fmt, ...);
    void (*log_info)(const char *fmt, ...);
    void (*log_debug)(const char *fmt, ...);
    void (*log_trace)(const char *fmt, ...);
    
    // Memory management
    void* (*malloc)(size_t size);
    void (*free)(void *ptr);
    
    // Configuration helpers
    const char* (*get_config)(const publisher_config_t *config, const char *key);
    int (*get_config_int)(const publisher_config_t *config, const char *key, int default_val);
    int (*get_config_bool)(const publisher_config_t *, const char *key, int default_val);
    
} publisher_api_helpers_t;

// Global helpers instance (set by core before init)
extern const publisher_api_helpers_t *publisher_helpers;

// Helper macros for logging
#define PLUGIN_LOG_ERROR(...) if(publisher_helpers) publisher_helpers->log_error(__VA_ARGS__)
#define PLUGIN_LOG_WARN(...)  if(publisher_helpers) publisher_helpers->log_warn(__VA_ARGS__)
#define PLUGIN_LOG_INFO(...)  if(publisher_helpers) publisher_helpers->log_info(__VA_ARGS__)
#define PLUGIN_LOG_DEBUG(...) if(publisher_helpers) publisher_helpers->log_debug(__VA_ARGS__)
#define PLUGIN_LOG_TRACE(...) if(publisher_helpers) publisher_helpers->log_trace(__VA_ARGS__)

// Helper for getting config values
#define PLUGIN_GET_CONFIG(cfg, key) \
    (publisher_helpers ? publisher_helpers->get_config(cfg, key) : NULL)

#define PLUGIN_GET_CONFIG_INT(cfg, key, def) \
    (publisher_helpers ? publisher_helpers->get_config_int(cfg, key, def) : (def))

#define PLUGIN_GET_CONFIG_BOOL(cfg, key, def) \
    (publisher_helpers->get_config_bool ? \
        publisher_helpers->get_config_bool((cfg), (key), (def)) : (def))

#endif // PUBLISHER_API_H
