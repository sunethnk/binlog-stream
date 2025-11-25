// publisher_loader.c
// Implementation of dynamic publisher plugin loader

#include "publisher_loader.h"
#include "logger.h"
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>
#include <errno.h>
#include <stdarg.h>

#define PUBLISHER_QUEUE_CAPACITY 1024

// Global helpers instance
const publisher_api_helpers_t *publisher_helpers = NULL;

// Helper implementations for plugins
static void plugin_log_error(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    char buf[4096];
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    log_error("%s", buf);
}

static void plugin_log_warn(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    char buf[4096];
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    log_warn("%s", buf);
}

static void plugin_log_info(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    char buf[4096];
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    log_info("%s", buf);
}

static void plugin_log_debug(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    char buf[4096];
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    log_debug("%s", buf);
}

static void plugin_log_trace(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    char buf[4096];
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    log_trace("%s", buf);
}

static const char* plugin_get_config(const publisher_config_t *config, const char *key) {
    if (!config || !key) return NULL;
    
    for (int i = 0; i < config->config_count; i++) {
        if (strcmp(config->config_keys[i], key) == 0) {
            return config->config_values[i];
        }
    }
    return NULL;
}

static int plugin_get_config_int(const publisher_config_t *config, const char *key, int default_val) {
    const char *val = plugin_get_config(config, key);
    if (!val) return default_val;
    return atoi(val);
}

static int plugin_get_config_bool(const publisher_config_t *config,
                                  const char *key,
                                  int default_val)
{
    const char *val = plugin_get_config(config, key);
    if (!val) return default_val;

    // Skip leading spaces
    while (*val == ' ' || *val == '\t' || *val == '\n' || *val == '\r') {
        val++;
    }

    if (*val == '\0') return default_val;

    // Numeric shortcuts
    if (strcmp(val, "1") == 0) return 1;
    if (strcmp(val, "0") == 0) return 0;

    // Case-insensitive text forms
    if (strcasecmp(val, "true")  == 0) return 1;
    if (strcasecmp(val, "yes")   == 0) return 1;
    if (strcasecmp(val, "on")    == 0) return 1;

    if (strcasecmp(val, "false") == 0) return 0;
    if (strcasecmp(val, "no")    == 0) return 0;
    if (strcasecmp(val, "off")   == 0) return 0;

    // If it's something unexpected, fall back
    return default_val;
}


// Initialize publisher manager
int publisher_manager_init(publisher_manager_t **manager) {
    publisher_manager_t *mgr = calloc(1, sizeof(publisher_manager_t));
    if (!mgr) return -1;
    
    // Setup helper functions for plugins
    mgr->helpers.log_error = plugin_log_error;
    mgr->helpers.log_warn = plugin_log_warn;
    mgr->helpers.log_info = plugin_log_info;
    mgr->helpers.log_debug = plugin_log_debug;
    mgr->helpers.log_trace = plugin_log_trace;
    mgr->helpers.malloc = malloc;
    mgr->helpers.free = free;
    mgr->helpers.get_config = plugin_get_config;
    mgr->helpers.get_config_int = plugin_get_config_int;
    mgr->helpers.get_config_bool = plugin_get_config_bool;
    
    // Set global helpers for plugins
    publisher_helpers = &mgr->helpers;
    
    *manager = mgr;
    return 0;
}

//// Deep copy CDC event
//static cdc_event_t* cdc_event_copy(const cdc_event_t *src) {
//    if (!src) return NULL;
//    
//    cdc_event_t *dst = malloc(sizeof(cdc_event_t));
//    if (!dst) return NULL;
//    
//    // Copy pointers (shallow - strings should be static or managed by caller)
//    memcpy(dst, src, sizeof(cdc_event_t));
//    return dst;
//}

// Deep copy CDC event (owns all strings in the copy)
static cdc_event_t* cdc_event_copy(const cdc_event_t *src) {
    if (!src) return NULL;

    cdc_event_t *dst = calloc(1, sizeof(*dst));
    if (!dst) return NULL;

    // Copy scalars
    dst->position = src->position;

    // Deep copy strings (it's fine to assign char* to const char*)
    if (src->db)          dst->db          = strdup(src->db);
    if (src->table)       dst->table       = strdup(src->table);
    if (src->json)        dst->json        = strdup(src->json);
    if (src->txn)         dst->txn         = strdup(src->txn);
    if (src->binlog_file) dst->binlog_file = strdup(src->binlog_file);

    return dst;
}

static void cdc_event_free(cdc_event_t *e) {
    if (!e) return;

    // cast away const only for freeing our own strdup'ed copies
    free((char*)e->db);
    free((char*)e->table);
    free((char*)e->json);
    free((char*)e->txn);
    free((char*)e->binlog_file);

    free(e);
}


// Queue management
static int queue_init(publisher_instance_t *inst) {
    log_trace("Initializing queue for %s",inst->name);
    if(inst->q_capacity > 0){
        inst->queue = calloc(inst->q_capacity, sizeof(cdc_event_t*));
        inst->q_capacity = inst->q_capacity;
    } else {
        inst->queue = calloc(PUBLISHER_QUEUE_CAPACITY, sizeof(cdc_event_t*));
        inst->q_capacity = PUBLISHER_QUEUE_CAPACITY;
    }
    log_trace("Queue depth set for %s is %d", inst->name, inst->q_capacity);
    if (!inst->queue) return -1;
    
    
    inst->q_head = inst->q_tail = inst->q_count = 0;
    inst->q_stop = 0;
    
    pthread_mutex_init(&inst->q_mutex, NULL);
    pthread_cond_init(&inst->q_cond, NULL);
    
    return 0;
}

static void queue_destroy(publisher_instance_t *inst) {
    if (!inst->queue) return;
    
    pthread_mutex_lock(&inst->q_mutex);
    for (int i = 0; i < inst->q_count; i++) {
        int idx = (inst->q_head + i) % inst->q_capacity;
        cdc_event_free(inst->queue[idx]);
    }
    pthread_mutex_unlock(&inst->q_mutex);
    
    free(inst->queue);
    inst->queue = NULL;
    
    pthread_mutex_destroy(&inst->q_mutex);
    pthread_cond_destroy(&inst->q_cond);
}

// Worker thread for async event processing
static void* publisher_worker_thread(void *arg) {
    publisher_instance_t *inst = (publisher_instance_t*)arg;
    
    log_info("Publisher worker started: %s", inst->name);
    
    while (1) {
        pthread_mutex_lock(&inst->q_mutex);
        
        while (inst->q_count == 0 && !inst->q_stop) {
            pthread_cond_wait(&inst->q_cond, &inst->q_mutex);
        }
        
        if (inst->q_stop && inst->q_count == 0) {
            pthread_mutex_unlock(&inst->q_mutex);
            break;
        }
        
        int idx = inst->q_head;
        cdc_event_t *event = inst->queue[idx];
        inst->queue[idx] = NULL;
        inst->q_head = (inst->q_head + 1) % inst->q_capacity;
        inst->q_count--;
        
        pthread_cond_signal(&inst->q_cond);
        pthread_mutex_unlock(&inst->q_mutex);
        
        // Process event
        if (event && inst->plugin && inst->plugin->callbacks->publish) {
            int ret = inst->plugin->callbacks->publish(
                inst->plugin->plugin_data,
                event
            );
            
            if (ret == 0) {
                inst->events_published++;
            } else {
                inst->errors++;
                log_warn("Publisher %s failed to publish event: ret=%d",
                        inst->name, ret);
            }
        }
        
        cdc_event_free(event);
    }
    
    log_info("Publisher worker exiting: %s", inst->name);
    return NULL;
}

// Load plugin from shared library
int publisher_manager_load_plugin(
    publisher_manager_t *manager,
    const char *name,
    const char *library_path,
    const publisher_config_t *config,
    publisher_instance_t **out_instance)
{
    if (!manager || !name || !library_path || !config) {
        log_error("Invalid parameters to publisher_manager_load_plugin");
        return -1;
    }
    
    if(!config->active){
        log_info("Loading publisher plugin: %s from %s skipped", name, library_path);
        return -1;
    }
    
    log_info("Loading publisher plugin: %s from %s %d", name, library_path);
    
    // Allocate instance
    publisher_instance_t *inst = calloc(1, sizeof(publisher_instance_t));
    if (!inst) {
        log_error("Failed to allocate publisher instance");
        return -1;
    }
    
    strncpy(inst->name, name, sizeof(inst->name) - 1);
    strncpy(inst->library_path, library_path, sizeof(inst->library_path) - 1);
    
    // Copy configuration - DON'T use memcpy as it copies pointers we don't own!
    inst->config.name = strdup(config->name ? config->name : "");
    inst->config.active = config->active;
    inst->config.db_count = 0;  // Will be set after successful copy
    inst->config.databases = NULL;
    inst->config.config_count = 0;  // Will be set after successful copy
    inst->config.config_keys = NULL;
    inst->config.config_values = NULL;

    inst->q_capacity =  config->max_q_depth > 0 ? config->max_q_depth : PUBLISHER_QUEUE_CAPACITY; 
    
    // Deep copy database list
    if (config->db_count > 0 && config->databases) {
        inst->config.databases = calloc(config->db_count, sizeof(char*));
        if (!inst->config.databases) {
            log_error("Failed to allocate database array for plugin %s", name);
            free((char*)inst->config.name);
            free(inst);
            return -1;
        }
        
        for (int i = 0; i < config->db_count; i++) {
            if (config->databases[i]) {
                inst->config.databases[i] = strdup(config->databases[i]);
                if (!inst->config.databases[i]) {
                    // Cleanup partial allocation
                    for (int j = 0; j < i; j++) {
                        free(inst->config.databases[j]);
                    }
                    free(inst->config.databases);
                    free((char*)inst->config.name);
                    free(inst);
                    return -1;
                }
            } else {
                inst->config.databases[i] = NULL;
            }
        }
        inst->config.db_count = config->db_count;
    }
    
    // Deep copy config key-value pairs
    if (config->config_count > 0 && config->config_keys && config->config_values) {
        inst->config.config_keys = calloc(config->config_count, sizeof(char*));
        inst->config.config_values = calloc(config->config_count, sizeof(char*));
        
        if (!inst->config.config_keys || !inst->config.config_values) {
            log_error("Failed to allocate config arrays for plugin %s", name);
            publisher_instance_destroy(inst);
            return -1;
        }
        
        for (int i = 0; i < config->config_count; i++) {
            if (config->config_keys[i]) {
                inst->config.config_keys[i] = strdup(config->config_keys[i]);
            } else {
                inst->config.config_keys[i] = NULL;
            }
            
            if (config->config_values[i]) {
                inst->config.config_values[i] = strdup(config->config_values[i]);
            } else {
                inst->config.config_values[i] = NULL;
            }
        }
        inst->config.config_count = config->config_count;
    }
    
    // Load shared library
    inst->dl_handle = dlopen(library_path, RTLD_NOW | RTLD_LOCAL);
    if (!inst->dl_handle) {
        log_error("Failed to load plugin %s: %s", library_path, dlerror());
        publisher_instance_destroy(inst);
        return -1;
    }
    
    // Get init function
    publisher_plugin_init_fn init_fn = 
        (publisher_plugin_init_fn)dlsym(inst->dl_handle, "publisher_plugin_init");
    
    if (!init_fn) {
        log_error("Plugin %s missing publisher_plugin_init symbol: %s",
                 library_path, dlerror());
        publisher_instance_destroy(inst);
        return -1;
    }
    
    // Initialize plugin
    if (init_fn(&inst->plugin) != 0 || !inst->plugin) {
        log_error("Plugin %s init failed", library_path);
        publisher_instance_destroy(inst);
        return -1;
    }
    
    // Verify plugin has required callbacks
    if (!inst->plugin->callbacks ||
        !inst->plugin->callbacks->get_name ||
        !inst->plugin->callbacks->init ||
        !inst->plugin->callbacks->publish) {
        log_error("Plugin %s missing required callbacks", library_path);
        publisher_instance_destroy(inst);
        return -1;
    }
    
    // Check API version
    if (inst->plugin->callbacks->get_api_version) {
        int api_ver = inst->plugin->callbacks->get_api_version();
        if (api_ver != PUBLISHER_API_VERSION) {
            log_error("Plugin %s API version mismatch: expected %d, got %d",
                     library_path, PUBLISHER_API_VERSION, api_ver);
            publisher_instance_destroy(inst);
            return -1;
        }
    }
    
    // Get plugin info
    const char *plugin_name = inst->plugin->callbacks->get_name();
    const char *plugin_version = inst->plugin->callbacks->get_version ?
        inst->plugin->callbacks->get_version() : "unknown";
    
    log_info("Loaded plugin: %s v%s", plugin_name, plugin_version);
    
    // Call plugin init
    if (inst->plugin->callbacks->init(&inst->config, &inst->plugin->plugin_data) != 0) {
        log_error("Plugin %s init callback failed", plugin_name);
        publisher_instance_destroy(inst);
        return -1;
    }
    
    // Initialize queue
    if (queue_init(inst) != 0) {
        log_error("Failed to initialize queue for publisher %s", name);
        publisher_instance_destroy(inst);
        return -1;
    }
    
    inst->active = config->active;
    
    // Add to manager
    inst->next = manager->instances;
    manager->instances = inst;
    manager->instance_count++;
    
    *out_instance = inst;
    
    log_info("Publisher %s loaded successfully (active=%d, databases=%d)",
            name, inst->active, inst->config.db_count);
    
    return 0;
}

// Start publisher
int publisher_instance_start(publisher_instance_t *inst) {
    if (!inst || !inst->active) return -1;
    if (inst->started) return 0;  // Already started
    
    log_info("Starting publisher: %s", inst->name);
    
    // Call plugin start if available
    if (inst->plugin->callbacks->start) {
        if (inst->plugin->callbacks->start(inst->plugin->plugin_data) != 0) {
            log_error("Plugin %s start callback failed", inst->name);
            return -1;
        }
    }
    
    // Start worker thread
    if (pthread_create(&inst->thread, NULL, publisher_worker_thread, inst) != 0) {
        log_error("Failed to start worker thread for publisher %s", inst->name);
        return -1;
    }
    
    inst->thread_started = 1;
    inst->started = 1;
    
    log_info("Publisher %s started", inst->name);
    return 0;
}

// Stop publisher
int publisher_instance_stop(publisher_instance_t *inst) {
    if (!inst || !inst->started) return 0;
    
    log_info("Stopping publisher: %s", inst->name);
    
    // Stop queue
    pthread_mutex_lock(&inst->q_mutex);
    inst->q_stop = 1;
    pthread_cond_broadcast(&inst->q_cond);
    pthread_mutex_unlock(&inst->q_mutex);
    
    // Wait for worker thread
    if (inst->thread_started) {
        pthread_join(inst->thread, NULL);
        inst->thread_started = 0;
    }
    
    // Call plugin stop if available
    if (inst->plugin->callbacks->stop) {
        inst->plugin->callbacks->stop(inst->plugin->plugin_data);
    }
    
    inst->started = 0;
    
    log_info("Publisher %s stopped (published=%llu, dropped=%llu, errors=%llu)",
            inst->name, inst->events_published, inst->events_dropped, inst->errors);
    
    return 0;
}

// Enqueue event for publishing
int publisher_instance_enqueue(publisher_instance_t *inst, const cdc_event_t *event) {
    if (!inst || !inst->active || !event) return -1;
    
    cdc_event_t *event_copy = cdc_event_copy(event);
    if (!event_copy) {
        inst->events_dropped++;
        return -1;
    }
    
    pthread_mutex_lock(&inst->q_mutex);
    
    // Check if queue is full
    if (inst->q_count >= inst->q_capacity) {
        pthread_mutex_unlock(&inst->q_mutex);
        cdc_event_free(event_copy);
        inst->events_dropped++;
        log_warn("Publisher %s queue full, dropping event", inst->name);
        return -1;
    }
    
    int idx = inst->q_tail;
    inst->queue[idx] = event_copy;
    inst->q_tail = (inst->q_tail + 1) % inst->q_capacity;
    inst->q_count++;
    
    pthread_cond_signal(&inst->q_cond);
    pthread_mutex_unlock(&inst->q_mutex);
    
    return 0;
}

// Check if should publish to this instance
int publisher_should_publish(publisher_instance_t *inst, const char *db) {
    if (!inst || !inst->active || !db) return 0;

    // If no database filter, publish to all
    if (inst->config.db_count == 0) return 1;
    
    // Check if database is in filter list
    for (int i = 0; i < inst->config.db_count; i++) {
        if (strcmp(inst->config.databases[i], db) == 0) {
            return 1;
        }
    }
    
    return 0;
}

// Cleanup instance
void publisher_instance_destroy(publisher_instance_t *inst) {
    if (!inst) return;
    
    // Stop if running
    if (inst->started) {
        publisher_instance_stop(inst);
    }
    
    // Cleanup plugin
    if (inst->plugin) {
        if (inst->plugin->callbacks && inst->plugin->callbacks->cleanup) {
            inst->plugin->callbacks->cleanup(inst->plugin->plugin_data);
        }
        free(inst->plugin);
        inst->plugin = NULL;
    }
    
    // Unload library
    if (inst->dl_handle) {
        dlclose(inst->dl_handle);
        inst->dl_handle = NULL;
    }
    
    // Cleanup queue
    queue_destroy(inst);
    
    // Free config - only if arrays were allocated and contain our own strings
    if (inst->config.name) {
        free((char*)inst->config.name);
    }
    
    if (inst->config.databases) {
        for (int i = 0; i < inst->config.db_count; i++) {
            if (inst->config.databases[i]) {
                free(inst->config.databases[i]);
            }
        }
        free(inst->config.databases);
    }
    
    if (inst->config.config_keys) {
        for (int i = 0; i < inst->config.config_count; i++) {
            if (inst->config.config_keys[i]) {
                free(inst->config.config_keys[i]);
            }
        }
        free(inst->config.config_keys);
    }
    
    if (inst->config.config_values) {
        for (int i = 0; i < inst->config.config_count; i++) {
            if (inst->config.config_values[i]) {
                free(inst->config.config_values[i]);
            }
        }
        free(inst->config.config_values);
    }
    
    free(inst);
}

// Cleanup manager
void publisher_manager_destroy(publisher_manager_t *manager) {
    if (!manager) return;
    
    publisher_instance_t *inst = manager->instances;
    while (inst) {
        publisher_instance_t *next = inst->next;
        publisher_instance_destroy(inst);
        inst = next;
    }
    
    free(manager);
    publisher_helpers = NULL;
}