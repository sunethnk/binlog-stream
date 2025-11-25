// publisher_loader.h
// Dynamic Publisher Plugin Loader
//
// Handles loading, initialization, and management of publisher plugins

#ifndef PUBLISHER_LOADER_H
#define PUBLISHER_LOADER_H

#include "publisher_api.h"
#include <pthread.h>

// Publisher instance (combines plugin with runtime state)
typedef struct publisher_instance {
    char name[128];
    char library_path[512];
    
    // Plugin handle
    void *dl_handle;                    // dlopen handle
    publisher_plugin_t *plugin;         // Plugin descriptor
    
    // Configuration
    publisher_config_t config;
    
    // Runtime state
    int active;
    int started;
    
    // Async queue for event processing
    cdc_event_t **queue;
    int q_head, q_tail, q_count, q_capacity;
    pthread_mutex_t q_mutex;
    pthread_cond_t q_cond;
    int q_stop;
    pthread_t thread;
    int thread_started;
    
    // Statistics
    uint64_t events_published;
    uint64_t events_dropped;
    uint64_t errors;
    
    struct publisher_instance *next;
} publisher_instance_t;

// Publisher manager
typedef struct publisher_manager {
    publisher_instance_t *instances;
    int instance_count;
    
    // Global helpers for plugins
    publisher_api_helpers_t helpers;
    
} publisher_manager_t;

// Initialize publisher manager
int publisher_manager_init(publisher_manager_t **manager);

// Load a publisher plugin from shared library
int publisher_manager_load_plugin(
    publisher_manager_t *manager,
    const char *name,
    const char *library_path,
    const publisher_config_t *config,
    publisher_instance_t **instance
);

// Start/stop publishers
int publisher_instance_start(publisher_instance_t *instance);
int publisher_instance_stop(publisher_instance_t *instance);

// Queue management
int publisher_instance_enqueue(publisher_instance_t *instance, const cdc_event_t *event);

// Cleanup
void publisher_instance_destroy(publisher_instance_t *instance);
void publisher_manager_destroy(publisher_manager_t *manager);

// Database filter check
int publisher_should_publish(publisher_instance_t *instance, const char *db);

#endif // PUBLISHER_LOADER_H
