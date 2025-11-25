// file_publisher.c
// File Publisher Plugin with event-based log rotation
//
// Build: gcc -shared -fPIC -o file_publisher.so file_publisher.c -I.

#include "publisher_api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* Plugin private data */
typedef struct {
    FILE *fp;
    char  file_path[512];

    int      flush_every_event;
    uint64_t events_written;

    /* rotation by event count */
    uint64_t rotate_max_events;   /* 0 = disabled */
    int      rotate_max_files;    /* how many rotated files to keep */
    uint64_t events_in_file;      /* events written to current file */
} file_publisher_data_t;

/* ------------------------------------------------------------------ */
/* Internal helpers                                                   */
/* ------------------------------------------------------------------ */

static int rotate_file(file_publisher_data_t *data) {
    if (!data || !data->file_path[0] || data->rotate_max_files <= 0) {
        return 0; /* nothing to do */
    }

    PLUGIN_LOG_INFO("Rotating file publisher output: %s", data->file_path);

    if (data->fp) {
        fflush(data->fp);
        fclose(data->fp);
        data->fp = NULL;
    }

    char oldname[512];
    char newname[512];

    /* Reserve space for ".<index>\0" ('.' + up to 10 digits + '\0') */
    const size_t suffix_reserve = 12;
    size_t max_base = sizeof(oldname) > suffix_reserve
                      ? sizeof(oldname) - suffix_reserve
                      : 0;

    size_t plen = strnlen(data->file_path, sizeof(data->file_path));
    if (plen > max_base) {
        plen = max_base;
    }

    /* Shift existing rotated files: path.(n-1) -> path.n */
    for (int i = data->rotate_max_files - 1; i >= 1; i--) {
        snprintf(oldname, sizeof(oldname), "%.*s.%d",
                 (int)plen, data->file_path, i - 1);
        snprintf(newname, sizeof(newname), "%.*s.%d",
                 (int)plen, data->file_path, i);
        rename(oldname, newname); /* ignore errors */
    }

    /* Move current base file to .0 */
    snprintf(oldname, sizeof(oldname), "%.*s",
             (int)plen, data->file_path);
    snprintf(newname, sizeof(newname), "%.*s.%d",
             (int)plen, data->file_path, 0);

    rename(oldname, newname); /* ignore errors */

    /* Reopen base file */
    data->fp = fopen(data->file_path, "a");
    if (!data->fp) {
        PLUGIN_LOG_ERROR("Failed to reopen rotated file: %s", data->file_path);
        data->events_in_file = 0;
        return -1;
    }

    data->events_in_file = 0;
    PLUGIN_LOG_INFO("Rotation complete, new file open: %s", data->file_path);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Plugin metadata                                                    */
/* ------------------------------------------------------------------ */

static const char* get_name(void) {
    return "file_publisher";
}

static const char* get_version(void) {
    return "1.0.0";
}

static int get_api_version(void) {
    return PUBLISHER_API_VERSION;
}

/* ------------------------------------------------------------------ */
/* Lifecycle                                                          */
/* ------------------------------------------------------------------ */

/* Initialize publisher */
static int init(const publisher_config_t *config, void **plugin_data) {
    PLUGIN_LOG_INFO("Initializing file publisher");

    file_publisher_data_t *data = calloc(1, sizeof(file_publisher_data_t));
    if (!data) {
        PLUGIN_LOG_ERROR("Failed to allocate publisher data");
        return -1;
    }

    /* Get file path from config */
    const char *file_path = PLUGIN_GET_CONFIG(config, "file_path");
    if (!file_path) {
        PLUGIN_LOG_ERROR("Missing required config: file_path");
        free(data);
        return -1;
    }

    snprintf(data->file_path, sizeof(data->file_path), "%s", file_path);

    /* Get optional flush setting */
    data->flush_every_event = PLUGIN_GET_CONFIG_INT(config, "flush_every_event", 1);

    /* Rotation settings (event-based) */
    data->rotate_max_events = (uint64_t) PLUGIN_GET_CONFIG_INT(
            config, "rotate_max_events", 0);    /* 0 = disabled */

    data->rotate_max_files = PLUGIN_GET_CONFIG_INT(
            config, "rotate_max_files", 5);     /* default 5 files */
    if (data->rotate_max_files < 1) {
        data->rotate_max_files = 1;
    }

    data->events_in_file  = 0;
    data->events_written  = 0;

    *plugin_data = data;

    PLUGIN_LOG_INFO(
        "File publisher configured: path=%s, flush=%d, rotate_max_events=%llu, rotate_max_files=%d",
        data->file_path,
        data->flush_every_event,
        (unsigned long long) data->rotate_max_events,
        data->rotate_max_files
    );

    return 0;
}

/* Start publisher */
static int start(void *plugin_data) {
    file_publisher_data_t *data = (file_publisher_data_t*)plugin_data;

    PLUGIN_LOG_INFO("Starting file publisher: %s", data->file_path);

    data->fp = fopen(data->file_path, "a");
    if (!data->fp) {
        PLUGIN_LOG_ERROR("Failed to open file: %s", data->file_path);
        return -1;
    }

    data->events_in_file = 0;

    PLUGIN_LOG_INFO("File publisher started: %s", data->file_path);
    return 0;
}

/* Publish event */
static int publish(void *plugin_data, const cdc_event_t *event) {
    file_publisher_data_t *data = (file_publisher_data_t*)plugin_data;

    if (!data || !data->fp || !event || !event->json) {
        return -1;
    }

    /* Event-based rotation: rotate before writing when limit reached */
    if (data->rotate_max_events > 0 &&
        data->events_in_file >= data->rotate_max_events) {

        if (rotate_file(data) != 0) {
            /* rotation failed -> better to fail than silently drop data */
            return -1;
        }
    }

    if (fprintf(data->fp, "%s\n", event->json) < 0) {
        PLUGIN_LOG_ERROR("Failed to write event to file: %s", data->file_path);
        return -1;
    }

    data->events_in_file++;
    data->events_written++;

    if (data->flush_every_event) {
        fflush(data->fp);
    }

    PLUGIN_LOG_TRACE("Published event to file: txn=%s, db=%s, table=%s",
                     event->txn   ? event->txn   : "",
                     event->db    ? event->db    : "",
                     event->table ? event->table : "");

    return 0;
}

/* Stop publisher */
static int stop(void *plugin_data) {
    file_publisher_data_t *data = (file_publisher_data_t*)plugin_data;

    PLUGIN_LOG_INFO(
        "Stopping file publisher: %s (events_written=%llu, events_in_file=%llu)",
        data->file_path,
        (unsigned long long) data->events_written,
        (unsigned long long) data->events_in_file
    );

    if (data && data->fp) {
        fflush(data->fp);
        fclose(data->fp);
        data->fp = NULL;
    }

    return 0;
}

/* Cleanup */
static void cleanup(void *plugin_data) {
    file_publisher_data_t *data = (file_publisher_data_t*)plugin_data;

    if (data) {
        if (data->fp) {
            fclose(data->fp);
            data->fp = NULL;
        }
        free(data);
    }

    PLUGIN_LOG_INFO("File publisher cleaned up");
}

/* Health check */
static int health_check(void *plugin_data) {
    file_publisher_data_t *data = (file_publisher_data_t*)plugin_data;
    return (data && data->fp) ? 0 : -1;
}

/* ------------------------------------------------------------------ */
/* Plugin callbacks & entry point                                     */
/* ------------------------------------------------------------------ */

static const publisher_callbacks_t callbacks = {
    .get_name        = get_name,
    .get_version     = get_version,
    .get_api_version = get_api_version,
    .init            = init,
    .start           = start,
    .stop            = stop,
    .cleanup         = cleanup,
    .publish         = publish,
    .publish_batch   = NULL,   /* Not implemented */
    .health_check    = health_check,
};

PUBLISHER_PLUGIN_DEFINE(file_publisher) {
    publisher_plugin_t *p = malloc(sizeof(publisher_plugin_t));
    if (!p) return -1;

    p->callbacks   = &callbacks;
    p->plugin_data = NULL;

    *plugin = p;
    return 0;
}
