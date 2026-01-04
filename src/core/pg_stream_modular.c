// pg_stream_modular.c
// PostgreSQL logical replication streamer with modular publisher plugin system
//
// Build:
//   gcc -O2 -Wall pg_stream_modular.c publisher_loader.c logger.c -o pg_stream \
//       -lpq -lz -luuid -ljson-c -lpthread -ldl -I$(pg_config --includedir)
//
// Key fixes in this version:
//   1) Proper async COPY stream loop: select()/PQconsumeInput()/drain PQgetCopyData()
//   2) PQflush() after PQputCopyData() so status updates actually go out
//   3) Correct LSN parsing from config (no overlapping writes / wrong pointer math)
//   4) Safer JSON config copying for plugin config strings (strdup instead of borrowing json-c pointers)
//   5) Portable htobe64/be64toh on macOS
//
// Restart correctness fixes (NEW):
//   6) On startup, clamp start_lsn to slot.confirmed_flush_lsn (never start ahead of server -> no gaps)
//   7) Best-effort final status update + flush/drain on shutdown so server persists confirmed_flush_lsn

#include <libpq-fe.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <zlib.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <arpa/inet.h>
#include <uuid/uuid.h>
#include <json-c/json.h>
#include <stdarg.h>
#include <pthread.h>
#include <errno.h>
#include <ctype.h>
#include <sys/select.h>

#if defined(__linux__)
#include <endian.h>
#elif defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#define be64toh OSSwapBigToHostInt64
#define htobe64 OSSwapHostToBigInt64
#endif

#include "banner.h"
#include "logger.h"
#include "publisher_loader.h"

// PostgreSQL logical replication message types (pgoutput)
#define PGOUTPUT_BEGIN       'B'
#define PGOUTPUT_COMMIT      'C'
#define PGOUTPUT_ORIGIN      'O'
#define PGOUTPUT_RELATION    'R'
#define PGOUTPUT_TYPE        'Y'
#define PGOUTPUT_INSERT      'I'
#define PGOUTPUT_UPDATE      'U'
#define PGOUTPUT_DELETE      'D'
#define PGOUTPUT_TRUNCATE    'T'
#define PGOUTPUT_MESSAGE     'M'

// PostgreSQL column types (subset - extend as needed)
#define PG_TYPE_BOOL        16
#define PG_TYPE_BYTEA       17
#define PG_TYPE_CHAR        18
#define PG_TYPE_INT8        20
#define PG_TYPE_INT2        21
#define PG_TYPE_INT4        23
#define PG_TYPE_TEXT        25
#define PG_TYPE_OID         26
#define PG_TYPE_FLOAT4      700
#define PG_TYPE_FLOAT8      701
#define PG_TYPE_BPCHAR      1042
#define PG_TYPE_VARCHAR     1043
#define PG_TYPE_DATE        1082
#define PG_TYPE_TIME        1083
#define PG_TYPE_TIMESTAMP   1114
#define PG_TYPE_TIMESTAMPTZ 1184
#define PG_TYPE_INTERVAL    1186
#define PG_TYPE_NUMERIC     1700
#define PG_TYPE_UUID        2950
#define PG_TYPE_JSON        114
#define PG_TYPE_JSONB       3802
#define PG_TYPE_ARRAY       1000

// Column metadata
typedef struct {
    char name[128];
    int position;
    int index;
} column_info_t;

// Table configuration
typedef struct {
    char name[128];
    char **primary_keys;
    int pk_count;
    column_info_t *columns;
    int column_count;
    int capture_all_columns;
} table_config_t;

// Schema configuration
typedef struct {
    char name[128];
    int capture_dml;
    int capture_ddl;
    table_config_t *tables;
    int table_count;
} schema_config_t;

// Main configuration
typedef struct {
    const char *log_level;
    const char *stdout_level;
    int max_log_count;
    uint64_t max_file_size;
    char log_file[512];
    FILE *log_fp;

    char host[256];
    int port;
    char username[128];
    char password[128];
    char dbname[128];

    char slot_name[128];
    char publication_name[128];
    uint64_t start_lsn;
    int save_last_position;
    uint64_t save_position_event_count;
    char checkpoint_file[512];

    publisher_manager_t *publisher_manager;

    schema_config_t *schemas;
    int schema_count;

} config_t;

static volatile int keep_running = 1;
static uint64_t current_lsn = 0;
static uint64_t events_received = 0;
static uint64_t events_since_save = 0;
static char current_txn_id[37] = "";
static int in_transaction = 0;

static config_t g_config;
static pthread_mutex_t checkpoint_mutex = PTHREAD_MUTEX_INITIALIZER;

// Relation (table) mapping
typedef struct pg_relation {
    uint32_t relid;
    char schema[128];
    char name[128];
    uint16_t natts;
    struct {
        uint8_t flags;
        char name[128];
        uint32_t typid;
        int32_t typmod;
    } *attrs;
    struct pg_relation *next;
} pg_relation_t;

static pg_relation_t *g_relations = NULL;

// ============================================================================
// BASIC UTILS
// ============================================================================

static void generate_txn_id(char *out) {
    uuid_t uuid;
    uuid_generate(uuid);
    uuid_unparse_lower(uuid, out);
}

static uint16_t pg_getmsgint16(const char **ptr) {
    uint16_t val;
    memcpy(&val, *ptr, 2);
    *ptr += 2;
    return ntohs(val);
}

static uint32_t pg_getmsgint32(const char **ptr) {
    uint32_t val;
    memcpy(&val, *ptr, 4);
    *ptr += 4;
    return ntohl(val);
}

static uint64_t pg_getmsgint64(const char **ptr) {
    uint64_t val;
    memcpy(&val, *ptr, 8);
    *ptr += 8;
    return be64toh(val);
}

static uint8_t pg_getmsgbyte(const char **ptr) {
    uint8_t val = (uint8_t)**ptr;
    (*ptr)++;
    return val;
}

static char* pg_getmsgstring(const char **ptr) {
    char *str = strdup(*ptr);
    *ptr += strlen(str) + 1;
    return str;
}

static void pg_getmsgbytes(const char **ptr, char *buf, int len) {
    memcpy(buf, *ptr, (size_t)len);
    *ptr += len;
}

static int parse_lsn_text(const char *lsn_str, uint64_t *out) {
    if (!lsn_str || !*lsn_str || !out) return -1;

    unsigned long upper = 0, lower = 0;
    // PostgreSQL typically prints LSN as HEX/HEX
    if (sscanf(lsn_str, "%lx/%lx", &upper, &lower) != 2) return -1;

    *out = ((uint64_t)upper << 32) | (uint64_t)lower;
    return 0;
}

void signal_handler(int sig) {
    (void)sig;
    keep_running = 0;
}

// ============================================================================
// CONFIG HELPERS
// ============================================================================

static schema_config_t* find_schema_config(const char *schema) {
    for(int i = 0; i < g_config.schema_count; i++) {
        if(strcmp(g_config.schemas[i].name, schema) == 0) {
            return &g_config.schemas[i];
        }
    }
    return NULL;
}

static table_config_t* find_table_config(const char *schema, const char *table) {
    schema_config_t *sch_cfg = find_schema_config(schema);
    if(!sch_cfg) return NULL;

    for(int i = 0; i < sch_cfg->table_count; i++) {
        if(strcmp(sch_cfg->tables[i].name, table) == 0) {
            return &sch_cfg->tables[i];
        }
    }
    return NULL;
}

static int should_capture_table(const char *schema, const char *table) {
    return find_table_config(schema, table) != NULL;
}

static int should_capture_dml(const char *schema) {
    schema_config_t *sch_cfg = find_schema_config(schema);
    return sch_cfg ? sch_cfg->capture_dml : 0;
}

static int should_capture_ddl(const char *schema) {
    schema_config_t *sch_cfg = find_schema_config(schema);
    return sch_cfg ? sch_cfg->capture_ddl : 0;
}

static int parse_log_level(const char *str) {
    if(strcasecmp(str, "ERROR") == 0) return LOG_ERROR;
    if(strcasecmp(str, "WARN")  == 0) return LOG_WARN;
    if(strcasecmp(str, "INFO")  == 0) return LOG_INFO;
    if(strcasecmp(str, "DEBUG") == 0) return LOG_DEBUG;
    if(strcasecmp(str, "TRACE") == 0) return LOG_TRACE;
    if(strcasecmp(str, "FATAL") == 0) return LOG_FATAL;
    return LOG_INFO;
}

// ============================================================================
// CONFIG PARSING (JSON)
// ============================================================================

static int log_schema_config(config_t *cfg) {
    log_info("========== Capture Configuration ==========");
    for(int i = 0; i < cfg->schema_count; i++) {
        schema_config_t *sch = &cfg->schemas[i];
        log_info("Schema: %s (DML:%s, DDL:%s, tables:%d)",
                 sch->name,
                 sch->capture_dml ? "YES" : "NO",
                 sch->capture_ddl ? "YES" : "NO",
                 sch->table_count);

        for(int j = 0; j < sch->table_count; j++) {
            table_config_t *tbl = &sch->tables[j];
            if(tbl->capture_all_columns) {
                log_info("  -> %s.%s: ALL COLUMNS (wildcard)", sch->name, tbl->name);
            } else {
                log_info("  -> %s.%s: %d specific columns",
                         sch->name, tbl->name, tbl->column_count);
                for(int k = 0; k < tbl->column_count && k < 10; k++) {
                    log_info("      %s", tbl->columns[k].name);
                }
                if(tbl->column_count > 10) {
                    log_info("      ... and %d more", tbl->column_count - 10);
                }
            }
        }
    }
    log_info("==========================================");
    return 0;
}

static int load_config(const char *filename, config_t *cfg) {
    memset(cfg, 0, sizeof(config_t));
    cfg->log_level = "INFO";
    cfg->stdout_level = "INFO";
    cfg->port = 5432;
    cfg->start_lsn = 0;
    cfg->save_position_event_count = 0;
    cfg->max_log_count = 10;
    cfg->max_file_size = 10 * 1024 * 1024;
    strncpy(cfg->checkpoint_file, "pg_checkpoint.dat", sizeof(cfg->checkpoint_file)-1);
    strncpy(cfg->slot_name, "cdc_slot", sizeof(cfg->slot_name)-1);
    strncpy(cfg->publication_name, "cdc_publication", sizeof(cfg->publication_name)-1);

    FILE *fp = fopen(filename, "r");
    if(!fp) {
        fprintf(stderr, "Cannot open config file: %s\n", filename);
        return -1;
    }

    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char *content = (char*)malloc((size_t)size + 1);
    if(!content) {
        fclose(fp);
        return -1;
    }

    size_t nread = fread(content, 1, (size_t)size, fp);
    if (nread != (size_t)size) {
        log_error("config: expected %ld bytes, got %zu", size, nread);
        free(content);
        fclose(fp);
        return -1;
    }
    content[size] = '\0';
    fclose(fp);

    json_object *root = json_tokener_parse(content);
    free(content);

    if(!root) {
        fprintf(stderr, "Failed to parse config JSON\n");
        return -1;
    }

    // Parse logging config
    json_object *logging = json_object_object_get(root, "logging");
    if(logging) {
        json_object *level = json_object_object_get(logging, "level");
        if(level) cfg->log_level = json_object_get_string(level);

        json_object *log_file = json_object_object_get(logging, "log_file");
        if(log_file) strncpy(cfg->log_file, json_object_get_string(log_file), sizeof(cfg->log_file) - 1);

        json_object *stdout_level = json_object_object_get(logging, "stdout");
        if(stdout_level) cfg->stdout_level = json_object_get_string(stdout_level);

        json_object *max_log_count = json_object_object_get(logging, "max_files");
        if(max_log_count) cfg->max_log_count = json_object_get_int(max_log_count);

        json_object *max_file_size = json_object_object_get(logging, "max_file_size");
        if(max_file_size) cfg->max_file_size = json_object_get_uint64(max_file_size);

    }

    // Parse PostgreSQL server config
    json_object *server = json_object_object_get(root, "postgres_server");
    if(server) {
        json_object *host = json_object_object_get(server, "host");
        if(host) strncpy(cfg->host, json_object_get_string(host), sizeof(cfg->host) - 1);

        json_object *port = json_object_object_get(server, "port");
        if(port) cfg->port = json_object_get_int(port);

        json_object *username = json_object_object_get(server, "username");
        if(username) strncpy(cfg->username, json_object_get_string(username), sizeof(cfg->username) - 1);

        json_object *password = json_object_object_get(server, "password");
        if(password) strncpy(cfg->password, json_object_get_string(password), sizeof(cfg->password) - 1);

        json_object *dbname = json_object_object_get(server, "database");
        if(dbname) strncpy(cfg->dbname, json_object_get_string(dbname), sizeof(cfg->dbname) - 1);
    }

    // Parse replication config
    json_object *replication = json_object_object_get(root, "replication");
    if(replication) {
        json_object *slot_name = json_object_object_get(replication, "slot_name");
        if(slot_name) strncpy(cfg->slot_name, json_object_get_string(slot_name), sizeof(cfg->slot_name) - 1);

        json_object *publication = json_object_object_get(replication, "publication_name");
        if(publication) strncpy(cfg->publication_name, json_object_get_string(publication), sizeof(cfg->publication_name) - 1);

        json_object *start_lsn = json_object_object_get(replication, "start_lsn");
        if(start_lsn) {
            const char *lsn_str = json_object_get_string(start_lsn);
            if(lsn_str && strcmp(lsn_str, "current") != 0) {
                uint64_t parsed = 0;
                if (parse_lsn_text(lsn_str, &parsed) == 0) {
                    cfg->start_lsn = parsed;
                } else {
                    log_warn("Invalid start_lsn in config: %s", lsn_str);
                }
            }
        }

        json_object *save_last_position = json_object_object_get(replication, "save_last_position");
        if(save_last_position) cfg->save_last_position = json_object_get_boolean(save_last_position);

        json_object *save_event_count = json_object_object_get(replication, "save_position_event_count");
        if(save_event_count) cfg->save_position_event_count = (uint64_t)json_object_get_int64(save_event_count);

        json_object *checkpoint_file = json_object_object_get(replication, "checkpoint_file");
        if(checkpoint_file) strncpy(cfg->checkpoint_file, json_object_get_string(checkpoint_file), sizeof(cfg->checkpoint_file) - 1);
    }

    // Parse capture config (schemas/tables)
    json_object *capture = json_object_object_get(root, "capture");
    if(capture) {
        json_object *schemas = json_object_object_get(capture, "schemas");
        if(schemas && json_object_is_type(schemas, json_type_array)) {
            int sch_count = json_object_array_length(schemas);
            cfg->schemas = (schema_config_t*)calloc((size_t)sch_count, sizeof(schema_config_t));
            cfg->schema_count = 0;

            for(int i = 0; i < sch_count; i++) {
                json_object *sch_wrapper = json_object_array_get_idx(schemas, i);
                json_object_object_foreach(sch_wrapper, sch_name, sch_obj) {
                    schema_config_t *sch_cfg = &cfg->schemas[cfg->schema_count++];
                    strncpy(sch_cfg->name, sch_name, sizeof(sch_cfg->name) - 1);

                    json_object *capture_dml = json_object_object_get(sch_obj, "capture_dml");
                    sch_cfg->capture_dml = capture_dml ? json_object_get_boolean(capture_dml) : 1;

                    json_object *capture_ddl = json_object_object_get(sch_obj, "capture_ddl");
                    sch_cfg->capture_ddl = capture_ddl ? json_object_get_boolean(capture_ddl) : 1;

                    json_object *tables = json_object_object_get(sch_obj, "tables");
                    if(tables && json_object_is_type(tables, json_type_array)) {
                        int tbl_count = json_object_array_length(tables);
                        sch_cfg->tables = (table_config_t*)calloc((size_t)tbl_count, sizeof(table_config_t));
                        sch_cfg->table_count = 0;

                        for(int j = 0; j < tbl_count; j++) {
                            json_object *tbl_wrapper = json_object_array_get_idx(tables, j);
                            json_object_object_foreach(tbl_wrapper, tbl_name, tbl_obj) {
                                table_config_t *tbl_cfg = &sch_cfg->tables[sch_cfg->table_count++];
                                strncpy(tbl_cfg->name, tbl_name, sizeof(tbl_cfg->name) - 1);

                                // Parse primary key
                                json_object *primary_key = json_object_object_get(tbl_obj, "primary_key");
                                if (primary_key) {
                                    if (json_object_is_type(primary_key, json_type_array)) {
                                        int pkc = json_object_array_length(primary_key);
                                        if (pkc > 0) {
                                            tbl_cfg->pk_count = pkc;
                                            tbl_cfg->primary_keys = (char**)calloc((size_t)pkc, sizeof(char*));
                                            for (int pk_i = 0; pk_i < pkc; pk_i++) {
                                                json_object *pk_item = json_object_array_get_idx(primary_key, pk_i);
                                                const char *pk_name = json_object_get_string(pk_item);
                                                if (pk_name && pk_name[0]) {
                                                    tbl_cfg->primary_keys[pk_i] = strdup(pk_name);
                                                }
                                            }
                                        }
                                    } else if (json_object_is_type(primary_key, json_type_string)) {
                                        const char *pk_name = json_object_get_string(primary_key);
                                        if (pk_name && pk_name[0]) {
                                            tbl_cfg->pk_count = 1;
                                            tbl_cfg->primary_keys = (char**)calloc(1, sizeof(char*));
                                            tbl_cfg->primary_keys[0] = strdup(pk_name);
                                        }
                                    }
                                }

                                // Parse columns
                                json_object *columns = json_object_object_get(tbl_obj, "columns");
                                if(columns && json_object_is_type(columns, json_type_array)) {
                                    int col_count = json_object_array_length(columns);

                                    if(col_count == 1) {
                                        json_object *first_col = json_object_array_get_idx(columns, 0);
                                        const char *col_str = json_object_get_string(first_col);
                                        if(col_str && strcmp(col_str, "*") == 0) {
                                            tbl_cfg->capture_all_columns = 1;
                                            tbl_cfg->column_count = 0;
                                            tbl_cfg->columns = NULL;
                                            continue;
                                        }
                                    }

                                    tbl_cfg->capture_all_columns = 0;
                                    tbl_cfg->columns = (column_info_t*)calloc((size_t)col_count, sizeof(column_info_t));
                                    tbl_cfg->column_count = col_count;

                                    for(int k = 0; k < col_count; k++) {
                                        json_object *col = json_object_array_get_idx(columns, k);
                                        const char *col_name = json_object_get_string(col);
                                        if (col_name) {
                                            strncpy(tbl_cfg->columns[k].name, col_name,
                                                    sizeof(tbl_cfg->columns[k].name) - 1);
                                        }
                                        tbl_cfg->columns[k].position = -1;
                                        tbl_cfg->columns[k].index = -1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Load publisher plugins
    if (publisher_manager_init(&cfg->publisher_manager) != 0) {
        log_error("Failed to initialize publisher manager");
        json_object_put(root);
        return -1;
    }

    json_object *publishers = json_object_object_get(root, "publishers");
    if(publishers && json_object_is_type(publishers, json_type_array)) {
        int pub_count = json_object_array_length(publishers);

        for(int i = 0; i < pub_count; i++) {
            json_object *pub_obj = json_object_array_get_idx(publishers, i);

            json_object *plugin_obj = json_object_object_get(pub_obj, "plugin");
            if (plugin_obj) {
                json_object *name_obj = json_object_object_get(plugin_obj, "name");
                json_object *lib_obj  = json_object_object_get(plugin_obj, "library_path");
                json_object *active_obj = json_object_object_get(plugin_obj, "active");
                json_object *max_queue_obj = json_object_object_get(plugin_obj, "max_queue_depth");

                if (!name_obj || !lib_obj) {
                    log_warn("Plugin missing required fields (name, library_path)");
                    continue;
                }

                const char *name = json_object_get_string(name_obj);
                const char *lib_path = json_object_get_string(lib_obj);
                int active = active_obj ? json_object_get_boolean(active_obj) : 1;
                uint64_t qdepth = max_queue_obj ? json_object_get_uint64(max_queue_obj) : 1024;

                publisher_config_t config;
                memset(&config, 0, sizeof(config));
                config.name = name;
                config.active = active;
                config.max_q_depth = qdepth;

                // Parse schema filter (publish_schemas)
                json_object *pub_schemas = json_object_object_get(plugin_obj, "publish_schemas");
                if (pub_schemas && json_object_is_type(pub_schemas, json_type_array)) {
                    config.db_count = json_object_array_length(pub_schemas);
                    config.databases = (char**)calloc((size_t)config.db_count, sizeof(char*));

                    for (int j = 0; j < config.db_count; j++) {
                        json_object *sch = json_object_array_get_idx(pub_schemas, j);
                        const char *s = json_object_get_string(sch);
                        config.databases[j] = s ? strdup(s) : strdup("");
                    }
                }

                // Parse custom config (copy strings; do not borrow json-c pointers)
                json_object *config_obj = json_object_object_get(plugin_obj, "config");
                if (config_obj && json_object_is_type(config_obj, json_type_object)) {
                    int count = 0;
                    json_object_object_foreach(config_obj, key, val) { (void)key; (void)val; count++; }
                    config.config_count = count;

                    if (config.config_count > 0) {
                        config.config_keys = (char**)calloc((size_t)config.config_count, sizeof(char*));
                        config.config_values = (char**)calloc((size_t)config.config_count, sizeof(char*));

                        int idx = 0;
                        json_object_object_foreach(config_obj, key, val) {
                            const char *v = json_object_get_string(val);
                            config.config_keys[idx] = strdup(key ? key : "");
                            config.config_values[idx] = strdup(v ? v : "");
                            idx++;
                        }
                    }
                }

                publisher_instance_t *inst = NULL;
                if (publisher_manager_load_plugin(
                        cfg->publisher_manager,
                        name,
                        lib_path,
                        &config,
                        &inst) == 0) {
                    log_info("Loaded publisher plugin: %s", name);
                } else {
                    log_warn("Failed to load publisher plugin: %s", name);
                }

                // Free copied config strings
                if (config.databases) {
                    for (int j = 0; j < config.db_count; j++) free(config.databases[j]);
                    free(config.databases);
                }
                if (config.config_keys) {
                    for (int j = 0; j < config.config_count; j++) free(config.config_keys[j]);
                    free(config.config_keys);
                }
                if (config.config_values) {
                    for (int j = 0; j < config.config_count; j++) free(config.config_values[j]);
                    free(config.config_values);
                }
            }
        }
    }

    json_object_put(root);

    log_info("Configuration loaded successfully");
    log_info("PostgreSQL: %s:%d/%s", cfg->host, cfg->port, cfg->dbname);
    log_info("Replication slot: %s", cfg->slot_name);
    log_info("Publication: %s", cfg->publication_name);

    return 0;
}

// ============================================================================
// POSITION PERSISTENCE
// ============================================================================

static void save_position(uint64_t lsn) {
    if(!g_config.save_last_position) return;

    pthread_mutex_lock(&checkpoint_mutex);

    FILE *fp = fopen(g_config.checkpoint_file, "w");
    if(!fp) {
        log_warn("Cannot save checkpoint to %s", g_config.checkpoint_file);
        pthread_mutex_unlock(&checkpoint_mutex);
        return;
    }

    fprintf(fp, "%lX/%lX\n", (unsigned long)(lsn >> 32), (unsigned long)(lsn & 0xFFFFFFFF));
    fclose(fp);

    log_debug("Checkpoint saved: %lX/%lX",
        (unsigned long)(lsn >> 32), (unsigned long)(lsn & 0xFFFFFFFF));

    pthread_mutex_unlock(&checkpoint_mutex);
}

static int restore_position(uint64_t *lsn) {
    if(!g_config.save_last_position) return -1;

    FILE *fp = fopen(g_config.checkpoint_file, "r");
    if(!fp) return -1;

    unsigned long upper = 0, lower = 0;
    if(fscanf(fp, "%lX/%lX\n", &upper, &lower) != 2) {
        fclose(fp);
        return -1;
    }

    *lsn = ((uint64_t)upper << 32) | (uint64_t)lower;
    fclose(fp);

    log_info("Restored checkpoint: %lX/%lX", upper, lower);
    return 0;
}

// NEW: read server-side slot confirmed_flush_lsn (authoritative resume point)
static int read_slot_confirmed_flush_lsn(PGconn *conn, const char *slot, uint64_t *out_lsn) {
    if (!conn || !slot || !*slot || !out_lsn) return -1;

    // slot_name is controlled by your config; still keep it simple.
    // If you want to harden further, parameterize or validate slot_name chars [a-zA-Z0-9_].
    char q[512];
    snprintf(q, sizeof(q),
             "SELECT confirmed_flush_lsn "
             "FROM pg_replication_slots WHERE slot_name = '%s'", slot);

    PGresult *res = PQexec(conn, q);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        log_warn("Failed to query slot confirmed_flush_lsn: %s", PQerrorMessage(conn));
        if (res) PQclear(res);
        return -1;
    }

    if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0)) {
        log_warn("Slot %s not found or confirmed_flush_lsn is NULL", slot);
        PQclear(res);
        return -1;
    }

    const char *lsn_str = PQgetvalue(res, 0, 0);
    uint64_t parsed = 0;
    if (parse_lsn_text(lsn_str, &parsed) != 0) {
        log_warn("Could not parse confirmed_flush_lsn: %s", lsn_str);
        PQclear(res);
        return -1;
    }

    *out_lsn = parsed;
    PQclear(res);
    return 0;
}

// ============================================================================
// RELATION CACHE
// ============================================================================

static pg_relation_t* find_relation(uint32_t relid) {
    pg_relation_t *rel = g_relations;
    while(rel) {
        if(rel->relid == relid) return rel;
        rel = rel->next;
    }
    return NULL;
}

static void add_relation(pg_relation_t *rel) {
    rel->next = g_relations;
    g_relations = rel;
}

static void free_relations(void) {
    pg_relation_t *rel = g_relations;
    while(rel) {
        pg_relation_t *next = rel->next;
        if(rel->attrs) free(rel->attrs);
        free(rel);
        rel = next;
    }
    g_relations = NULL;
}

// ============================================================================
// PUBLISH EVENT
// ============================================================================

void publish_event(const char *schema, const char *table,
                   const char *event_json, const char *txn) {
    if (!g_config.publisher_manager) return;

    cdc_event_t event = {
        .db = schema,
        .table = table,
        .json = event_json,
        .txn = txn,
        .position = current_lsn,
        .binlog_file = ""
    };

    int dispatched = 0;
    publisher_instance_t *inst = g_config.publisher_manager->instances;

    while (inst) {
        if (publisher_should_publish(inst, schema)) {
            if (publisher_instance_enqueue(inst, &event) == 0) {
                log_trace("Dispatching event publisher=%s txn=%s schema=%s table=%s LSN=%lX/%lX: %s",
                    inst->name, txn, schema, table,
                    (unsigned long)(current_lsn >> 32), (unsigned long)(current_lsn & 0xFFFFFFFF),
                    event_json);
                dispatched++;
            }
        }
        inst = inst->next;
    }

    if (dispatched > 0) {
        log_debug("Dispatched to %d publisher(s) for schema=%s table=%s",
                  dispatched, schema ? schema : "", table ? table : "");
    }
}

// ============================================================================
// VALUE FORMATTERS
// ============================================================================

static void append_value_to_json(char *json_buf, size_t buf_size, size_t *offset,
                                 const char *value, int len, uint32_t typid,
                                 const char *col_name) {
    *offset += snprintf(json_buf + *offset, buf_size - *offset, "\"%s\":", col_name);

    if (!value) {
        *offset += snprintf(json_buf + *offset, buf_size - *offset, "null");
        return;
    }

    switch(typid) {
        case PG_TYPE_BOOL:
            *offset += snprintf(json_buf + *offset, buf_size - *offset,
                                "%s", value[0] == 't' ? "true" : "false");
            break;

        case PG_TYPE_INT2:
        case PG_TYPE_INT4:
        case PG_TYPE_INT8:
        case PG_TYPE_OID:
            *offset += snprintf(json_buf + *offset, buf_size - *offset, "%s", value);
            break;

        case PG_TYPE_FLOAT4:
        case PG_TYPE_FLOAT8:
        case PG_TYPE_NUMERIC:
            *offset += snprintf(json_buf + *offset, buf_size - *offset, "%s", value);
            break;

        case PG_TYPE_JSON:
        case PG_TYPE_JSONB:
            *offset += snprintf(json_buf + *offset, buf_size - *offset, "%.*s", len, value);
            break;

        default:
            *offset += snprintf(json_buf + *offset, buf_size - *offset, "\"");
            for(int i = 0; i < len && *offset < buf_size - 10; i++) {
                char c = value[i];
                if(c == '"') *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\\"");
                else if(c == '\\') *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\\\");
                else if(c == '\n') *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\n");
                else if(c == '\r') *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\r");
                else if(c == '\t') *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\t");
                else if((unsigned char)c < 32)
                    *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\u%04x", (unsigned char)c);
                else json_buf[(*offset)++] = c;
            }
            *offset += snprintf(json_buf + *offset, buf_size - *offset, "\"");
            break;
    }
}

static void append_primary_key_metadata(char *json_buf, size_t buf_size,
                                        size_t *json_offset,
                                        const char *schema, const char *table) {
    table_config_t *tbl_cfg = find_table_config(schema, table);
    if (!tbl_cfg || tbl_cfg->pk_count <= 0 || !tbl_cfg->primary_keys) return;

    *json_offset += snprintf(json_buf + *json_offset,
                             buf_size - *json_offset,
                             ",\"primary_key\":[");
    for (int i = 0; i < tbl_cfg->pk_count; i++) {
        if (i > 0) *json_offset += snprintf(json_buf + *json_offset, buf_size - *json_offset, ",");
        const char *pk_name = tbl_cfg->primary_keys[i] ? tbl_cfg->primary_keys[i] : "";
        *json_offset += snprintf(json_buf + *json_offset, buf_size - *json_offset, "\"%s\"", pk_name);
    }
    *json_offset += snprintf(json_buf + *json_offset, buf_size - *json_offset, "]");
}

// ============================================================================
// MESSAGE PARSERS
// ============================================================================

static void parse_relation_message(const char **ptr) {
    uint32_t relid = pg_getmsgint32(ptr);
    char *schema = pg_getmsgstring(ptr);
    char *relname = pg_getmsgstring(ptr);
    uint8_t replica_identity = pg_getmsgbyte(ptr);
    (void)replica_identity;
    uint16_t natts = pg_getmsgint16(ptr);

    log_debug("RELATION: relid=%u schema=%s table=%s natts=%u",
              relid, schema, relname, natts);

    if(!should_capture_table(schema, relname)) {
        log_debug("Table %s.%s not in capture list, skipping", schema, relname);
        free(schema);
        free(relname);

        // Skip attribute definitions
        for(int i = 0; i < natts; i++) {
            pg_getmsgbyte(ptr);            // flags
            free(pg_getmsgstring(ptr));    // name
            pg_getmsgint32(ptr);           // typid
            pg_getmsgint32(ptr);           // typmod
        }
        return;
    }

    pg_relation_t *rel = (pg_relation_t*)malloc(sizeof(pg_relation_t));
    memset(rel, 0, sizeof(pg_relation_t));

    rel->relid = relid;
    strncpy(rel->schema, schema, sizeof(rel->schema) - 1);
    strncpy(rel->name, relname, sizeof(rel->name) - 1);
    rel->natts = natts;
    rel->attrs = (typeof(rel->attrs))calloc(natts, sizeof(*rel->attrs));

    for(int i = 0; i < natts; i++) {
        rel->attrs[i].flags = pg_getmsgbyte(ptr);
        char *name = pg_getmsgstring(ptr);
        strncpy(rel->attrs[i].name, name, sizeof(rel->attrs[i].name) - 1);
        free(name);
        rel->attrs[i].typid = pg_getmsgint32(ptr);
        rel->attrs[i].typmod = (int32_t)pg_getmsgint32(ptr);

        log_trace("  Column: %s typid=%u", rel->attrs[i].name, rel->attrs[i].typid);
    }

    // Map columns to table config
    table_config_t *tbl_cfg = find_table_config(schema, relname);
    if(tbl_cfg) {
        if(tbl_cfg->capture_all_columns) {
            // allocate columns list if not already allocated
            if (tbl_cfg->columns) free(tbl_cfg->columns);
            tbl_cfg->column_count = natts;
            tbl_cfg->columns = (column_info_t*)calloc(natts, sizeof(column_info_t));
            for(int i = 0; i < natts; i++) {
                strncpy(tbl_cfg->columns[i].name, rel->attrs[i].name, sizeof(tbl_cfg->columns[i].name) - 1);
                tbl_cfg->columns[i].index = i;
                tbl_cfg->columns[i].position = i;
            }
        } else {
            for(int i = 0; i < tbl_cfg->column_count; i++) {
                tbl_cfg->columns[i].index = -1;
                for(int j = 0; j < natts; j++) {
                    if(strcmp(tbl_cfg->columns[i].name, rel->attrs[j].name) == 0) {
                        tbl_cfg->columns[i].index = j;
                        break;
                    }
                }
            }
        }
    }

    add_relation(rel);

    free(schema);
    free(relname);
}

static void parse_insert_message(const char **ptr) {
    uint32_t relid = pg_getmsgint32(ptr);
    pg_relation_t *rel = find_relation(relid);

    if(!rel) {
        log_warn("INSERT: relation %u not found", relid);
        return;
    }
    if(!should_capture_dml(rel->schema)) return;

    table_config_t *tbl_cfg = find_table_config(rel->schema, rel->name);
    if(!tbl_cfg) return;

    uint8_t tuple_type = pg_getmsgbyte(ptr); // 'N'
    (void)tuple_type;

    uint16_t ncols = pg_getmsgint16(ptr);

    char json_event[32768];
    size_t json_offset = 0;

    json_offset += snprintf(json_event, sizeof(json_event),
        "{\"type\":\"INSERT\",\"txn\":\"%s\",\"schema\":\"%s\",\"table\":\"%s\"",
        current_txn_id, rel->schema, rel->name);

    append_primary_key_metadata(json_event, sizeof(json_event), &json_offset, rel->schema, rel->name);

    json_offset += snprintf(json_event + json_offset, sizeof(json_event) - json_offset,
                            ",\"rows\":[{");

    int first = 1;
    for(int i = 0; i < ncols; i++) {
        uint8_t col_type = pg_getmsgbyte(ptr);

        char *value = NULL;
        int len = 0;

        if(col_type == 'n') {
            // NULL
        } else if(col_type == 'u') {
            // TOAST unchanged (no bytes)
        } else if(col_type == 't') {
            len = (int)pg_getmsgint32(ptr);
            value = (char*)malloc((size_t)len + 1);
            pg_getmsgbytes(ptr, value, len);
            value[len] = '\0';
        }

        int should_include = 0;
        const char *col_name = NULL;

        if(i < (int)rel->natts) {
            col_name = rel->attrs[i].name;
            if(tbl_cfg->capture_all_columns) {
                should_include = 1;
            } else {
                for(int j = 0; j < tbl_cfg->column_count; j++) {
                    if(tbl_cfg->columns[j].index == i) { should_include = 1; break; }
                }
            }
        }

        if(should_include && col_name) {
            if(!first) json_offset += snprintf(json_event + json_offset, sizeof(json_event) - json_offset, ",");
            first = 0;

            uint32_t typid = (i < (int)rel->natts) ? rel->attrs[i].typid : PG_TYPE_TEXT;
            append_value_to_json(json_event, sizeof(json_event), &json_offset, value, len, typid, col_name);
        }

        if(value) free(value);
    }

    json_offset += snprintf(json_event + json_offset, sizeof(json_event) - json_offset, "}]}");

    publish_event(rel->schema, rel->name, json_event, current_txn_id);
    log_debug("INSERT %s.%s: 1 row captured", rel->schema, rel->name);
    events_since_save++;
}

static void parse_update_message(const char **ptr) {
    uint32_t relid = pg_getmsgint32(ptr);
    pg_relation_t *rel = find_relation(relid);

    if(!rel) {
        log_warn("UPDATE: relation %u not found", relid);
        return;
    }
    if(!should_capture_dml(rel->schema)) return;

    table_config_t *tbl_cfg = find_table_config(rel->schema, rel->name);
    if(!tbl_cfg) return;

    uint8_t old_tuple_type = pg_getmsgbyte(ptr);
    uint16_t old_ncols = 0;
    char **old_values = NULL;
    int *old_lens = NULL;

    if(old_tuple_type == 'K' || old_tuple_type == 'O') {
        old_ncols = pg_getmsgint16(ptr);
        old_values = (char**)calloc(old_ncols, sizeof(char*));
        old_lens = (int*)calloc(old_ncols, sizeof(int));

        for(int i = 0; i < old_ncols; i++) {
            uint8_t col_type = pg_getmsgbyte(ptr);
            if(col_type == 't') {
                old_lens[i] = (int)pg_getmsgint32(ptr);
                old_values[i] = (char*)malloc((size_t)old_lens[i] + 1);
                pg_getmsgbytes(ptr, old_values[i], old_lens[i]);
                old_values[i][old_lens[i]] = '\0';
            } else if(col_type == 'n' || col_type == 'u') {
                // no data
            }
        }
    } else {
        // If old_tuple_type is actually new tuple indicator, pgoutput format differs; but keep as original intent.
        // In practice, for UPDATE, you may get 'K'/'O' followed by 'N', or directly 'N'.
    }

    uint8_t new_tuple_type = pg_getmsgbyte(ptr); // likely 'N'
    (void)new_tuple_type;
    uint16_t new_ncols = pg_getmsgint16(ptr);

    char json_event[32768];
    size_t json_offset = 0;

    json_offset += snprintf(json_event, sizeof(json_event),
        "{\"type\":\"UPDATE\",\"txn\":\"%s\",\"schema\":\"%s\",\"table\":\"%s\"",
        current_txn_id, rel->schema, rel->name);

    append_primary_key_metadata(json_event, sizeof(json_event), &json_offset, rel->schema, rel->name);

    json_offset += snprintf(json_event + json_offset, sizeof(json_event) - json_offset,
                            ",\"rows\":[{");

    if(old_values) {
        json_offset += snprintf(json_event + json_offset, sizeof(json_event) - json_offset,
                                "\"before\":{");

        int first = 1;
        for(int i = 0; i < (int)old_ncols && i < (int)rel->natts; i++) {
            int should_include = 0;
            const char *col_name = rel->attrs[i].name;

            if(tbl_cfg->capture_all_columns) {
                should_include = 1;
            } else {
                for(int j = 0; j < tbl_cfg->column_count; j++) {
                    if(tbl_cfg->columns[j].index == i) { should_include = 1; break; }
                }
            }

            if(should_include) {
                if(!first) json_offset += snprintf(json_event + json_offset, sizeof(json_event) - json_offset, ",");
                first = 0;

                append_value_to_json(json_event, sizeof(json_event), &json_offset,
                                     old_values[i], old_lens[i], rel->attrs[i].typid, col_name);
            }
        }

        json_offset += snprintf(json_event + json_offset, sizeof(json_event) - json_offset, "},");
    }

    json_offset += snprintf(json_event + json_offset, sizeof(json_event) - json_offset,
                            "\"after\":{");

    int first = 1;
    for(int i = 0; i < (int)new_ncols; i++) {
        uint8_t col_type = pg_getmsgbyte(ptr);

        char *value = NULL;
        int len = 0;

        if(col_type == 't') {
            len = (int)pg_getmsgint32(ptr);
            value = (char*)malloc((size_t)len + 1);
            pg_getmsgbytes(ptr, value, len);
            value[len] = '\0';
        } else if(col_type == 'n' || col_type == 'u') {
            // no data
        }

        int should_include = 0;
        const char *col_name = NULL;

        if(i < (int)rel->natts) {
            col_name = rel->attrs[i].name;
            if(tbl_cfg->capture_all_columns) {
                should_include = 1;
            } else {
                for(int j = 0; j < tbl_cfg->column_count; j++) {
                    if(tbl_cfg->columns[j].index == i) { should_include = 1; break; }
                }
            }
        }

        if(should_include && col_name) {
            if(!first) json_offset += snprintf(json_event + json_offset, sizeof(json_event) - json_offset, ",");
            first = 0;

            uint32_t typid = (i < (int)rel->natts) ? rel->attrs[i].typid : PG_TYPE_TEXT;
            append_value_to_json(json_event, sizeof(json_event), &json_offset,
                                 value, len, typid, col_name);
        }

        if(value) free(value);
    }

    json_offset += snprintf(json_event + json_offset, sizeof(json_event) - json_offset, "}}]}");

    publish_event(rel->schema, rel->name, json_event, current_txn_id);
    log_debug("UPDATE %s.%s: 1 row captured", rel->schema, rel->name);
    
    events_since_save++;

    if(old_values) {
        for(int i = 0; i < (int)old_ncols; i++) free(old_values[i]);
        free(old_values);
        free(old_lens);
    }
}

static void parse_delete_message(const char **ptr) {
    uint32_t relid = pg_getmsgint32(ptr);
    pg_relation_t *rel = find_relation(relid);

    if(!rel) {
        log_warn("DELETE: relation %u not found", relid);
        return;
    }
    if(!should_capture_dml(rel->schema)) return;

    table_config_t *tbl_cfg = find_table_config(rel->schema, rel->name);
    if(!tbl_cfg) return;

    uint8_t tuple_type = pg_getmsgbyte(ptr); // 'K' or 'O'
    uint16_t ncols = pg_getmsgint16(ptr);
    (void)tuple_type;

    char json_event[32768];
    size_t json_offset = 0;

    json_offset += snprintf(json_event, sizeof(json_event),
        "{\"type\":\"DELETE\",\"txn\":\"%s\",\"schema\":\"%s\",\"table\":\"%s\"",
        current_txn_id, rel->schema, rel->name);

    append_primary_key_metadata(json_event, sizeof(json_event), &json_offset, rel->schema, rel->name);

    json_offset += snprintf(json_event + json_offset, sizeof(json_event) - json_offset,
                            ",\"rows\":[{");

    int first = 1;
    for(int i = 0; i < (int)ncols; i++) {
        uint8_t col_type = pg_getmsgbyte(ptr);

        char *value = NULL;
        int len = 0;

        if(col_type == 't') {
            len = (int)pg_getmsgint32(ptr);
            value = (char*)malloc((size_t)len + 1);
            pg_getmsgbytes(ptr, value, len);
            value[len] = '\0';
        } else if(col_type == 'n' || col_type == 'u') {
            // no data
        }

        int should_include = 0;
        const char *col_name = NULL;

        if(i < (int)rel->natts) {
            col_name = rel->attrs[i].name;
            if(tbl_cfg->capture_all_columns) {
                should_include = 1;
            } else {
                for(int j = 0; j < tbl_cfg->column_count; j++) {
                    if(tbl_cfg->columns[j].index == i) { should_include = 1; break; }
                }
            }
        }

        if(should_include && col_name) {
            if(!first) json_offset += snprintf(json_event + json_offset, sizeof(json_event) - json_offset, ",");
            first = 0;

            uint32_t typid = (i < (int)rel->natts) ? rel->attrs[i].typid : PG_TYPE_TEXT;
            append_value_to_json(json_event, sizeof(json_event), &json_offset, value, len, typid, col_name);
        }

        if(value) free(value);
    }

    json_offset += snprintf(json_event + json_offset, sizeof(json_event) - json_offset, "}]}");

    publish_event(rel->schema, rel->name, json_event, current_txn_id);
    log_debug("DELETE %s.%s: 1 row captured", rel->schema, rel->name);
    
    events_since_save++;
}

static void parse_begin_message(const char **ptr) {
    uint64_t final_lsn = pg_getmsgint64(ptr);
    uint64_t commit_time = pg_getmsgint64(ptr);
    uint32_t xid = pg_getmsgint32(ptr);

    (void)final_lsn;
    (void)commit_time;

    generate_txn_id(current_txn_id);
    in_transaction = 1;

    log_debug("[txn:%s] BEGIN transaction (pg_xid=%u)", current_txn_id, xid);
}

static void parse_commit_message(const char **ptr) {
    uint8_t flags = pg_getmsgbyte(ptr);
    uint64_t commit_lsn = pg_getmsgint64(ptr);
    uint64_t end_lsn = pg_getmsgint64(ptr);
    uint64_t commit_time = pg_getmsgint64(ptr);

    (void)flags;
    (void)commit_time;

    current_lsn = end_lsn;

    if(in_transaction && current_txn_id[0]) {
        log_debug("[txn:%s] COMMIT at LSN %lX/%lX",
                 current_txn_id,
                 (unsigned long)(commit_lsn >> 32),
                 (unsigned long)(commit_lsn & 0xFFFFFFFF));
    }

    in_transaction = 0;
    current_txn_id[0] = '\0';

    //events_since_save++;
    if(g_config.save_last_position) {
        if(g_config.save_position_event_count > 0) {
            if(events_since_save >= g_config.save_position_event_count) {
                save_position(current_lsn);
                events_since_save = 0;
            }
        } else {
            save_position(current_lsn);
            events_since_save = 0;
        }
    }
}

// ============================================================================
// MAIN STREAM LOOP (FIXED)
// ============================================================================

static int send_status_update(PGconn *conn, int force_reply) {
    (void)force_reply;

    // Per protocol: write/flush/apply LSN + timestamp + reply-requested byte
    // NOTE: feedback message is: 'r' + 8 + 8 + 8 + 8 + 1 = 34 bytes
    char statusbuf[1 + 8 + 8 + 8 + 8 + 1];

    uint64_t write_lsn = current_lsn;
    uint64_t flush_lsn = current_lsn;
    uint64_t apply_lsn = current_lsn;

    // PostgreSQL epoch starts 2000-01-01, Unix starts 1970-01-01
    // offset = 946684800 seconds -> microseconds below
    int64_t now = time(NULL);
    int64_t timestamp = now * 1000000LL + 946684800000000LL;

    statusbuf[0] = 'r';
    uint64_t tmp = htobe64(write_lsn);
    memcpy(&statusbuf[1], &tmp, 8);
    tmp = htobe64(flush_lsn);
    memcpy(&statusbuf[9], &tmp, 8);
    tmp = htobe64(apply_lsn);
    memcpy(&statusbuf[17], &tmp, 8);
    tmp = htobe64((uint64_t)timestamp);
    memcpy(&statusbuf[25], &tmp, 8);

    // reply requested?
    statusbuf[33] = 0;

    if(PQputCopyData(conn, statusbuf, (int)sizeof(statusbuf)) < 0) {
        log_error("Failed to send status update: %s", PQerrorMessage(conn));
        return -1;
    }
    if (PQflush(conn) == -1) {
        log_error("PQflush failed: %s", PQerrorMessage(conn));
        return -1;
    }
    return 0;
}

static int stream_changes(PGconn *conn, uint64_t start_lsn) {
    char query[1024];

    snprintf(query, sizeof(query),
        "START_REPLICATION SLOT %s LOGICAL %lX/%lX ("
        "proto_version '1', publication_names '%s')",
        g_config.slot_name,
        (unsigned long)(start_lsn >> 32),
        (unsigned long)(start_lsn & 0xFFFFFFFF),
        g_config.publication_name);

    PGresult *res = PQexec(conn, query);
    if (!res || PQresultStatus(res) != PGRES_COPY_BOTH) {
        log_error("Could not start replication: %s", PQerrorMessage(conn));
        if (res) PQclear(res);
        return -1;
    }
    PQclear(res);

    // Set nonblocking so our select()/consume loop works cleanly
    if (PQsetnonblocking(conn, 1) != 0) {
        log_warn("PQsetnonblocking failed (continuing): %s", PQerrorMessage(conn));
    }

    int sock = PQsocket(conn);
    if (sock < 0) {
        log_error("PQsocket returned invalid fd");
        return -1;
    }

    log_info("Streaming from LSN %lX/%lX",
        (unsigned long)(start_lsn >> 32), (unsigned long)(start_lsn & 0xFFFFFFFF));
    log_info("Waiting for events (Ctrl+C to stop)...");

    char *copybuf = NULL;
    int64_t last_status = time(NULL);

    while(keep_running) {

        // Wait for readability with timeout so we can send periodic status
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(sock, &rfds);

        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;

        int sel = select(sock + 1, &rfds, NULL, NULL, &tv);
        if (sel < 0) {
            if (errno == EINTR) continue;
            log_error("select() failed: %s", strerror(errno));
            break;
        }

        if (sel > 0 && FD_ISSET(sock, &rfds)) {
            if (PQconsumeInput(conn) == 0) {
                log_error("PQconsumeInput failed: %s", PQerrorMessage(conn));
                break;
            }
        }

        // Drain all buffered CopyData
        for (;;) {
            int ret = PQgetCopyData(conn, &copybuf, 1);

            if(ret == 0) {
                break; // nothing buffered
            } else if(ret == -1) {
                keep_running = 0; // end of stream
                break;
            } else if(ret == -2) {
                log_error("Error in copy stream: %s", PQerrorMessage(conn));
                keep_running = 0;
                break;
            }

            log_trace("COPY msg lead=%c len=%d", copybuf[0], ret);

            if(copybuf[0] == 'w') {
                const char *ptr = copybuf + 1;
                uint64_t msg_start_lsn = pg_getmsgint64(&ptr);
                uint64_t msg_end_lsn   = pg_getmsgint64(&ptr);
                pg_getmsgint64(&ptr); // send time

                (void)msg_start_lsn;
                current_lsn = msg_end_lsn;

                // Parse pgoutput message
                char msg_type = *ptr++;

                events_received++;

                log_trace("XLogData end=%lX/%lX pgoutput=%c",
                    (unsigned long)(msg_end_lsn >> 32),
                    (unsigned long)(msg_end_lsn & 0xFFFFFFFF),
                    msg_type);

                switch(msg_type) {
                    case PGOUTPUT_BEGIN:    parse_begin_message(&ptr);    break;
                    case PGOUTPUT_COMMIT:   parse_commit_message(&ptr);   break;
                    case PGOUTPUT_RELATION: parse_relation_message(&ptr); break;
                    case PGOUTPUT_INSERT:   parse_insert_message(&ptr);   break;
                    case PGOUTPUT_UPDATE:   parse_update_message(&ptr);   break;
                    case PGOUTPUT_DELETE:   parse_delete_message(&ptr);   break;
                    default:
                        log_trace("Unhandled message type: %c", msg_type);
                        break;
                }
            } else if(copybuf[0] == 'k') {
                const char *ptr = copybuf + 1;
                pg_getmsgint64(&ptr);  // end LSN
                pg_getmsgint64(&ptr);  // timestamp
                uint8_t reply = pg_getmsgbyte(&ptr);

                if(reply) {
                    if (send_status_update(conn, 1) != 0) {
                        keep_running = 0;
                    }
                }
            }

            PQfreemem(copybuf);
            copybuf = NULL;
        }

        // Periodic status update (every 10 seconds)
        int64_t now = time(NULL);
        if(now - last_status >= 10) {
            if (send_status_update(conn, 0) != 0) break;
            last_status = now;
        }
    }

    // NEW: best-effort final feedback so server persists confirmed_flush_lsn
    log_info("Shutting down (final feedback/flush)...");
    (void)send_status_update(conn, 0);

    // We are nonblocking; do a small flush/consume loop
    for (int i = 0; i < 5; i++) {
        (void)PQflush(conn);
        (void)PQconsumeInput(conn);
        usleep(100 * 1000); // 100ms
    }

    if(copybuf) PQfreemem(copybuf);
    return 0;
}

// ============================================================================
// MAIN
// ============================================================================

static log_file_t main_log;

int main(int argc, char **argv) {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    if(argc < 2) {
        fprintf(stderr, "Usage: %s config.json\n", argv[0]);
        return 1;
    }

    binlog_print_banner();  // Reuse the same banner function

    if(load_config(argv[1], &g_config) != 0) {
        fprintf(stderr, "Failed to load configuration\n");
        return 1;
    }

    log_set_level(parse_log_level(g_config.stdout_level));
    if (log_add_rotating_file(&main_log,
                              g_config.log_file,
                              g_config.max_file_size,
                              g_config.max_log_count,
                              parse_log_level(g_config.log_level)) != 0) {
        fprintf(stderr, "Failed to open rotating log file\n");
    }

    if(log_schema_config(&g_config) != 0) {
        log_warn("Failed to print schema configurations");
    }

    // Start all publishers
    if (g_config.publisher_manager) {
        publisher_instance_t *inst = g_config.publisher_manager->instances;
        while (inst) {
            if (inst->active) {
                if (publisher_instance_start(inst) == 0) {
                    log_info("Started publisher: %s", inst->name);
                } else {
                    log_error("Failed to start publisher: %s", inst->name);
                }
            }
            inst = inst->next;
        }
    }

    // Build connection string
    char conninfo[1024];
    snprintf(conninfo, sizeof(conninfo),
        "host=%s port=%d dbname=%s user=%s password=%s replication=database",
        g_config.host, g_config.port, g_config.dbname,
        g_config.username, g_config.password);

    PGconn *conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        log_error("Connection failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }

    const char *sv = PQparameterStatus(conn, "server_version");
    log_info("Connected to PostgreSQL %s", sv ? sv : "(unknown)");

    // Restore or determine start LSN
    uint64_t start_lsn = g_config.start_lsn;

    // If checkpoint exists, it will override start_lsn
    if(restore_position(&start_lsn) != 0) {
        if(start_lsn == 0) {
            PGresult *res = PQexec(conn, "SELECT pg_current_wal_lsn()");
            if(res && PQresultStatus(res) == PGRES_TUPLES_OK) {
                const char *lsn_str = PQgetvalue(res, 0, 0);
                uint64_t parsed = 0;
                if (parse_lsn_text(lsn_str, &parsed) == 0) {
                    start_lsn = parsed;
                    log_info("Starting from current LSN: %s", lsn_str);
                } else {
                    log_warn("Could not parse pg_current_wal_lsn(): %s", lsn_str);
                }
            } else {
                log_warn("Failed to read pg_current_wal_lsn(): %s", PQerrorMessage(conn));
            }
            if(res) PQclear(res);
        }
    }

    // NEW: clamp local start_lsn to server slot confirmed_flush_lsn to avoid gaps on restart
    uint64_t slot_lsn = 0;
    if (read_slot_confirmed_flush_lsn(conn, g_config.slot_name, &slot_lsn) == 0) {
        log_info("Slot confirmed_flush_lsn: %lX/%lX",
                 (unsigned long)(slot_lsn >> 32),
                 (unsigned long)(slot_lsn & 0xFFFFFFFF));

        if (start_lsn > slot_lsn) {
            log_warn("Local checkpoint/start_lsn is ahead of slot confirmed_flush_lsn; clamping "
                     "from %lX/%lX down to %lX/%lX to avoid skipping changes",
                     (unsigned long)(start_lsn >> 32),
                     (unsigned long)(start_lsn & 0xFFFFFFFF),
                     (unsigned long)(slot_lsn >> 32),
                     (unsigned long)(slot_lsn & 0xFFFFFFFF));
            start_lsn = slot_lsn;
        }
    } else {
        log_warn("Could not read slot confirmed_flush_lsn; continuing with start_lsn %lX/%lX",
                 (unsigned long)(start_lsn >> 32),
                 (unsigned long)(start_lsn & 0xFFFFFFFF));
    }

    current_lsn = start_lsn;

    int ret = stream_changes(conn, start_lsn);

    if(g_config.save_last_position) {
        save_position(current_lsn);
    }

    PQfinish(conn);

    // Stop and cleanup publishers
    if (g_config.publisher_manager) {
        publisher_instance_t *inst = g_config.publisher_manager->instances;
        while (inst) {
            if (inst->started) {
                publisher_instance_stop(inst);
            }
            inst = inst->next;
        }

        publisher_manager_destroy(g_config.publisher_manager);
        g_config.publisher_manager = NULL;
    }

    free_relations();

    for (int i = 0; i < g_config.schema_count; i++) {
        for (int j = 0; j < g_config.schemas[i].table_count; j++) {
            table_config_t *tbl = &g_config.schemas[i].tables[j];
            free(tbl->columns);
            if (tbl->primary_keys) {
                for (int pk = 0; pk < tbl->pk_count; pk++) free(tbl->primary_keys[pk]);
                free(tbl->primary_keys);
            }
        }
        free(g_config.schemas[i].tables);
    }
    free(g_config.schemas);

    log_info("Total events: %llu", (unsigned long long)events_received);

    log_close_file(&main_log);
    return ret;
}
