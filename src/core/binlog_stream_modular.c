// binlog_stream_modular.c
// MySQL/MariaDB binlog streamer with modular publisher plugin system
//
// Build:
//   gcc -O2 -Wall binlog_stream_modular.c publisher_loader.c logger.c -o binlog_stream 
//       -lmysqlclient -lz -luuid -ljson-c -lpthread -ldl

#include <mysql/mysql.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <zlib.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <sys/socket.h>
#include <uuid/uuid.h>
#include <json-c/json.h>
#include <stdarg.h>
#include <pthread.h>
#include <errno.h>
#include <ctype.h>

#include "logger.h"
#include "publisher_loader.h"

// Event types
#define EVT_QUERY_EVENT            2
#define EVT_ROTATE                 4
#define EVT_FORMAT_DESCRIPTION    15
#define EVT_XID                   16
#define EVT_TABLE_MAP             19
#define EVT_WRITE_ROWSv1          23
#define EVT_UPDATE_ROWSv1         24
#define EVT_DELETE_ROWSv1         25
#define EVT_WRITE_ROWSv2          30
#define EVT_UPDATE_ROWSv2         31
#define EVT_DELETE_ROWSv2         32
#define EVT_MARIA_GTID                    162
#define EVT_MARIA_WRITE_ROWS_COMPRESSED   166
#define EVT_MARIA_UPDATE_ROWS_COMPRESSED  167
#define EVT_MARIA_DELETE_ROWS_COMPRESSED  168

// Column types
#define MT_DECIMAL      0
#define MT_TINY         1
#define MT_SHORT        2
#define MT_LONG         3
#define MT_FLOAT        4
#define MT_DOUBLE       5
#define MT_NULL         6
#define MT_TIMESTAMP    7
#define MT_LONGLONG     8
#define MT_INT24        9
#define MT_DATE        10
#define MT_TIME        11
#define MT_DATETIME    12
#define MT_YEAR        13
#define MT_NEWDATE     14
#define MT_VARCHAR     15
#define MT_BIT         16
#define MT_TIMESTAMP2  17
#define MT_DATETIME2   18
#define MT_TIME2       19
#define MT_NEWDECIMAL 246
#define MT_ENUM       247
#define MT_SET        248
#define MT_TINY_BLOB  249
#define MT_MEDIUM_BLOB 250
#define MT_LONG_BLOB  251
#define MT_BLOB       252
#define MT_VAR_STRING 253
#define MT_STRING     254
#define MT_GEOMETRY   255

// Column metadata
typedef struct {
    char name[128];
    int position;
    int index;
} column_info_t;

// Table configuration
typedef struct {
    char name[128];
    //char primary_key[128];
    char **primary_keys;
    int pk_count;
    column_info_t *columns;
    int column_count;
    int capture_all_columns;
} table_config_t;

// Database configuration
typedef struct {
    char name[128];
    int capture_dml;
    int capture_ddl;
    table_config_t *tables;
    int table_count;
} database_config_t;

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
    char timezone[64];

    int server_id;
    char binlog_file[256];
    int64_t binlog_position;
    int save_last_position;
    uint64_t save_position_event_count;
    char checkpoint_file[512];

    publisher_manager_t *publisher_manager;

    database_config_t *databases;
    int database_count;

} config_t;

// ENUM string cache
typedef struct {
    int loaded;
    int count;
    char **values;
} enum_cache_t;

static volatile int keep_running = 1;
static volatile int g_socket_fd = -1;
static int has_checksum = 0;
static char current_binlog[256] = "";
static uint64_t current_position = 4;
static uint64_t events_received = 0;
static uint64_t events_since_save = 0;
static char current_txn_id[37] = "";
static int in_transaction = 0;

static config_t g_config;
static pthread_mutex_t checkpoint_mutex = PTHREAD_MUTEX_INITIALIZER;
static MYSQL *g_metadata_conn = NULL;

typedef struct {
    uint64_t table_id;
    char db[128];
    char tbl[128];
    uint32_t ncols;
    unsigned char *types;
    uint16_t *metadata;
    unsigned char *real_types;
    char **column_names;
    int column_names_fetched;
} table_map_t;

static table_map_t g_map = {0, "", "", 0, NULL, NULL, NULL, NULL, 0};
static enum_cache_t *g_enum_cache = NULL;

// ============================================================================
// BASIC UTILS
// ============================================================================

static void generate_txn_id(char *out) {
    uuid_t uuid;
    uuid_generate(uuid);
    uuid_unparse_lower(uuid, out);
}

static uint16_t le16(const unsigned char *p){
    return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}
static uint32_t le24(const unsigned char *p){
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16);
}
static uint32_t le32(const unsigned char *p){
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}
static uint64_t le48(const unsigned char *p){
    return (uint64_t)le16(p+4) << 32 | le32(p);
}
static uint64_t le64(const unsigned char *p){
    uint64_t v = 0;
    for (int i = 7; i >= 0; --i) v = (v << 8) | p[i];
    return v;
}
static int bit_get(const unsigned char *bits, int idx){
    return (bits[idx >> 3] >> (idx & 7)) & 1;
}

static uint32_t count_present_columns(const unsigned char *present, uint32_t ncols) {
    uint32_t count = 0;
    for (uint32_t i = 0; i < ncols; ++i) {
        if (bit_get(present, i)) count++;
    }
    return count;
}

static int get_mysql_socket_fd(MYSQL *mysql) {
    int *ptr = (int*)mysql;
    for(int i = 0; i < 32; i++) {
        int fd = ptr[i];
        if(fd >= 3 && fd < 1024) {
            int type;
            socklen_t len = sizeof(type);
            if(getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &len) == 0) {
                return fd;
            }
        }
    }
    return -1;
}

void signal_handler(int sig) {
    (void)sig;
    keep_running = 0;
    if(g_socket_fd >= 0) {
        shutdown(g_socket_fd, SHUT_RDWR);
    }
}

// ============================================================================
// ENUM CACHE
// ============================================================================

static void free_enum_cache(void) {
    if (!g_enum_cache) return;
    for (uint32_t i = 0; i < g_map.ncols; i++) {
        if (g_enum_cache[i].values) {
            for (int j = 0; j < g_enum_cache[i].count; j++) {
                free(g_enum_cache[i].values[j]);
            }
            free(g_enum_cache[i].values);
        }
    }
    free(g_enum_cache);
    g_enum_cache = NULL;
}

// ============================================================================
// CONFIG HELPERS
// ============================================================================

static database_config_t* find_database_config(const char *db) {
    for(int i = 0; i < g_config.database_count; i++) {
        if(strcmp(g_config.databases[i].name, db) == 0) {
            return &g_config.databases[i];
        }
    }
    return NULL;
}

static table_config_t* find_table_config(const char *db, const char *table) {
    database_config_t *db_cfg = find_database_config(db);
    if(!db_cfg) return NULL;

    for(int i = 0; i < db_cfg->table_count; i++) {
        if(strcmp(db_cfg->tables[i].name, table) == 0) {
            return &db_cfg->tables[i];
        }
    }
    return NULL;
}

static int should_capture_table(const char *db, const char *table) {
    return find_table_config(db, table) != NULL;
}

static int should_capture_dml(const char *db) {
    database_config_t *db_cfg = find_database_config(db);
    return db_cfg ? db_cfg->capture_dml : 0;
}

static int should_capture_ddl(const char *db) {
    database_config_t *db_cfg = find_database_config(db);
    return db_cfg ? db_cfg->capture_ddl : 0;
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

static int log_db_config(config_t *cfg) {
    log_info("========== Capture Configuration ==========");
    for(int i = 0; i < cfg->database_count; i++) {
        database_config_t *db = &cfg->databases[i];
        log_info("Database: %s (DML:%s, DDL:%s, tables:%d)",
                 db->name,
                 db->capture_dml ? "YES" : "NO",
                 db->capture_ddl ? "YES" : "NO",
                 db->table_count);

        for(int j = 0; j < db->table_count; j++) {
            table_config_t *tbl = &db->tables[j];
            if(tbl->capture_all_columns) {
                log_info("  -> %s.%s: ALL COLUMNS (wildcard)", db->name, tbl->name);
            } else {
                log_info("  -> %s.%s: %d specific columns",
                         db->name, tbl->name, tbl->column_count);
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
    cfg->port = 3306;
    cfg->server_id = 1;
    cfg->binlog_position = 4;
    cfg->save_position_event_count = 0;
    cfg->max_log_count = 10;
    cfg->max_file_size = 10 * 1024 * 1024;
    strcpy(cfg->checkpoint_file, "binlog_checkpoint.dat");

    FILE *fp = fopen(filename, "r");
    if(!fp) {
        fprintf(stderr, "Cannot open config file: %s\n", filename);
        return -1;
    }

    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char *content = malloc(size + 1);
    if(!content) {
        fclose(fp);
        return -1;
    }

    //fread(content, 1, size, fp);
    size_t nread = fread(content, 1, size, fp);
    if (nread != size) {
        // Handle error or EOF – depending on your needs
        // You can log it, or fail the config load.
        log_error("config: expected %zu bytes, got %zu", size, nread);
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

    json_object *logging = json_object_object_get(root, "logging");
    if(logging) {
        json_object *level = json_object_object_get(logging, "level");
        if(level) {
            cfg->log_level = json_object_get_string(level);
        }
        json_object *log_file = json_object_object_get(logging, "log_file");
        if(log_file) {
            strncpy(cfg->log_file, json_object_get_string(log_file), sizeof(cfg->log_file) - 1);
        }
        json_object *stdout_level = json_object_object_get(logging, "stdout");
        if(stdout_level) {
            cfg->stdout_level = json_object_get_string(stdout_level);
        }
        json_object *max_log_count = json_object_object_get(logging, "max_files");
        if(max_log_count) cfg->max_log_count = json_object_get_int(max_log_count);
        
        json_object *max_file_size = json_object_object_get(logging, "max_file_size");
        if(max_file_size) cfg->max_file_size = json_object_get_uint64(max_file_size);
    }

    json_object *master = json_object_object_get(root, "master_server");
    if(master) {
        json_object *host = json_object_object_get(master, "host");
        if(host) strncpy(cfg->host, json_object_get_string(host), sizeof(cfg->host) - 1);

        json_object *port = json_object_object_get(master, "port");
        if(port) cfg->port = json_object_get_int(port);

        json_object *username = json_object_object_get(master, "username");
        if(username) strncpy(cfg->username, json_object_get_string(username), sizeof(cfg->username) - 1);

        json_object *password = json_object_object_get(master, "password");
        if(password) strncpy(cfg->password, json_object_get_string(password), sizeof(cfg->password) - 1);

        json_object *timezone = json_object_object_get(master, "timezone");
        if(timezone) strncpy(cfg->timezone, json_object_get_string(timezone), sizeof(cfg->timezone) - 1);
    }

    json_object *replication = json_object_object_get(root, "replication");
    if(replication) {
        json_object *server_id = json_object_object_get(replication, "server_id");
        if(server_id) cfg->server_id = json_object_get_int(server_id);

        json_object *binlog_file = json_object_object_get(replication, "binlog_file");
        if(binlog_file) {
            const char *bf = json_object_get_string(binlog_file);
            if(strcmp(bf, "current") != 0) {
                strncpy(cfg->binlog_file, bf, sizeof(cfg->binlog_file) - 1);
            }
        }

        json_object *binlog_position = json_object_object_get(replication, "binlog_position");
        if(binlog_position) cfg->binlog_position = json_object_get_int64(binlog_position);

        json_object *save_last_position = json_object_object_get(replication, "save_last_position");
        if(save_last_position) cfg->save_last_position = json_object_get_boolean(save_last_position);

        json_object *save_event_count = json_object_object_get(replication, "save_position_event_count");
        if(save_event_count) cfg->save_position_event_count = json_object_get_int(save_event_count);
        
        json_object *checkpoint_file = json_object_object_get(replication, "checkpoint_file");
        if(checkpoint_file) strncpy(cfg->checkpoint_file, json_object_get_string(checkpoint_file), sizeof(cfg->checkpoint_file) - 1);
        
    }

    json_object *capture = json_object_object_get(root, "capture");
    if(capture) {
        json_object *databases = json_object_object_get(capture, "databases");
        if(databases && json_object_is_type(databases, json_type_array)) {
            int db_count = json_object_array_length(databases);
            cfg->databases = calloc(db_count, sizeof(database_config_t));
            cfg->database_count = 0;

            for(int i = 0; i < db_count; i++) {
                json_object *db_wrapper = json_object_array_get_idx(databases, i);
                json_object_object_foreach(db_wrapper, db_name, db_obj) {
                    database_config_t *db_cfg = &cfg->databases[cfg->database_count++];
                    strncpy(db_cfg->name, db_name, sizeof(db_cfg->name) - 1);

                    json_object *capture_dml = json_object_object_get(db_obj, "capture_dml");
                    db_cfg->capture_dml = capture_dml ? json_object_get_boolean(capture_dml) : 1;

                    json_object *capture_ddl = json_object_object_get(db_obj, "capture_ddl");
                    db_cfg->capture_ddl = capture_ddl ? json_object_get_boolean(capture_ddl) : 1;

                    json_object *tables = json_object_object_get(db_obj, "tables");
                    if(tables && json_object_is_type(tables, json_type_array)) {
                        int tbl_count = json_object_array_length(tables);
                        db_cfg->tables = calloc(tbl_count, sizeof(table_config_t));
                        db_cfg->table_count = 0;

                        for(int j = 0; j < tbl_count; j++) {
                            json_object *tbl_wrapper = json_object_array_get_idx(tables, j);
                            json_object_object_foreach(tbl_wrapper, tbl_name, tbl_obj) {
                                table_config_t *tbl_cfg = &db_cfg->tables[db_cfg->table_count++];
                                strncpy(tbl_cfg->name, tbl_name, sizeof(tbl_cfg->name) - 1);

//                                json_object *primary_key = json_object_object_get(tbl_obj, "primary_key");
//                                if(primary_key) {
//                                    strncpy(tbl_cfg->primary_key,
//                                            json_object_get_string(primary_key),
//                                            sizeof(tbl_cfg->primary_key) - 1);
//                                }
                                json_object *primary_key = json_object_object_get(tbl_obj, "primary_key");
                                if (primary_key) {
                                    if (json_object_is_type(primary_key, json_type_array)) {
                                        int pkc = json_object_array_length(primary_key);
                                        if (pkc > 0) {
                                            tbl_cfg->pk_count = pkc;
                                            tbl_cfg->primary_keys = calloc(pkc, sizeof(char*));
                                            for (int pk_i = 0; pk_i < pkc; pk_i++) {
                                                json_object *pk_item = json_object_array_get_idx(primary_key, pk_i);
                                                const char *pk_name = json_object_get_string(pk_item);
                                                if (pk_name && pk_name[0]) {
                                                    tbl_cfg->primary_keys[pk_i] = strdup(pk_name);
                                                }
                                            }
                                        }
                                    } else if (json_object_is_type(primary_key, json_type_string)) {
                                        /* backward compatibility if someone uses "primary_key": "id" */
                                        const char *pk_name = json_object_get_string(primary_key);
                                        if (pk_name && pk_name[0]) {
                                            tbl_cfg->pk_count = 1;
                                            tbl_cfg->primary_keys = calloc(1, sizeof(char*));
                                            tbl_cfg->primary_keys[0] = strdup(pk_name);
                                        }
                                    }
                                }


                                json_object *columns = json_object_object_get(tbl_obj, "columns");
                                if(columns && json_object_is_type(columns, json_type_array)) {
                                    int col_count = json_object_array_length(columns);

                                    if(col_count == 1) {
                                        json_object *first_col = json_object_array_get_idx(columns, 0);
                                        const char *col_str = json_object_get_string(first_col);
                                        if(strcmp(col_str, "*") == 0) {
                                            tbl_cfg->capture_all_columns = 1;
                                            tbl_cfg->column_count = 0;
                                            tbl_cfg->columns = NULL;
                                            continue;
                                        }
                                    }

                                    tbl_cfg->capture_all_columns = 0;
                                    tbl_cfg->columns = calloc(col_count, sizeof(column_info_t));
                                    tbl_cfg->column_count = col_count;

                                    for(int k = 0; k < col_count; k++) {
                                        json_object *col = json_object_array_get_idx(columns, k);
                                        const char *col_name = json_object_get_string(col);
                                        strncpy(tbl_cfg->columns[k].name, col_name,
                                                sizeof(tbl_cfg->columns[k].name) - 1);
                                        tbl_cfg->columns[k].position = -1;
                                        tbl_cfg->columns[k].index = -1;
                                    }
                                }
                            }
                        }
                    }

                    log_info("Configured database '%s' (DML:%d, DDL:%d, tables:%d)",
                             db_name, db_cfg->capture_dml, db_cfg->capture_ddl, db_cfg->table_count);
                }
            }
        }
    }

    // ========================================================================
    // LOAD PUBLISHER PLUGINS
    // ========================================================================
    
    // Initialize publisher manager
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
            
            // Look for "plugin" publisher type
            json_object *plugin_obj = json_object_object_get(pub_obj, "plugin");
            if (plugin_obj) {
                // Parse plugin configuration
                json_object *name_obj = json_object_object_get(plugin_obj, "name");
                json_object *lib_obj = json_object_object_get(plugin_obj, "library_path");
                json_object *active_obj = json_object_object_get(plugin_obj, "active");
                json_object *max_queu_obj = json_object_object_get(plugin_obj, "max_queu_depth");
                
                if (!name_obj || !lib_obj) {
                    log_warn("Plugin missing required fields (name, library_path)");
                    continue;
                }
                
                const char *name = json_object_get_string(name_obj);
                const char *lib_path = json_object_get_string(lib_obj);
                int active = active_obj ? json_object_get_boolean(active_obj) : 1;
                uint64_t qdepth = max_queu_obj ? json_object_get_uint64(max_queu_obj) : 1024;
                // Build publisher_config_t
                publisher_config_t config = {0};
                config.name = name;
                config.active = active;
                config.max_q_depth = qdepth;
                // Parse database filter
                json_object *pub_dbs = json_object_object_get(plugin_obj, "publish_databases");
                if (pub_dbs && json_object_is_type(pub_dbs, json_type_array)) {
                    config.db_count = json_object_array_length(pub_dbs);
                    config.databases = calloc(config.db_count, sizeof(char*));
                    
                    for (int j = 0; j < config.db_count; j++) {
                        json_object *db = json_object_array_get_idx(pub_dbs, j);
                        config.databases[j] = (char*)json_object_get_string(db);
                    }
                }
                
                // Parse custom config key-value pairs
                json_object *config_obj = json_object_object_get(plugin_obj, "config");
                if (config_obj && json_object_is_type(config_obj, json_type_object)) {
                    json_object_object_foreach(config_obj, key, val) {
                        (void) key;
                        config.config_count++;
                    }
                    
                    if (config.config_count > 0) {
                        config.config_keys = calloc(config.config_count, sizeof(char*));
                        config.config_values = calloc(config.config_count, sizeof(char*));
                        
                        int idx = 0;
                        json_object_object_foreach(config_obj, key, val) {
                            config.config_keys[idx] = (char*)key;
                            config.config_values[idx] = (char*)json_object_get_string(val);
                            idx++;
                        }
                    }
                }
                
                // Load plugin
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
                
                // Cleanup temporary arrays (strings are owned by JSON)
                free(config.databases);
                free(config.config_keys);
                free(config.config_values);
            }
        }
    }

    json_object_put(root);

    log_info("Configuration loaded successfully");
    log_info("Master: %s:%d", cfg->host, cfg->port);
    log_info("Server ID: %d", cfg->server_id);
    log_info("Binlog: %s @ %lld",
             cfg->binlog_file[0] ? cfg->binlog_file : "current",
             (long long)cfg->binlog_position);
    log_info("Save position every: %d events", cfg->save_position_event_count);

    return 0;
}

// ============================================================================
// POSITION PERSISTENCE
// ============================================================================

static void save_position(const char *binlog_file, uint64_t position) {
    if(!g_config.save_last_position) return;

    pthread_mutex_lock(&checkpoint_mutex);

    FILE *fp = fopen(g_config.checkpoint_file, "w");
    if(!fp) {
        log_warn("Cannot save checkpoint to %s", g_config.checkpoint_file);
        pthread_mutex_unlock(&checkpoint_mutex);
        return;
    }

    fprintf(fp, "%s\n%llu\n", binlog_file, (unsigned long long)position);
    fclose(fp);

    log_info("Checkpoint saved: %s @ %llu", binlog_file, (unsigned long long)position);

    pthread_mutex_unlock(&checkpoint_mutex);
}

static int restore_position(char *binlog_file, uint64_t *position) {
    if(!g_config.save_last_position) return -1;

    FILE *fp = fopen(g_config.checkpoint_file, "r");
    if(!fp) return -1;

    if(fscanf(fp, "%255s\n%llu\n", binlog_file, (unsigned long long *)position) != 2) {
        fclose(fp);
        return -1;
    }

    fclose(fp);
    log_info("Restored checkpoint: %s @ %llu", binlog_file, (unsigned long long)*position);
    return 0;
}

// ============================================================================
// MYSQL / BINLOG HELPERS
// ============================================================================

static void detect_checksum(MYSQL *mysql){
    has_checksum = 0;
    if(mysql_query(mysql, "SHOW GLOBAL VARIABLES LIKE 'binlog_checksum'") != 0)
        return;
    MYSQL_RES *r = mysql_store_result(mysql);
    if(!r) return;
    MYSQL_ROW row;
    while((row = mysql_fetch_row(r))){
        if(row[1] && strcasecmp(row[1], "NONE") != 0) has_checksum = 1;
    }
    mysql_free_result(r);
}

/* Return 1 if the server is MariaDB, 0 otherwise (assume MySQL if unsure) */
static int server_is_mariadb(MYSQL *mysql)
{
    if (!mysql) return 0;

    /* VERSION() contains "MariaDB" on MariaDB servers */
    if (mysql_query(mysql, "SELECT VERSION()") != 0) {
        return 0; /* on any failure, just assume not MariaDB */
    }

    MYSQL_RES *res = mysql_store_result(mysql);
    if (!res) {
        return 0;
    }

    MYSQL_ROW row = mysql_fetch_row(res);
    int is_maria = 0;

    if (row && row[0] && strstr(row[0], "MariaDB") != NULL) {
        is_maria = 1;
    }

    mysql_free_result(res);
    return is_maria;
}


static void announce_checksum(MYSQL *mysql)
{
    if (!mysql) return;

    /*
     * These work on both MySQL and MariaDB – if binlog_checksum doesn't exist
     * (very old versions), the statements will just fail and we ignore it.
     */
    (void)mysql_query(mysql,
        "SET @source_binlog_checksum = @@GLOBAL.binlog_checksum");
    (void)mysql_query(mysql,
        "SET @master_binlog_checksum = @@GLOBAL.binlog_checksum");

    /*
     * MariaDB-specific capability flag – MUST NOT be sent to MySQL,
     * so we guard it with server_is_mariadb().
     *
     * 4 == supports annotated GTID events & checksums
     */
    if (server_is_mariadb(mysql)) {
        (void)mysql_query(mysql, "SET @mariadb_slave_capability = 3");
    }
}



//static void announce_checksum(MYSQL *mysql){
//    mysql_query(mysql, "SET @source_binlog_checksum = @@GLOBAL.binlog_checksum");
//    mysql_query(mysql, "SET @master_binlog_checksum = @@GLOBAL.binlog_checksum");
//    mysql_query(mysql, "SET @mariadb_slave_capability = 4");
//}

static int get_master_position(MYSQL *mysql, char *file_out, size_t file_size,
                               uint64_t *pos_out)
{
    if(mysql_query(mysql, "SHOW MASTER STATUS") != 0){
        
        return -1;
    }
    MYSQL_RES *res = mysql_store_result(mysql);
    if(!res) return -1;
    MYSQL_ROW row = mysql_fetch_row(res);
    if(!row || !row[0] || !row[1]){
        mysql_free_result(res);
        return -1;
    }
    strncpy(file_out, row[0], file_size - 1);
    file_out[file_size - 1] = 0;
    *pos_out = strtoull(row[1], NULL, 10);
    mysql_free_result(res);
    return 0;
}

static int mariadb_decompress_rows(const unsigned char *in, size_t in_len,
                                   unsigned char **out, size_t *out_len)
{
    if(in_len < 2) return -1;
    uint8_t header = in[0];
    unsigned header_size = header & 0x07;
    unsigned algo = (header & 0x70) >> 4;
    if(header_size == 0 || 1 + header_size > in_len || algo != 0) return -1;

    size_t expected = 0;
    for(unsigned i = 0; i < header_size; ++i){
        expected |= (size_t)in[1 + i] << (8 * i);
    }

    *out = (unsigned char*)malloc(expected ? expected : 1);
    if(!*out) return -1;

    uLongf dest_len = (uLongf)expected;
    if(uncompress(*out, &dest_len, in + 1 + header_size,
                  (uLongf)(in_len - 1 - header_size)) != Z_OK){
        free(*out);
        *out = NULL;
        return -1;
    }
    *out_len = (size_t)dest_len;
    return 0;
}

// ============================================================================
// ENUM SUPPORT
// ============================================================================

static int load_enum_values_for_column(const char *db,
                                       const char *tbl,
                                       const char *col_name,
                                       enum_cache_t *cache)
{
    if (!g_metadata_conn || !db || !tbl || !col_name || !cache) return -1;
    if (cache->loaded) return 0;

    char query[512];
    snprintf(query, sizeof(query),
             "SELECT COLUMN_TYPE "
             "FROM INFORMATION_SCHEMA.COLUMNS "
             "WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s'",
             db, tbl, col_name);

    if (mysql_query(g_metadata_conn, query) != 0) {
        log_warn("Failed to get ENUM definition for %s.%s.%s: %s",
                 db, tbl, col_name, mysql_error(g_metadata_conn));
        return -1;
    }

    MYSQL_RES *res = mysql_store_result(g_metadata_conn);
    if (!res) {
        log_warn("No result for ENUM definition query: %s", mysql_error(g_metadata_conn));
        return -1;
    }

    MYSQL_ROW row = mysql_fetch_row(res);
    if (!row || !row[0]) {
        mysql_free_result(res);
        return -1;
    }

    const char *col_type = row[0];
    const char *p = strchr(col_type, '(');
    const char *end = strrchr(col_type, ')');

    if (!p || !end || end <= p + 1) {
        mysql_free_result(res);
        return -1;
    }

    p++;

    int count = 0;
    const char *q = p;
    while (q < end) {
        const char *s = strchr(q, '\'');
        if (!s || s >= end) break;
        const char *e = strchr(s + 1, '\'');
        if (!e || e >= end) break;
        count++;
        q = e + 1;
    }

    if (count == 0) {
        mysql_free_result(res);
        return -1;
    }

    cache->values = (char **)calloc(count, sizeof(char *));
    cache->count = 0;

    q = p;
    while (q < end && cache->count < count) {
        const char *s = strchr(q, '\'');
        if (!s || s >= end) break;
        const char *e = strchr(s + 1, '\'');
        if (!e || e >= end) break;

        size_t len = (size_t)(e - (s + 1));
        char *val = (char *)malloc(len + 1);
        memcpy(val, s + 1, len);
        val[len] = '\0';

        cache->values[cache->count++] = val;
        q = e + 1;
    }

    cache->loaded = 1;
    log_trace("Loaded %d ENUM values for %s.%s.%s",
              cache->count, db, tbl, col_name);

    mysql_free_result(res);
    return 0;
}

// ============================================================================
// COLUMN NAME FETCH
// ============================================================================

static void fetch_column_names(const char *db, const char *tbl) {
    if(!db || !tbl) return;

    if(g_map.column_names_fetched &&
       strcmp(g_map.db, db) == 0 &&
       strcmp(g_map.tbl, tbl) == 0) {
        log_debug("Using cached column names for %s.%s", db, tbl);
        return;
    }

    if(g_map.column_names) {
        for(uint32_t i = 0; i < g_map.ncols; i++) {
            free(g_map.column_names[i]);
        }
        free(g_map.column_names);
        g_map.column_names = NULL;
        g_map.column_names_fetched = 0;
    }

    if(!g_metadata_conn) {
        log_warn("No metadata connection available, cannot fetch column names for %s.%s", db, tbl);
        return;
    }

    char query[512];
    snprintf(query, sizeof(query), "SELECT * FROM `%s`.`%s` LIMIT 0", db, tbl);

    if(mysql_query(g_metadata_conn, query) != 0) {
        log_warn("Cannot get column names for %s.%s: %s", db, tbl, mysql_error(g_metadata_conn));
        return;
    }

    MYSQL_RES *res = mysql_store_result(g_metadata_conn);
    if(!res) {
        log_warn("No result for column names query: %s", mysql_error(g_metadata_conn));
        return;
    }

    int num_fields = mysql_num_fields(res);
    g_map.column_names = calloc(num_fields, sizeof(char*));

    if(g_map.column_names) {
        MYSQL_FIELD *fields = mysql_fetch_fields(res);
        for(int i = 0; i < num_fields; i++) {
            g_map.column_names[i] = strdup(fields[i].name);
        }
        g_map.column_names_fetched = 1;
        log_trace("Fetched %d column names for %s.%s", num_fields, db, tbl);
    }

    mysql_free_result(res);
}

// Due to length constraints, I'll continue in the next part...
// [Continuing binlog_stream_modular.c - Part 2]

// ============================================================================
// TABLE_MAP PARSER
// ============================================================================

static void parse_table_map(const unsigned char *p, uint32_t len){
    if(len < 8) return;

    uint64_t tid = le48(p);  p += 6;
    p += 2;

    unsigned sch_len = *p++;
    char new_db[128] = "";
    if(sch_len >= sizeof(new_db)) sch_len = sizeof(new_db) - 1;
    memcpy(new_db, p, sch_len);
    new_db[sch_len] = 0;
    p += sch_len + 1;

    unsigned tbl_len = *p++;
    char new_tbl[128] = "";
    if(tbl_len >= sizeof(new_tbl)) tbl_len = sizeof(new_tbl) - 1;
    memcpy(new_tbl, p, tbl_len);
    new_tbl[tbl_len] = 0;
    p += tbl_len + 1;

    uint64_t ncols64 = *p++;
    uint32_t ncols = (uint32_t)ncols64;
    
    g_map.table_id = tid;
    //strncpy(g_map.db, new_db, sizeof(g_map.db) - 1);
    //strncpy(g_map.tbl, new_tbl, sizeof(g_map.tbl) - 1);
    snprintf(g_map.db,  sizeof(g_map.db),  "%s", new_db);
    snprintf(g_map.tbl, sizeof(g_map.tbl), "%s", new_tbl);

    if(!should_capture_table(new_db, new_tbl)) {
        log_debug("TABLE_MAP tid=%llu db='%s' table='%s' - IGNORED (not in capture list)",
                  (unsigned long long)tid, new_db, new_tbl);
        g_map.table_id = 0;
        return;
    }

    if(!should_capture_dml(new_db)) {
        log_debug("TABLE_MAP tid=%llu db='%s' table='%s' - IGNORED (DML capture disabled)",
                  (unsigned long long)tid, new_db, new_tbl);
        g_map.table_id = 0;
        return;
    }

    int table_changed = (strcmp(g_map.db, new_db) != 0 ||
                         strcmp(g_map.tbl, new_tbl) != 0);
    uint32_t old_ncols = g_map.ncols;
    int ncols_changed = (old_ncols != ncols);

    if(!in_transaction) {
        generate_txn_id(current_txn_id);
        in_transaction = 1;
    }

    free(g_map.types);
    free(g_map.metadata);
    free(g_map.real_types);
    g_map.types = NULL;
    g_map.metadata = NULL;
    g_map.real_types = NULL;

//    g_map.table_id = tid;
//    strncpy(g_map.db, new_db, sizeof(g_map.db) - 1);
//    strncpy(g_map.tbl, new_tbl, sizeof(g_map.tbl) - 1);
    g_map.ncols = ncols;

    if(table_changed || ncols_changed || g_enum_cache == NULL) {
        free_enum_cache();
        if (g_map.ncols > 0) {
            g_enum_cache = (enum_cache_t *)calloc(g_map.ncols, sizeof(enum_cache_t));
        }
    }

    g_map.types = (unsigned char*)malloc(ncols);
    g_map.metadata = (uint16_t*)calloc(ncols, sizeof(uint16_t));
    g_map.real_types = (unsigned char*)malloc(ncols);

    if(!g_map.types || !g_map.metadata || !g_map.real_types) return;

    memcpy(g_map.types, p, ncols);
    memcpy(g_map.real_types, p, ncols);
    p += ncols;

    uint64_t meta_len = 0;
    if(*p < 251) {
        meta_len = *p++;
    } else if(*p == 252) {
        p++;
        meta_len = le16(p);
        p += 2;
    } else if(*p == 253) {
        p++;
        meta_len = le32(p) & 0xFFFFFF;
        p += 3;
    }

    const unsigned char *meta_start = p;

    for(uint32_t i = 0; i < ncols && (size_t)(p - meta_start) < meta_len; ++i){
        uint8_t t = g_map.types[i];

        switch(t){
            case MT_FLOAT:
            case MT_DOUBLE:
            case MT_TIMESTAMP2:
            case MT_DATETIME2:
            case MT_TIME2:
            case MT_BLOB:
            case MT_GEOMETRY:
                if(p < meta_start + meta_len){
                    g_map.metadata[i] = *p++;
                }
                break;

            case MT_BIT:
            case MT_VARCHAR:
            case MT_NEWDECIMAL:
            case MT_SET:
            case MT_ENUM:
                if(p + 1 < meta_start + meta_len){
                    g_map.metadata[i] = le16(p);
                    p += 2;
                }
                break;

            case MT_STRING:
                if(p + 1 < meta_start + meta_len){
                    uint16_t meta = le16(p);
                    g_map.metadata[i] = meta;
                    p += 2;

                    uint8_t real_type = meta & 0xFF;
                    if(real_type == MT_ENUM || real_type == MT_SET){
                        g_map.real_types[i] = real_type;
                    }
                }
                break;

            default:
                g_map.metadata[i] = 0;
                break;
        }
    }

    fetch_column_names(g_map.db, g_map.tbl);

    table_config_t *tbl_cfg = find_table_config(g_map.db, g_map.tbl);
    if(tbl_cfg && g_map.column_names) {
        if(tbl_cfg->capture_all_columns) {
            tbl_cfg->column_count = g_map.ncols;
            tbl_cfg->columns = calloc(g_map.ncols, sizeof(column_info_t));
            for(uint32_t i = 0; i < g_map.ncols; i++) {
                if(g_map.column_names[i]) {
                    strncpy(tbl_cfg->columns[i].name, g_map.column_names[i],
                            sizeof(tbl_cfg->columns[i].name) - 1);
                    tbl_cfg->columns[i].index = i;
                    tbl_cfg->columns[i].position = i;
                }
            }
        } else {
            for(int i = 0; i < tbl_cfg->column_count; i++) {
                tbl_cfg->columns[i].index = -1;
                for(uint32_t j = 0; j < g_map.ncols; j++) {
                    if(g_map.column_names[j] &&
                       strcmp(tbl_cfg->columns[i].name, g_map.column_names[j]) == 0) {
                        tbl_cfg->columns[i].index = j;
                        log_trace("Mapped column %s to index %d", tbl_cfg->columns[i].name, j);
                        break;
                    }
                }
                if(tbl_cfg->columns[i].index == -1) {
                    log_warn("Column %s not found in table %s.%s",
                             tbl_cfg->columns[i].name, g_map.db, g_map.tbl);
                }
            }
        }
    }

    log_debug("[txn:%s] TABLE_MAP tid=%llu db='%s' table='%s' ncols=%u",
             current_txn_id, (unsigned long long)tid, g_map.db, g_map.tbl, g_map.ncols);
}

// ============================================================================
// COLUMN VALUE PARSER (simplified - full implementation in original file)
// ============================================================================

static const unsigned char* append_column_value_to_json(
    char *json_buf, size_t buf_size, size_t *offset,
    const unsigned char *p,
    uint32_t col_idx,
    int is_null,
    const char *col_name)
{
    if(is_null){
        *offset += snprintf(json_buf + *offset, buf_size - *offset,
                            "\"%s\":null", col_name);
        return p;
    }

//    unsigned char type = g_map.types[col_idx];
    unsigned char real_type = g_map.real_types[col_idx];
    uint16_t meta = g_map.metadata[col_idx];

    *offset += snprintf(json_buf + *offset, buf_size - *offset, "\"%s\":", col_name);

    switch(real_type){
        case MT_TINY:
            *offset += snprintf(json_buf + *offset, buf_size - *offset,
                                "%d", (int8_t)*p);
            return p + 1;
        case MT_SHORT:
        case MT_YEAR:
            *offset += snprintf(json_buf + *offset, buf_size - *offset,
                                "%d", (int16_t)le16(p));
            return p + 2;
        case MT_INT24: {
            int32_t v = le24(p);
            if(v & 0x800000) v |= ~0xFFFFFF;
            *offset += snprintf(json_buf + *offset, buf_size - *offset, "%d", v);
            return p + 3;
        }
        case MT_LONG:
            *offset += snprintf(json_buf + *offset, buf_size - *offset,
                                "%u", le32(p));
            return p + 4;
        case MT_LONGLONG:
            *offset += snprintf(json_buf + *offset, buf_size - *offset,
                                "%llu", (unsigned long long)le64(p));
            return p + 8;
        case MT_FLOAT: {
            float f;
            memcpy(&f, p, 4);
            *offset += snprintf(json_buf + *offset, buf_size - *offset, "%f", f);
            return p + 4;
        }
        case MT_DOUBLE: {
            double d;
            memcpy(&d, p, 8);
            *offset += snprintf(json_buf + *offset, buf_size - *offset, "%f", d);
            return p + 8;
        }
        case MT_TIMESTAMP:
            *offset += snprintf(json_buf + *offset, buf_size - *offset,
                                "%u", le32(p));
            return p + 4;
        case MT_TIMESTAMP2: {
            uint32_t sec = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                           ((uint32_t)p[2] << 8) | (uint32_t)p[3];
            p += 4;
            if(meta > 0) {
                int frac_bytes = (meta + 1) / 2;
                uint32_t frac = 0;
                for(int i = 0; i < frac_bytes; i++)
                    frac = (frac << 8) | *p++;
                time_t t = (time_t)sec;
                struct tm *tm = localtime(&t);
                if(tm){
                    *offset += snprintf(json_buf + *offset, buf_size - *offset,
                           "\"%04d-%02d-%02d %02d:%02d:%02d.%0*u\"",
                           tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
                           tm->tm_hour, tm->tm_min, tm->tm_sec, meta, frac);
                } else {
                    *offset += snprintf(json_buf + *offset, buf_size - *offset,
                                        "\"%u.%0*u\"", sec, meta, frac);
                }
            } else {
                time_t t = (time_t)sec;
                struct tm *tm = localtime(&t);
                if(tm){
                    *offset += snprintf(json_buf + *offset, buf_size - *offset,
                           "\"%04d-%02d-%02d %02d:%02d:%02d\"",
                           tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
                           tm->tm_hour, tm->tm_min, tm->tm_sec);
                } else {
                    *offset += snprintf(json_buf + *offset, buf_size - *offset,
                                        "\"%u\"", sec);
                }
            }
            return p;
        }
        case MT_DATETIME2: {
            uint64_t val = 0;
            for(int i = 0; i < 5; i++){
                val = (val << 8) | *p++;
            }
            val -= 0x8000000000LL;
            int ymd = val >> 17;
            int ym = ymd >> 5;
            int hms = val & 0x1FFFF;
            int year = ym / 13;
            int mon = ym % 13;
            int day = ymd & 0x1F;
            int hour = hms >> 12;
            int min = (hms >> 6) & 0x3F;
            int sec = hms & 0x3F;
            *offset += snprintf(json_buf + *offset, buf_size - *offset,
                   "\"%04d-%02d-%02d %02d:%02d:%02d\"",
                   year, mon, day, hour, min, sec);
            if(meta > 0) {
                int frac_bytes = (meta + 1) / 2;
                p += frac_bytes;
            }
            return p;
        }
        case MT_VARCHAR: {
            unsigned len;
            if(meta < 256){
                len = *p++;
            } else {
                len = le16(p);
                p += 2;
            }
            *offset += snprintf(json_buf + *offset, buf_size - *offset, "\"");
            for(unsigned i = 0; i < len && *offset < buf_size - 10; i++) {
                char c = p[i];
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
            return p + len;
        }
        case MT_BLOB: {
            uint32_t len = 0;
            for(unsigned i = 0; i < meta; i++){
                len |= (uint32_t)p[i] << (8 * i);
            }
            p += meta;
            unsigned display_len = len > 200 ? 200 : len;
            *offset += snprintf(json_buf + *offset, buf_size - *offset, "\"");
            for(unsigned i = 0; i < display_len && *offset < buf_size - 10; i++) {
                char c = p[i];
                if(c == '"') *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\\"");
                else if(c == '\\') *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\\\");
                else if(c < 32 || c > 126)
                    *offset += snprintf(json_buf + *offset, buf_size - *offset, ".");
                else json_buf[(*offset)++] = c;
            }
            if(len > 200) {
                *offset += snprintf(json_buf + *offset, buf_size - *offset, "...");
            }
            *offset += snprintf(json_buf + *offset, buf_size - *offset, "\"");
            return p + len;
        }
        case MT_ENUM: {
            uint16_t enum_val;
            uint8_t pack_len = (meta >> 8) & 0xFF;
            if (pack_len == 1) {
                enum_val = *p;
                p += 1;
            } else {
                enum_val = le16(p);
                p += 2;
            }

            const char *enum_str = NULL;
            enum_cache_t *cache = NULL;

            if (g_enum_cache && col_idx < g_map.ncols) {
                cache = &g_enum_cache[col_idx];
                if (!cache->loaded && g_map.column_names &&
                    g_map.column_names[col_idx]) {
                    load_enum_values_for_column(g_map.db,
                                                g_map.tbl,
                                                g_map.column_names[col_idx],
                                                cache);
                }
                if (cache->loaded &&
                    enum_val >= 1 &&
                    enum_val <= (uint16_t)cache->count) {
                    enum_str = cache->values[enum_val - 1];
                }
            }

            if (enum_str) {
                *offset += snprintf(json_buf + *offset, buf_size - *offset, "\"");
                for (const char *s = enum_str; *s && *offset < buf_size - 10; s++) {
                    char c = *s;
                    if (c == '"') *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\\"");
                    else if (c == '\\') *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\\\");
                    else if (c == '\n') *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\n");
                    else if (c == '\r') *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\r");
                    else if (c == '\t') *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\t");
                    else if ((unsigned char)c < 32)
                        *offset += snprintf(json_buf + *offset, buf_size - *offset, "\\u%04x", (unsigned char)c);
                    else json_buf[(*offset)++] = c;
                }
                *offset += snprintf(json_buf + *offset, buf_size - *offset, "\"");
            } else {
                *offset += snprintf(json_buf + *offset, buf_size - *offset, "%u", enum_val);
            }
            return p;
        }
        case MT_STRING: {
            unsigned len;
            uint16_t byte0 = meta >> 8;
            if(byte0 == 0){
                len = *p++;
            } else {
                len = le16(p);
                p += 2;
            }
            *offset += snprintf(json_buf + *offset, buf_size - *offset, "\"");
            for(unsigned i = 0; i < len && *offset < buf_size - 10; i++) {
                char c = p[i];
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
            return p + len;
        }
        default:
            *offset += snprintf(json_buf + *offset, buf_size - *offset, "null");
            return p;
    }
}

static const unsigned char* skip_column_value(const unsigned char *p, uint32_t col_idx) {
    unsigned char real_type = g_map.real_types[col_idx];
    uint16_t meta = g_map.metadata[col_idx];

    switch(real_type){
        case MT_TINY:      return p + 1;
        case MT_SHORT:
        case MT_YEAR:      return p + 2;
        case MT_INT24:     return p + 3;
        case MT_LONG:
        case MT_FLOAT:
        case MT_TIMESTAMP: return p + 4;
        case MT_LONGLONG:
        case MT_DOUBLE:    return p + 8;
        case MT_TIMESTAMP2:
            p += 4;
            if(meta > 0) p += (meta + 1) / 2;
            return p;
        case MT_DATETIME2:
            p += 5;
            if(meta > 0) p += (meta + 1) / 2;
            return p;
        case MT_VARCHAR: {
            unsigned len;
            if(meta < 256){ len = *p++; }
            else { len = le16(p); p += 2; }
            return p + len;
        }
        case MT_BLOB: {
            uint32_t len = 0;
            for(unsigned i = 0; i < meta; i++)
                len |= (uint32_t)p[i] << (8 * i);
            p += meta;
            return p + len;
        }
        case MT_ENUM: {
            uint8_t pack_len = (meta >> 8) & 0xFF;
            return p + (pack_len == 1 ? 1 : 2);
        }
        case MT_STRING: {
            unsigned len;
            uint16_t byte0 = meta >> 8;
            if(byte0 == 0){ len = *p++; }
            else { len = le16(p); p += 2; }
            return p + len;
        }
        default:
            return p;
    }
}

static int parse_row_to_json_filtered(const unsigned char **p_ptr, size_t *len_ptr,
                                      uint32_t ncols, const unsigned char *present,
                                      char *json_buf, size_t buf_size, size_t *json_offset)
{
//    const unsigned char *p = *p_ptr;
//    size_t len = *len_ptr;
//    const unsigned char *start_p = p;
//
//    uint32_t bmp_len = (ncols + 7) >> 3;
//    if(len < bmp_len) return -1;
//
//    const unsigned char *nullmap = p;
//    p += bmp_len;
//
//    *json_offset += snprintf(json_buf + *json_offset, buf_size - *json_offset, "{");
//
//    int first = 1;
//    int seen = 0;
//
//    table_config_t *tbl_cfg = find_table_config(g_map.db, g_map.tbl);
//
//    for(uint32_t i = 0; i < ncols; ++i){
//        if(!bit_get(present, i)) continue;
//
//        int is_null = bit_get(nullmap, seen++);
    const unsigned char *p   = *p_ptr;
    size_t len               = *len_ptr;
    const unsigned char *start_p = p;

    /* Number of columns actually present in this row (bitmask) */
    uint32_t present_count = count_present_columns(present, ncols);
    uint32_t bmp_len       = (present_count + 7) >> 3;

    if (len < bmp_len) return -1;

    const unsigned char *nullmap = p;
    p += bmp_len;

    *json_offset += snprintf(json_buf + *json_offset, buf_size - *json_offset, "{");

    int first = 1;
    int seen  = 0;

    table_config_t *tbl_cfg = find_table_config(g_map.db, g_map.tbl);

    for (uint32_t i = 0; i < ncols; ++i) {
        if (!bit_get(present, i)) continue;

        int is_null = bit_get(nullmap, seen++);

        int should_include = 0;
        const char *col_name = NULL;

        if(tbl_cfg) {
            if(tbl_cfg->capture_all_columns) {
                should_include = 1;
                col_name = (i < g_map.ncols && g_map.column_names && g_map.column_names[i])
                           ? g_map.column_names[i]
                           : "unknown";
            } else {
                for(int j = 0; j < tbl_cfg->column_count; j++) {
                    if(tbl_cfg->columns[j].index == (int)i) {
                        should_include = 1;
                        col_name = tbl_cfg->columns[j].name;
                        break;
                    }
                }
            }
        }

        if(!should_include) {
            if(!is_null) p = skip_column_value(p, i);
            continue;
        }

        if(!first) {
            *json_offset += snprintf(json_buf + *json_offset, buf_size - *json_offset, ",");
        }
        first = 0;

        const unsigned char *old_p = p;
        p = append_column_value_to_json(json_buf, buf_size, json_offset, p, i, is_null, col_name);
        if(p == old_p && !is_null) return -1;

        size_t consumed = p - start_p;
        if(consumed > len) return -1;
    }

    *json_offset += snprintf(json_buf + *json_offset, buf_size - *json_offset, "}");

    size_t consumed = p - start_p;
    *p_ptr = p;
    *len_ptr = len - consumed;
    return 0;
}

// ============================================================================
// QUERY EVENT PARSER
// ============================================================================

static void parse_query(const unsigned char *p, uint32_t len){
if(len < 13) return;

    const unsigned char *start = p;
    p += 4;
    p += 4;
    uint8_t db_len = *p++;
    p += 2;
    uint16_t status_vars_len = le16(p); p += 2;

    if(status_vars_len > 0) {
        size_t offset = p - start;
        if(offset + status_vars_len <= len) {
            p += status_vars_len;
        } else {
            return;
        }
    }

    char db[256] = "";
    if(db_len > 0) {
        size_t offset = p - start;
        if(db_len < sizeof(db) && offset + db_len + 1 <= len) {
            memcpy(db, p, db_len);
            db[db_len] = 0;
            p += db_len;
            p++;
        } else {
            return;
        }
    } else {
        size_t offset = p - start;
        if(offset < len && *p == 0) {
            p++;
        }
    }

    size_t offset = p - start;
    if(offset >= len) return;

    size_t query_len = len - offset;
    const char *query = (const char*)p;

    while(query_len > 0 && query[query_len - 1] == 0) {
        query_len--;
    }

    if(query_len == 0) return;

    const char *type = "QUERY";
    int is_begin = 0;
    int is_commit = 0;
    int is_rollback = 0;
    int is_ddl = 0;

    if(strncasecmp(query, "BEGIN", 5) == 0) {
        type = "BEGIN"; is_begin = 1;
    } else if(strncasecmp(query, "COMMIT", 6) == 0) {
        type = "COMMIT"; is_commit = 1;
    } else if(strncasecmp(query, "ROLLBACK", 8) == 0) {
        type = "ROLLBACK"; is_rollback = 1;
    } else if(strncasecmp(query, "CREATE", 6) == 0) {
        type = "CREATE"; is_ddl = 1;
    } else if(strncasecmp(query, "ALTER", 5) == 0) {
        type = "ALTER"; is_ddl = 1;
    } else if(strncasecmp(query, "DROP", 4) == 0) {
        type = "DROP"; is_ddl = 1;
    } else if(strncasecmp(query, "TRUNCATE", 8) == 0) {
        type = "TRUNCATE"; is_ddl = 1;
    } else if(strncasecmp(query, "RENAME", 6) == 0) {
        type = "RENAME"; is_ddl = 1;
    }
    
    if(is_begin) {
        in_transaction = 1;
        generate_txn_id(current_txn_id);
        log_debug("[txn:%s] BEGIN transaction", current_txn_id);
    } else if(is_ddl && !in_transaction) {
        generate_txn_id(current_txn_id);
    } else if(!in_transaction) {
        generate_txn_id(current_txn_id);
    }

    if(is_ddl && db_len > 0 && !should_capture_ddl(db)) {
        log_debug("[txn:%s] DDL/DCL for database %s - IGNORED (capture disabled)",current_txn_id, db);
        return;
    }

    if(is_ddl || is_begin || is_commit || is_rollback) {
        log_debug("[txn:%s] %s%s%s: %.*s",
                 current_txn_id, type,
                 db_len > 0 ? " db=" : "",
                 db_len > 0 ? db : "",
                 (int)query_len, query);
    } else {
        log_debug("[txn:%s] %s%s%s: %.*s",
                  current_txn_id, type,
                  db_len > 0 ? " db=" : "",
                  db_len > 0 ? db : "",
                  (int)query_len, query);
    }

    if(is_ddl && db_len > 0 && should_capture_ddl(db)) {
        char event_json[4096];
        char escaped_query[2048];

        int j = 0;
        for(int i = 0; i < (int)query_len && i < 1023 && j < 2046; i++) {
            char c = query[i];
            if(c == '"') {
                escaped_query[j++] = '\\';
                escaped_query[j++] = '"';
            } else if(c == '\\') {
                escaped_query[j++] = '\\';
                escaped_query[j++] = '\\';
            } else if(c == '\n') {
                escaped_query[j++] = '\\';
                escaped_query[j++] = 'n';
            } else if(c == '\r') {
                escaped_query[j++] = '\\';
                escaped_query[j++] = 'r';
            } else if(c == '\t') {
                escaped_query[j++] = '\\';
                escaped_query[j++] = 't';
            } else {
                escaped_query[j++] = c;
            }
        }
        escaped_query[j] = '\0';

        snprintf(event_json, sizeof(event_json),
                "{\"type\":\"%s\",\"txn\":\"%s\",\"db\":\"%s\",\"query\":\"%s\"}",
                type, current_txn_id, db, escaped_query);
        // no table for DDL query event here → pass empty table
        extern void publish_event(const char *db, const char *table, const char *event_json, const char *txn);
        publish_event(db, type, event_json, current_txn_id);
    }

    if(is_commit || is_rollback) {
        log_info("[txn:%s] Transaction %s", current_txn_id,
                 is_commit ? "COMMITTED" : "ROLLED BACK");
        in_transaction = 0;
        current_txn_id[0] = '\0';
    }
}

// ============================================================================
// PUBLISH EVENT (USING PLUGIN SYSTEM)
// ============================================================================

void publish_event(const char *db, const char *table, 
                  const char *event_json, const char *txn) {
    if (!g_config.publisher_manager) return;
    
    // Build CDC event
    cdc_event_t event = {
        .db = db,
        .table = table,
        .json = event_json,
        .txn = txn,
        .position = current_position,
        .binlog_file = current_binlog
    };

    // Dispatch to matching publishers
    int dispatched = 0;
    publisher_instance_t *inst = g_config.publisher_manager->instances;
    
    while (inst) {
        if (publisher_should_publish(inst, db)) {
            if (publisher_instance_enqueue(inst, &event) == 0) {
                log_trace("Dispatching event publisher=%s txn=%s db=%s table=%s binlog_file=%s position=%d : %s", inst->name, txn, db, table, current_binlog, current_position, event_json);
                dispatched++;
            }
        } else {
            log_trace("Publisher: %s not configured to publish db:%s",inst->name, db);
        }
        inst = inst->next;
    }
    
    if (dispatched > 0) {
        log_debug("Dispatched to %d publisher(s) for db=%s table=%s",
                 dispatched, db ? db : "", table ? table : "");
    }
}

// ============================================================================
// WRITE / UPDATE / DELETE PARSERS
// ============================================================================

static void append_primary_key_metadata(char *json_buf, size_t buf_size,
                                        size_t *json_offset,
                                        const char *db, const char *tbl) {
    table_config_t *tbl_cfg = find_table_config(db, tbl);
    if (!tbl_cfg || tbl_cfg->pk_count <= 0 || !tbl_cfg->primary_keys) {
        return;
    }

    *json_offset += snprintf(json_buf + *json_offset,
                             buf_size - *json_offset,
                             ",\"primary_key\":[");
    for (int i = 0; i < tbl_cfg->pk_count; i++) {
        if (i > 0) {
            *json_offset += snprintf(json_buf + *json_offset,
                                     buf_size - *json_offset, ",");
        }
        const char *pk_name = tbl_cfg->primary_keys[i] ? tbl_cfg->primary_keys[i] : "";
        *json_offset += snprintf(json_buf + *json_offset,
                                 buf_size - *json_offset,
                                 "\"%s\"", pk_name);
    }
    *json_offset += snprintf(json_buf + *json_offset,
                             buf_size - *json_offset, "]");
}


static void parse_write_rows(const unsigned char *row_data, size_t row_len,
                            uint32_t ncols, const unsigned char *present)
{
    if(g_map.table_id == 0) return;

    char json_event[32768];
    size_t json_offset = 0;

//    json_offset += snprintf(json_event, sizeof(json_event),
//        "{\"type\":\"INSERT\",\"txn\":\"%s\",\"db\":\"%s\",\"table\":\"%s\",\"rows\":[",
//        current_txn_id, g_map.db, g_map.tbl);
    
    json_offset += snprintf(json_event, sizeof(json_event),
    "{\"type\":\"INSERT\",\"txn\":\"%s\",\"db\":\"%s\",\"table\":\"%s\"",
    current_txn_id, g_map.db, g_map.tbl);

    /* add primary_key metadata if configured */
    append_primary_key_metadata(json_event, sizeof(json_event),
                                &json_offset, g_map.db, g_map.tbl);

    /* now start rows array */
    json_offset += snprintf(json_event + json_offset,
                            sizeof(json_event) - json_offset,
                            ",\"rows\":[");
    

    int row_num = 0;
    const unsigned char *p = row_data;
    size_t len = row_len;

    uint32_t min_row_size = (ncols + 7) >> 3;

    while(len >= min_row_size && json_offset < sizeof(json_event) - 2000){
        row_num++;
        if(row_num > 1)
            json_offset += snprintf(json_event + json_offset,
                                   sizeof(json_event) - json_offset, ",");

        if(parse_row_to_json_filtered(&p, &len, ncols, present,
                                     json_event, sizeof(json_event), &json_offset) != 0) {
            break;
        }
    }

    json_offset += snprintf(json_event + json_offset,
                           sizeof(json_event) - json_offset, "]}");

    if(row_num > 0) {
        publish_event(g_map.db, g_map.tbl, json_event, current_txn_id);
        log_debug("INSERT %s.%s: %d row(s) captured", g_map.db, g_map.tbl, row_num);
    }
}

static void parse_update_rows(const unsigned char *row_data, size_t row_len,
                             uint32_t ncols,
                             const unsigned char *before_present,
                             const unsigned char *after_present)
{
    if(g_map.table_id == 0) return;

    char json_event[32768];
    size_t json_offset = 0;

//    json_offset += snprintf(json_event, sizeof(json_event),
//        "{\"type\":\"UPDATE\",\"txn\":\"%s\",\"db\":\"%s\",\"table\":\"%s\",\"rows\":[",
//        current_txn_id, g_map.db, g_map.tbl);
    json_offset += snprintf(json_event, sizeof(json_event),
    "{\"type\":\"UPDATE\",\"txn\":\"%s\",\"db\":\"%s\",\"table\":\"%s\"",
    current_txn_id, g_map.db, g_map.tbl);

    append_primary_key_metadata(json_event, sizeof(json_event),
                                &json_offset, g_map.db, g_map.tbl);

    json_offset += snprintf(json_event + json_offset,
                            sizeof(json_event) - json_offset,
                            ",\"rows\":[");


    int row_num = 0;
    const unsigned char *p = row_data;
    size_t len = row_len;

    uint32_t min_row_size = 2 * ((ncols + 7) >> 3);
    uint32_t conservative_min = min_row_size + ncols * 2;

    while(len >= conservative_min && json_offset < sizeof(json_event) - 4000){
        row_num++;
        if(row_num > 1)
            json_offset += snprintf(json_event + json_offset,
                                   sizeof(json_event) - json_offset, ",");

        json_offset += snprintf(json_event + json_offset,
                               sizeof(json_event) - json_offset, "{\"before\":");

        if(parse_row_to_json_filtered(&p, &len, ncols, before_present,
                                     json_event, sizeof(json_event), &json_offset) != 0) {
            break;
        }

        json_offset += snprintf(json_event + json_offset,
                               sizeof(json_event) - json_offset, ",\"after\":");

        if(parse_row_to_json_filtered(&p, &len, ncols, after_present,
                                     json_event, sizeof(json_event), &json_offset) != 0) {
            break;
        }

        json_offset += snprintf(json_event + json_offset,
                               sizeof(json_event) - json_offset, "}");
    }

    json_offset += snprintf(json_event + json_offset,
                           sizeof(json_event) - json_offset, "]}");

    if(row_num > 0) {
        publish_event(g_map.db, g_map.tbl, json_event, current_txn_id);
        log_debug("UPDATE %s.%s: %d row(s) captured", g_map.db, g_map.tbl, row_num);
    }
}

static void parse_delete_rows(const unsigned char *row_data, size_t row_len,
                             uint32_t ncols, const unsigned char *present)
{
    if(g_map.table_id == 0) return;

    char json_event[32768];
    size_t json_offset = 0;

//    json_offset += snprintf(json_event, sizeof(json_event),
//        "{\"type\":\"DELETE\",\"txn\":\"%s\",\"db\":\"%s\",\"table\":\"%s\",\"rows\":[",
//        current_txn_id, g_map.db, g_map.tbl);
    
    json_offset += snprintf(json_event, sizeof(json_event),
    "{\"type\":\"DELETE\",\"txn\":\"%s\",\"db\":\"%s\",\"table\":\"%s\"",
    current_txn_id, g_map.db, g_map.tbl);

    append_primary_key_metadata(json_event, sizeof(json_event),
                                &json_offset, g_map.db, g_map.tbl);

    json_offset += snprintf(json_event + json_offset,
                            sizeof(json_event) - json_offset,
                            ",\"rows\":[");


    int row_num = 0;
    const unsigned char *p = row_data;
    size_t len = row_len;

    uint32_t min_row_size = (ncols + 7) >> 3;

    while(len >= min_row_size && json_offset < sizeof(json_event) - 2000){
        row_num++;
        if(row_num > 1)
            json_offset += snprintf(json_event + json_offset,
                                   sizeof(json_event) - json_offset, ",");

        if(parse_row_to_json_filtered(&p, &len, ncols, present,
                                     json_event, sizeof(json_event), &json_offset) != 0) {
            break;
        }
    }

    json_offset += snprintf(json_event + json_offset,
                           sizeof(json_event) - json_offset, "]}");

    if(row_num > 0) {
        publish_event(g_map.db, g_map.tbl, json_event, current_txn_id);
        log_debug("DELETE %s.%s: %d row(s) captured", g_map.db, g_map.tbl, row_num);
    }
}

// ============================================================================
// ROWS EVENT DEMUX
// ============================================================================

static void parse_rows_event(uint8_t event_type, const unsigned char *payload,
                             uint32_t payload_len)
{
    const unsigned char *p = payload;
    if(payload_len < 8) return;

    uint64_t table_id = le48(p); p += 6;
    (void)table_id;
    p += 2;

//    if(event_type == EVT_WRITE_ROWSv2 || event_type == EVT_UPDATE_ROWSv2 ||
//       event_type == EVT_DELETE_ROWSv2){
//        if(payload_len < (uint32_t)(p - payload + 2)) return;
//        uint16_t extra_len = le16(p); p += 2;
//        if(payload_len < (uint32_t)(p - payload + extra_len)) return;
//        p += extra_len;
//    }
    if (event_type == EVT_WRITE_ROWSv2 ||
        event_type == EVT_UPDATE_ROWSv2 ||
        event_type == EVT_DELETE_ROWSv2) {

        /* extra_data_len (2 bytes) includes its own length */
        if (payload_len < (uint32_t)(p - payload + 2)) return;

        uint16_t extra_len = le16(p);
        p += 2; /* consumed the length field itself */

        if (extra_len > 2) {
            uint16_t extra_data_len = extra_len - 2;

            if (payload_len < (uint32_t)(p - payload + extra_data_len)) return;
            p += extra_data_len;
        }
    }

    if(payload_len < (uint32_t)(p - payload + 1)) return;
    uint64_t ncols64 = *p++;
    uint32_t ncols = (uint32_t)ncols64;
    uint32_t bmp_len = (uint32_t)((ncols + 7) >> 3);

    const unsigned char *before_present = NULL;
    const unsigned char *after_present = NULL;

    if(event_type == EVT_UPDATE_ROWSv1 || event_type == EVT_UPDATE_ROWSv2 ||
       event_type == EVT_MARIA_UPDATE_ROWS_COMPRESSED){
        if(payload_len < (uint32_t)(p - payload + bmp_len * 2)) return;
        before_present = p;
        p += bmp_len;
        after_present = p;
        p += bmp_len;
    } else {
        if(payload_len < (uint32_t)(p - payload + bmp_len)) return;
        before_present = p;
        p += bmp_len;
    }

    const unsigned char *row_data = NULL;
    size_t row_len = 0;
    unsigned char *dec = NULL;

    if(event_type == EVT_MARIA_WRITE_ROWS_COMPRESSED ||
       event_type == EVT_MARIA_UPDATE_ROWS_COMPRESSED ||
       event_type == EVT_MARIA_DELETE_ROWS_COMPRESSED){
        if(mariadb_decompress_rows(p, payload_len - (uint32_t)(p - payload),
                                   &dec, &row_len) != 0){
            log_error("Failed to decompress rows");
            return;
        }
        row_data = dec;
    } else {
        row_data = p;
        row_len = payload_len - (uint32_t)(p - payload);
    }

    if(event_type == EVT_WRITE_ROWSv1 || event_type == EVT_WRITE_ROWSv2 ||
       event_type == EVT_MARIA_WRITE_ROWS_COMPRESSED){
        parse_write_rows(row_data, row_len, ncols, before_present);
    } else if(event_type == EVT_UPDATE_ROWSv1 || event_type == EVT_UPDATE_ROWSv2 ||
              event_type == EVT_MARIA_UPDATE_ROWS_COMPRESSED){
        parse_update_rows(row_data, row_len, ncols,
                         before_present, after_present);
    } else {
        parse_delete_rows(row_data, row_len, ncols, before_present);
    }

    if(dec) free(dec);
}

// ============================================================================
// EVENT DISPATCH
// ============================================================================

static void parse_format(const unsigned char *p, uint32_t len){
    if(len < 2) return;
    uint16_t v = le16(p);
    log_info("FORMAT_DESCRIPTION ver=%u", v);
}

static void parse_rotate(const unsigned char *p, uint32_t len){
    if(len < 8) return;

    uint64_t pos = le64(p);
    p += 8;
    len -= 8;

    size_t name_len = 0;
    while(name_len < len &&
          name_len < sizeof(current_binlog) - 1 &&
          p[name_len] != 0) {
        if(p[name_len] < 32 || p[name_len] > 126) break;
        name_len++;
    }

    if(name_len > 0) {
        memcpy(current_binlog, p, name_len);
        current_binlog[name_len] = 0;
    } else {
        strcpy(current_binlog, "<unknown>");
    }
    current_position = pos;

    log_info("ROTATE to '%s' @ %llu", current_binlog, (unsigned long long)pos);

    if(g_config.save_last_position) {
        save_position(current_binlog, current_position);
        events_since_save = 0;
    }
}

static int parse_event(const unsigned char *buf, uint32_t size){
    if(size < 1 || buf[0] != 0x00) return -1;
    buf++; size--;

    if(size < 19) return -1;

    uint8_t type = buf[4];
    uint32_t event_len = le32(buf + 9);
    uint32_t next_pos = le32(buf + 13);

    if(event_len > size) event_len = size;
    if(next_pos > 0) current_position = next_pos;

    uint32_t payload_len = event_len - 19;
    const unsigned char *payload = buf + 19;

    if(has_checksum && payload_len >= 4) payload_len -= 4;

    switch(type){
        case EVT_QUERY_EVENT:
            parse_query(payload, payload_len);
            break;
        case EVT_XID:
            uint64_t xid = 0;
            if(payload_len >= 8) xid = le64(payload);
            if(in_transaction && current_txn_id[0]) {
                log_debug("[txn:%s] XID COMMIT (server_xid=%llu)",
                         current_txn_id, (unsigned long long)xid);
                char event_json[256];
                /* Use last known table-map DB for routing (if any) */
                const char *db = (g_map.db[0] != '\0') ? g_map.db : "";
                if(should_capture_ddl(db)){
                    snprintf(event_json, sizeof(event_json),
                             "{\"type\":\"COMMIT\",\"txn\":\"%s\",\"db\":\"%s\",\"xid\":%llu}"
                             ,current_txn_id, db, (unsigned long long)xid);
                    /* No specific table for a COMMIT boundary */
                    publish_event(db, "COMMIT", event_json, current_txn_id);
                } else {
                    log_debug("[txn:%s] DDL/DCL for database %s - IGNORED (capture disabled)", current_txn_id, db);
                }
            } else {
                log_debug("XID COMMIT (server_xid=%llu)", (unsigned long long)xid);
            }
            in_transaction = 0;
            current_txn_id[0] = '\0';
            break;
            break;
        case EVT_FORMAT_DESCRIPTION:
            parse_format(payload, payload_len);
            break;
        case EVT_ROTATE:
            parse_rotate(payload, payload_len);
            break;
        case EVT_TABLE_MAP:
            parse_table_map(payload, payload_len);
            break;
        case EVT_WRITE_ROWSv1:
        case EVT_WRITE_ROWSv2:
        case EVT_UPDATE_ROWSv1:
        case EVT_UPDATE_ROWSv2:
        case EVT_DELETE_ROWSv1:
        case EVT_DELETE_ROWSv2:
        case EVT_MARIA_WRITE_ROWS_COMPRESSED:
        case EVT_MARIA_UPDATE_ROWS_COMPRESSED:
        case EVT_MARIA_DELETE_ROWS_COMPRESSED:
            parse_rows_event(type, payload, payload_len);
            break;
    }

    events_since_save++;
    if(g_config.save_last_position) {
        if(g_config.save_position_event_count > 0) {
            if(events_since_save >= g_config.save_position_event_count) {
                save_position(current_binlog, current_position);
                events_since_save = 0;
            }
        } else {
            save_position(current_binlog, current_position);
            events_since_save = 0;
        }
    }

    return 0;
}

// ============================================================================
// STREAM LOOP
// ============================================================================

static int stream_binlog(MYSQL *m, MYSQL_RPL *rpl){
    log_info("Streaming from %s @ %llu", rpl->file_name,
            (unsigned long long)rpl->start_position);
    log_info("Waiting for events (Ctrl+C to stop)...");

    while(keep_running){
        int ret = mysql_binlog_fetch(m, rpl);
        if(ret != 0){
            if(!keep_running) {
                log_info("Shutting down gracefully...");
                return 0;
            }
            if(mysql_errno(m) == 0){
                usleep(100000);
                continue;
            }
            return -1;
        }
        if(rpl->size == 0){
            usleep(100000);
            continue;
        }
        events_received++;
        parse_event(rpl->buffer, (uint32_t)rpl->size);
    }
    return 0;
}

// ============================================================================
// MAIN
// ============================================================================

static log_file_t main_log;

int main(int argc, char **argv){
    signal(SIGINT,  signal_handler);
    signal(SIGTERM, signal_handler);

    if(argc < 2){
        fprintf(stderr, "Usage: %s config.json\n", argv[0]);
        return 1;
    }

    if(load_config(argv[1], &g_config) != 0) {
        fprintf(stderr, "Failed to load configuration\n");
        return 1;
    }
    
//    if(g_config.log_file[0]) {
//        g_config.log_fp = fopen(g_config.log_file, "a");
//        if(!g_config.log_fp) {
//            fprintf(stderr, "Cannot open log file: %s\n", g_config.log_file);
//            g_config.log_fp = NULL;
//        }
//    }

    //log_add_fp(g_config.log_fp, parse_log_level(g_config.log_level));
    log_set_level(parse_log_level(g_config.stdout_level));
    if (log_add_rotating_file(&main_log,
                              g_config.log_file,
                              g_config.max_file_size,
                              g_config.max_log_count,
                              parse_log_level(g_config.log_level)) != 0) {
        fprintf(stderr, "Failed to open rotating log file\n");
    }

    if(log_db_config(&g_config) != 0) {
        log_warn("Failed to print Database configurations");
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

    MYSQL *m = mysql_init(NULL);
    if(!m) {
        log_error("Failed to initialize MySQL connection");
        return 1;
    }

    g_metadata_conn = mysql_init(NULL);
    if(!g_metadata_conn) {
        log_warn("Failed to initialize metadata connection");
    }

    if(!mysql_real_connect(m, g_config.host, g_config.username, g_config.password,
                           NULL, g_config.port, NULL, 0)){
        log_error("connect: %s", mysql_error(m));
        mysql_close(m);
        if(g_metadata_conn) mysql_close(g_metadata_conn);
        return 1;
    }

    if(g_metadata_conn) {
        if(!mysql_real_connect(g_metadata_conn, g_config.host, g_config.username,
                               g_config.password, NULL, g_config.port, NULL, 0)){
            log_warn("Metadata connection failed: %s", mysql_error(g_metadata_conn));
            mysql_close(g_metadata_conn);
            g_metadata_conn = NULL;
        } else {
            log_info("Metadata connection established");
        }
    }

    g_socket_fd = get_mysql_socket_fd(m);
    detect_checksum(m);
    announce_checksum(m);

    char start_file[256] = "";
    uint64_t start_pos = 4;

    if(restore_position(start_file, &start_pos) != 0) {
        if(g_config.binlog_file[0]) {
            //strncpy(start_file, g_config.binlog_file, sizeof(start_file) - 1);
            snprintf(start_file, sizeof(start_file), "%s", g_config.binlog_file);
            start_pos = g_config.binlog_position;
        } else {
            if(get_master_position(m, start_file, sizeof(start_file), &start_pos) != 0){
                log_error("Cannot get master position: %s", mysql_error(m));
                mysql_close(m);
                if(g_metadata_conn) mysql_close(g_metadata_conn);
                return 1;
            }
        }
    }

    MYSQL_RPL rpl;
    memset(&rpl, 0, sizeof(rpl));
    rpl.file_name_length = strlen(start_file);
    rpl.file_name = start_file;
    rpl.start_position = start_pos;
    rpl.server_id = g_config.server_id;
    rpl.flags = 0;

    if(mysql_binlog_open(m, &rpl) != 0){
        log_error("mysql_binlog_open: %s", mysql_error(m));
        mysql_close(m);
        if(g_metadata_conn) mysql_close(g_metadata_conn);
        return 1;
    }

    int ret = stream_binlog(m, &rpl);

    if(g_config.save_last_position) {
        save_position(current_binlog, current_position);
    }

    mysql_binlog_close(m, &rpl);
    mysql_close(m);
    if(g_metadata_conn) {
        mysql_close(g_metadata_conn);
        g_metadata_conn = NULL;
    }
    g_socket_fd = -1;

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

    free(g_map.types);
    free(g_map.metadata);
    free(g_map.real_types);

    if(g_map.column_names) {
        for(uint32_t i = 0; i < g_map.ncols; i++) {
            free(g_map.column_names[i]);
        }
        free(g_map.column_names);
    }

    free_enum_cache();

//    for(int i = 0; i < g_config.database_count; i++) {
//        for(int j = 0; j < g_config.databases[i].table_count; j++) {
//            free(g_config.databases[i].tables[j].columns);
//        }
//        free(g_config.databases[i].tables);
//    }
//    free(g_config.databases);
    for (int i = 0; i < g_config.database_count; i++) {
        for (int j = 0; j < g_config.databases[i].table_count; j++) {
            table_config_t *tbl = &g_config.databases[i].tables[j];

            /* free columns array (if allocated) */
            free(tbl->columns);

            /* free primary key strings */
            if (tbl->primary_keys) {
                for (int pk = 0; pk < tbl->pk_count; pk++) {
                    free(tbl->primary_keys[pk]);
                }
                free(tbl->primary_keys);
            }
        }
        free(g_config.databases[i].tables);
    }
    free(g_config.databases);


    log_info("Total events: %llu", (unsigned long long)events_received);
//    if(g_config.log_fp && g_config.log_fp != stdout) {
//        fclose(g_config.log_fp);
//    }
    log_close_file(&main_log);
    return ret;
}
