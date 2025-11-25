// mysql_publisher.c
// MySQL Database Publisher Plugin
// Writes CDC events to a MySQL table for auditing/archival
//
// Build: gcc -shared -fPIC -o mysql_publisher.so mysql_publisher.c -I. -lmysqlclient

#include "publisher_api.h"
#include <mysql/mysql.h>
#include <stdlib.h>
#include <string.h>

// Plugin private data
typedef struct {
    MYSQL *conn;
    char host[128];
    int port;
    char username[64];
    char password[64];
    char database[64];
    char table[128];
    uint64_t events_written;
    uint64_t events_failed;
} mysql_publisher_data_t;

static const char* get_name(void) {
    return "mysql_publisher";
}

static const char* get_version(void) {
    return "1.0.0";
}

static int get_api_version(void) {
    return PUBLISHER_API_VERSION;
}

static int init(const publisher_config_t *config, void **plugin_data) {
    PLUGIN_LOG_INFO("Initializing MySQL publisher");
    
    mysql_publisher_data_t *data = calloc(1, sizeof(mysql_publisher_data_t));
    if (!data) {
        PLUGIN_LOG_ERROR("Failed to allocate publisher data");
        return -1;
    }
    
    // Get required config
    const char *host = PLUGIN_GET_CONFIG(config, "host");
    const char *database = PLUGIN_GET_CONFIG(config, "database");
    const char *table = PLUGIN_GET_CONFIG(config, "table");
    
    if (!host || !database || !table) {
        PLUGIN_LOG_ERROR("Missing required config: host, database, or table");
        free(data);
        return -1;
    }
    
    strncpy(data->host, host, sizeof(data->host) - 1);
    strncpy(data->database, database, sizeof(data->database) - 1);
    strncpy(data->table, table, sizeof(data->table) - 1);
    
    // Get optional config
    data->port = PLUGIN_GET_CONFIG_INT(config, "port", 3306);
    
    const char *username = PLUGIN_GET_CONFIG(config, "username");
    const char *password = PLUGIN_GET_CONFIG(config, "password");
    
    if (username) strncpy(data->username, username, sizeof(data->username) - 1);
    if (password) strncpy(data->password, password, sizeof(data->password) - 1);
    
    *plugin_data = data;
    
    PLUGIN_LOG_INFO("MySQL publisher configured: %s:%d/%s.%s",
                   data->host, data->port, data->database, data->table);
    
    return 0;
}

static int start(void *plugin_data) {
    mysql_publisher_data_t *data = (mysql_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Starting MySQL publisher");
    
    // Initialize MySQL connection
    data->conn = mysql_init(NULL);
    if (!data->conn) {
        PLUGIN_LOG_ERROR("Failed to initialize MySQL connection");
        return -1;
    }
    
    // Set connection options
    my_bool reconnect = 1;
    mysql_options(data->conn, MYSQL_OPT_RECONNECT, &reconnect);
    
    // Connect to MySQL
    if (!mysql_real_connect(data->conn, 
                           data->host, 
                           data->username[0] ? data->username : NULL,
                           data->password[0] ? data->password : NULL,
                           data->database,
                           data->port, 
                           NULL, 
                           0)) {
        PLUGIN_LOG_ERROR("Failed to connect to MySQL: %s", mysql_error(data->conn));
        mysql_close(data->conn);
        data->conn = NULL;
        return -1;
    }
    
    // Verify table exists or create it
    char query[1024];
    snprintf(query, sizeof(query),
             "CREATE TABLE IF NOT EXISTS %s ("
             "  id BIGINT AUTO_INCREMENT PRIMARY KEY,"
             "  event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
             "  txn_id VARCHAR(64),"
             "  source_db VARCHAR(64),"
             "  source_table VARCHAR(64),"
             "  event_type VARCHAR(32),"
             "  binlog_file VARCHAR(128),"
             "  binlog_position BIGINT,"
             "  event_json JSON,"
             "  INDEX idx_time (event_time),"
             "  INDEX idx_source (source_db, source_table),"
             "  INDEX idx_txn (txn_id)"
             ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
             data->table);
    
    if (mysql_query(data->conn, query) != 0) {
        PLUGIN_LOG_WARN("Failed to create table (may already exist): %s", 
                       mysql_error(data->conn));
        // Continue anyway - table might already exist
    }
    
    PLUGIN_LOG_INFO("MySQL publisher started: %s:%d/%s.%s",
                   data->host, data->port, data->database, data->table);
    
    return 0;
}

static int publish(void *plugin_data, const cdc_event_t *event) {
    mysql_publisher_data_t *data = (mysql_publisher_data_t*)plugin_data;
    
    if (!data->conn || !event || !event->json) {
        return -1;
    }
    
    // Escape JSON string for SQL
    size_t json_len = strlen(event->json);
    char *escaped_json = malloc(json_len * 2 + 1);
    if (!escaped_json) {
        data->events_failed++;
        return -1;
    }
    
    mysql_real_escape_string(data->conn, escaped_json, event->json, json_len);
    
    // Build INSERT query
    char query[65536];
    snprintf(query, sizeof(query),
             "INSERT INTO %s (txn_id, source_db, source_table, binlog_file, "
             "binlog_position, event_json) VALUES ('%s', '%s', '%s', '%s', %llu, '%s')",
             data->table,
             event->txn ? event->txn : "",
             event->db ? event->db : "",
             event->table ? event->table : "",
             event->binlog_file ? event->binlog_file : "",
             (unsigned long long)event->position,
             escaped_json);
    
    free(escaped_json);
    
    // Execute query
    if (mysql_query(data->conn, query) != 0) {
        PLUGIN_LOG_ERROR("Failed to insert event: %s", mysql_error(data->conn));
        data->events_failed++;
        return -1;
    }
    
    data->events_written++;
    
    PLUGIN_LOG_TRACE("Written to MySQL: db=%s, table=%s, txn=%s",
                    event->db, event->table, event->txn ? event->txn : "");
    
    return 0;
}

static int stop(void *plugin_data) {
    mysql_publisher_data_t *data = (mysql_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Stopping MySQL publisher (written=%llu, failed=%llu)",
                   data->events_written, data->events_failed);
    
    if (data->conn) {
        mysql_close(data->conn);
        data->conn = NULL;
    }
    
    return 0;
}

static void cleanup(void *plugin_data) {
    mysql_publisher_data_t *data = (mysql_publisher_data_t*)plugin_data;
    
    if (data) {
        if (data->conn) {
            mysql_close(data->conn);
        }
        free(data);
    }
    
    PLUGIN_LOG_INFO("MySQL publisher cleaned up");
}

static int health_check(void *plugin_data) {
    mysql_publisher_data_t *data = (mysql_publisher_data_t*)plugin_data;
    
    if (!data || !data->conn) {
        return -1;
    }
    
    // Ping the connection
    if (mysql_ping(data->conn) != 0) {
        PLUGIN_LOG_WARN("MySQL connection health check failed");
        return -1;
    }
    
    return 0;
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

PUBLISHER_PLUGIN_DEFINE(mysql_publisher) {
    publisher_plugin_t *p = malloc(sizeof(publisher_plugin_t));
    if (!p) return -1;
    
    p->callbacks = &callbacks;
    p->plugin_data = NULL;
    
    *plugin = p;
    return 0;
}