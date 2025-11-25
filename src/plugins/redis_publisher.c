// redis_publisher.c
// Redis Publisher Plugin - Publishes CDC events to Redis streams or pub/sub
//
// Build: gcc -shared -fPIC -o redis_publisher.so redis_publisher.c -I. -lhiredis

#include "publisher_api.h"
#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// Plugin private data
typedef struct {
    redisContext *redis;
    char host[128];
    int port;
    char password[128];
    int db;
    int use_streams;  // 0 = pub/sub, 1 = streams
    char stream_prefix[64];
    char pubsub_channel[128];
    uint64_t events_published;
    uint64_t events_failed;
} redis_publisher_data_t;

static const char* get_name(void) {
    return "redis_publisher";
}

static const char* get_version(void) {
    return "1.0.0";
}

static int get_api_version(void) {
    return PUBLISHER_API_VERSION;
}

static int init(const publisher_config_t *config, void **plugin_data) {
    PLUGIN_LOG_INFO("Initializing Redis publisher");
    
    redis_publisher_data_t *data = calloc(1, sizeof(redis_publisher_data_t));
    if (!data) {
        PLUGIN_LOG_ERROR("Failed to allocate publisher data");
        return -1;
    }
    
    // Get Redis connection config
    const char *host = PLUGIN_GET_CONFIG(config, "host");
    if (!host) host = "localhost";
    strncpy(data->host, host, sizeof(data->host) - 1);
    
    data->port = PLUGIN_GET_CONFIG_INT(config, "port", 6379);
    data->db = PLUGIN_GET_CONFIG_INT(config, "db", 0);
    
    const char *password = PLUGIN_GET_CONFIG(config, "password");
    if (password) {
        strncpy(data->password, password, sizeof(data->password) - 1);
    }
    
    // Get mode: streams or pub/sub
    data->use_streams = PLUGIN_GET_CONFIG_BOOL(config, "use_streams", 0);
    
    if (data->use_streams) {
        // Stream mode config
        const char *prefix = PLUGIN_GET_CONFIG(config, "stream_prefix");
        if (!prefix) prefix = "cdc:";
        strncpy(data->stream_prefix, prefix, sizeof(data->stream_prefix) - 1);
    } else {
        // Pub/Sub mode config
        const char *channel = PLUGIN_GET_CONFIG(config, "channel");
        if (!channel) channel = "cdc_events";
        strncpy(data->pubsub_channel, channel, sizeof(data->pubsub_channel) - 1);
    }
    
    *plugin_data = data;
    
    PLUGIN_LOG_INFO("Redis publisher configured: %s:%d db=%d mode=%s",
                   data->host, data->port, data->db,
                   data->use_streams ? "streams" : "pub/sub");
    
    return 0;
}

static int start(void *plugin_data) {
    redis_publisher_data_t *data = (redis_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Starting Redis publisher: %s:%d", data->host, data->port);
    
    // Connect to Redis
    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    data->redis = redisConnectWithTimeout(data->host, data->port, timeout);
    
    if (data->redis == NULL || data->redis->err) {
        if (data->redis) {
            PLUGIN_LOG_ERROR("Failed to connect to Redis: %s", data->redis->errstr);
            redisFree(data->redis);
        } else {
            PLUGIN_LOG_ERROR("Failed to allocate Redis context");
        }
        data->redis = NULL;
        return -1;
    }
    
    // Authenticate if password provided
    if (data->password[0] != '\0') {
        redisReply *reply = redisCommand(data->redis, "AUTH %s", data->password);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            PLUGIN_LOG_ERROR("Redis authentication failed");
            if (reply) freeReplyObject(reply);
            redisFree(data->redis);
            data->redis = NULL;
            return -1;
        }
        freeReplyObject(reply);
    }
    
    // Select database
    if (data->db != 0) {
        redisReply *reply = redisCommand(data->redis, "SELECT %d", data->db);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            PLUGIN_LOG_ERROR("Failed to select Redis database %d", data->db);
            if (reply) freeReplyObject(reply);
            redisFree(data->redis);
            data->redis = NULL;
            return -1;
        }
        freeReplyObject(reply);
    }
    
    PLUGIN_LOG_INFO("Redis publisher started: %s:%d db=%d", 
                   data->host, data->port, data->db);
    return 0;
}

static int publish(void *plugin_data, const cdc_event_t *event) {
    redis_publisher_data_t *data = (redis_publisher_data_t*)plugin_data;
    
    if (!data->redis || !event || !event->json) {
        return -1;
    }
    
    redisReply *reply = NULL;
    
    if (data->use_streams) {
        // Redis Streams mode: XADD stream_name * json <data> db <db> table <table>
        char stream_name[256];
        snprintf(stream_name, sizeof(stream_name), "%s%s.%s",
                data->stream_prefix,
                event->db ? event->db : "unknown",
                event->table ? event->table : "unknown");
        
        reply = redisCommand(data->redis, 
                           "XADD %s * json %s db %s table %s txn %s",
                           stream_name,
                           event->json,
                           event->db ? event->db : "",
                           event->table ? event->table : "",
                           event->txn ? event->txn : "");
    } else {
        // Pub/Sub mode: PUBLISH channel <json>
        reply = redisCommand(data->redis, "PUBLISH %s %s",
                           data->pubsub_channel, event->json);
    }
    
    if (!reply) {
        PLUGIN_LOG_ERROR("Redis command failed: %s", data->redis->errstr);
        data->events_failed++;
        return -1;
    }
    
    if (reply->type == REDIS_REPLY_ERROR) {
        PLUGIN_LOG_ERROR("Redis error: %s", reply->str);
        freeReplyObject(reply);
        data->events_failed++;
        return -1;
    }
    
    freeReplyObject(reply);
    data->events_published++;
    
    PLUGIN_LOG_TRACE("Published to Redis: db=%s, table=%s, mode=%s",
                    event->db, event->table,
                    data->use_streams ? "stream" : "pub/sub");
    
    return 0;
}

static int stop(void *plugin_data) {
    redis_publisher_data_t *data = (redis_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Stopping Redis publisher (published=%llu, failed=%llu)",
                   data->events_published, data->events_failed);
    
    if (data->redis) {
        redisFree(data->redis);
        data->redis = NULL;
    }
    
    return 0;
}

static void cleanup(void *plugin_data) {
    redis_publisher_data_t *data = (redis_publisher_data_t*)plugin_data;
    
    if (data) {
        if (data->redis) {
            redisFree(data->redis);
        }
        free(data);
    }
    
    PLUGIN_LOG_INFO("Redis publisher cleaned up");
}

static int health_check(void *plugin_data) {
    redis_publisher_data_t *data = (redis_publisher_data_t*)plugin_data;
    
    if (!data || !data->redis) {
        return -1;
    }
    
    // Ping Redis
    redisReply *reply = redisCommand(data->redis, "PING");
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        if (reply) freeReplyObject(reply);
        return -1;
    }
    
    freeReplyObject(reply);
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

PUBLISHER_PLUGIN_DEFINE(redis_publisher) {
    publisher_plugin_t *p = malloc(sizeof(publisher_plugin_t));
    if (!p) return -1;
    
    p->callbacks = &callbacks;
    p->plugin_data = NULL;
    
    *plugin = p;
    return 0;
}