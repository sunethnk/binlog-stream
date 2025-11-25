// kafka_publisher.c
// Kafka Publisher Plugin for Binlog Streamer
//
// Build: gcc -shared -fPIC -o kafka_publisher.so kafka_publisher.c -I. -lrdkafka

#include "publisher_api.h"
#include <librdkafka/rdkafka.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

// Plugin private data
typedef struct {
    rd_kafka_t *producer;
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topic_conf;
    
    char bootstrap_servers[512];
    int topic_per_table;
    char topic_prefix[128];
    char compression[32];
    int flush_timeout_ms;
    int batch_size;
    
    uint64_t messages_sent;
    uint64_t messages_failed;
    uint64_t bytes_sent;
    
} kafka_publisher_data_t;

// Plugin metadata
static const char* get_name(void) {
    return "kafka_publisher";
}

static const char* get_version(void) {
    return "1.0.0";
}

static int get_api_version(void) {
    return PUBLISHER_API_VERSION;
}

// Kafka delivery report callback
static void kafka_delivery_callback(rd_kafka_t *rk,
                                   const rd_kafka_message_t *rkmessage,
                                   void *opaque) {
    kafka_publisher_data_t *data = (kafka_publisher_data_t*)opaque;
    
    if (rkmessage->err) {
        data->messages_failed++;
        PLUGIN_LOG_WARN("Kafka delivery failed: %s",
                       rd_kafka_err2str(rkmessage->err));
    } else {
        data->messages_sent++;
        data->bytes_sent += rkmessage->len;
        
        PLUGIN_LOG_TRACE("Kafka message delivered: topic=%s partition=%d offset=%ld",
                        rd_kafka_topic_name(rkmessage->rkt),
                        rkmessage->partition,
                        rkmessage->offset);
    }
}

// Kafka error callback
static void kafka_error_callback(rd_kafka_t *rk, int err,
                                const char *reason, void *opaque) {
    PLUGIN_LOG_ERROR("Kafka error: %s: %s",
                    rd_kafka_err2str(err), reason);
}

// Kafka logger callback
static void kafka_logger_callback(const rd_kafka_t *rk, int level,
                                 const char *fac, const char *buf) {
    if (level <= 3) {  // LOG_ERR
        PLUGIN_LOG_ERROR("Kafka [%s]: %s", fac, buf);
    } else if (level <= 5) {  // LOG_NOTICE
        PLUGIN_LOG_WARN("Kafka [%s]: %s", fac, buf);
    } else if (level <= 6) {  // LOG_INFO
        PLUGIN_LOG_INFO("Kafka [%s]: %s", fac, buf);
    } else {
        PLUGIN_LOG_DEBUG("Kafka [%s]: %s", fac, buf);
    }
}

// Initialize publisher
static int init(const publisher_config_t *config, void **plugin_data) {
    PLUGIN_LOG_INFO("Initializing Kafka publisher");
    
    kafka_publisher_data_t *data = calloc(1, sizeof(kafka_publisher_data_t));
    if (!data) {
        PLUGIN_LOG_ERROR("Failed to allocate publisher data");
        return -1;
    }
    
    // Get required config
    const char *bootstrap = PLUGIN_GET_CONFIG(config, "bootstrap_servers");
    if (!bootstrap) {
        PLUGIN_LOG_ERROR("Missing required config: bootstrap_servers");
        free(data);
        return -1;
    }
    
    strncpy(data->bootstrap_servers, bootstrap, sizeof(data->bootstrap_servers) - 1);
    
    // Get optional config
    const char *prefix = PLUGIN_GET_CONFIG(config, "topic_prefix");
    if (prefix) {
        strncpy(data->topic_prefix, prefix, sizeof(data->topic_prefix) - 1);
    } else {
        strcpy(data->topic_prefix, "cdc.");
    }
    
    const char *compression = PLUGIN_GET_CONFIG(config, "compression");
    if (compression) {
        strncpy(data->compression, compression, sizeof(data->compression) - 1);
    } else {
        strcpy(data->compression, "snappy");
    }
    
    data->flush_timeout_ms = PLUGIN_GET_CONFIG_INT(config, "flush_timeout_ms", 1000);
    data->batch_size = PLUGIN_GET_CONFIG_INT(config, "batch_size", 1000);
    data->topic_per_table = PLUGIN_GET_CONFIG_BOOL(config, "topic_per_table", 0);
    
    *plugin_data = data;
    
    PLUGIN_LOG_INFO("Kafka publisher configured: bootstrap=%s, prefix=%s, compression=%s",
                   data->bootstrap_servers, data->topic_prefix, data->compression);
    
    return 0;
}

// Start publisher
static int start(void *plugin_data) {
    kafka_publisher_data_t *data = (kafka_publisher_data_t*)plugin_data;
    char errstr[512];
    
    PLUGIN_LOG_INFO("Starting Kafka publisher");
    
    // Create Kafka configuration
    data->conf = rd_kafka_conf_new();
    if (!data->conf) {
        PLUGIN_LOG_ERROR("Failed to create Kafka config");
        return -1;
    }
    
    // Set bootstrap servers
    if (rd_kafka_conf_set(data->conf, "bootstrap.servers",
                         data->bootstrap_servers,
                         errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        PLUGIN_LOG_ERROR("Failed to set bootstrap.servers: %s", errstr);
        rd_kafka_conf_destroy(data->conf);
        data->conf = NULL;
        return -1;
    }
    
    // Set compression
    if (rd_kafka_conf_set(data->conf, "compression.type",
                         data->compression,
                         errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        PLUGIN_LOG_ERROR("Failed to set compression: %s", errstr);
        rd_kafka_conf_destroy(data->conf);
        data->conf = NULL;
        return -1;
    }
    
    // Set batch settings for better throughput
    char batch_size_str[32];
    snprintf(batch_size_str, sizeof(batch_size_str), "%d", data->batch_size);
    rd_kafka_conf_set(data->conf, "batch.num.messages", batch_size_str, NULL, 0);
    rd_kafka_conf_set(data->conf, "linger.ms", "10", NULL, 0);
    
    // Set callbacks
    rd_kafka_conf_set_dr_msg_cb(data->conf, kafka_delivery_callback);
    rd_kafka_conf_set_error_cb(data->conf, kafka_error_callback);
    rd_kafka_conf_set_log_cb(data->conf, kafka_logger_callback);
    rd_kafka_conf_set_opaque(data->conf, data);
    
    // Create producer
    data->producer = rd_kafka_new(RD_KAFKA_PRODUCER, data->conf,
                                  errstr, sizeof(errstr));
    if (!data->producer) {
        PLUGIN_LOG_ERROR("Failed to create Kafka producer: %s", errstr);
        rd_kafka_conf_destroy(data->conf);
        data->conf = NULL;
        return -1;
    }
    
    // Config is now owned by producer
    data->conf = NULL;
    
    PLUGIN_LOG_INFO("Kafka publisher started: %s", data->bootstrap_servers);
    return 0;
}

// Build topic name from database and table
static void build_topic_name(const kafka_publisher_data_t *data,
                             const char *db,
                             const char *table,
                             char *topic,
                             size_t topic_size) {
    char *ntb = NULL;
    if(data->topic_per_table){
        snprintf(topic, topic_size, "%s%s.%s",
                data->topic_prefix,
                db ? db : "unknown",
                table ? table : "unknown");
    } else {
        snprintf(topic, topic_size, "%s",
                data->topic_prefix);
    }
    free(ntb);
}

// Publish event
static int publish(void *plugin_data, const cdc_event_t *event) {
    kafka_publisher_data_t *data = (kafka_publisher_data_t*)plugin_data;
    
    if (!data->producer || !event || !event->json) {
        return -1;
    }
    
    // Build topic name
    char topic[256];
    build_topic_name(data, event->db, event->table, topic, sizeof(topic));
    
    // Produce message
    int ret = rd_kafka_producev(
        data->producer,
        RD_KAFKA_V_TOPIC(topic),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_VALUE((void*)event->json, strlen(event->json)),
        RD_KAFKA_V_KEY(event->txn, event->txn ? strlen(event->txn) : 0),
        RD_KAFKA_V_END
    );
    
    if (ret != RD_KAFKA_RESP_ERR_NO_ERROR) {
        PLUGIN_LOG_WARN("Failed to produce message: %s",
                       rd_kafka_err2str(rd_kafka_last_error()));
        data->messages_failed++;
        return -1;
    }
    
    // Poll for delivery reports (non-blocking)
    rd_kafka_poll(data->producer, 0);
    
    PLUGIN_LOG_TRACE("Published to Kafka: topic=%s, txn=%s",
                    topic, event->txn ? event->txn : "");
    
    return 0;
}

// Batch publish (optimized)
static int publish_batch(void *plugin_data, const cdc_event_t **events, int count) {
    kafka_publisher_data_t *data = (kafka_publisher_data_t*)plugin_data;
    int success = 0;
    
    for (int i = 0; i < count; i++) {
        if (publish(plugin_data, events[i]) == 0) {
            success++;
        }
    }
    
    // Poll for delivery reports after batch
    rd_kafka_poll(data->producer, 0);
    
    PLUGIN_LOG_DEBUG("Published batch: %d/%d succeeded", success, count);
    
    return success == count ? 0 : -1;
}

// Stop publisher
static int stop(void *plugin_data) {
    kafka_publisher_data_t *data = (kafka_publisher_data_t*)plugin_data;
    
    PLUGIN_LOG_INFO("Stopping Kafka publisher (sent=%llu, failed=%llu, bytes=%llu)",
                   data->messages_sent, data->messages_failed, data->bytes_sent);
    
    if (data->producer) {
        // Flush pending messages
        PLUGIN_LOG_INFO("Flushing pending Kafka messages...");
        rd_kafka_flush(data->producer, data->flush_timeout_ms);
        
        // Destroy producer
        rd_kafka_destroy(data->producer);
        data->producer = NULL;
    }
    
    return 0;
}

// Cleanup
static void cleanup(void *plugin_data) {
    kafka_publisher_data_t *data = (kafka_publisher_data_t*)plugin_data;
    
    if (data) {
        if (data->producer) {
            rd_kafka_destroy(data->producer);
        }
        if (data->conf) {
            rd_kafka_conf_destroy(data->conf);
        }
        if (data->topic_conf) {
            rd_kafka_topic_conf_destroy(data->topic_conf);
        }
        free(data);
    }
    
    // Wait for background threads to terminate
    rd_kafka_wait_destroyed(1000);
    
    PLUGIN_LOG_INFO("Kafka publisher cleaned up");
}

// Health check
static int health_check(void *plugin_data) {
    kafka_publisher_data_t *data = (kafka_publisher_data_t*)plugin_data;
    
    if (!data || !data->producer) {
        return -1;
    }
    
    // Check outbound queue length
    int queue_len = rd_kafka_outq_len(data->producer);
    
    if (queue_len > data->batch_size * 10) {
        PLUGIN_LOG_WARN("Kafka queue backlog: %d messages", queue_len);
        return -1;
    }
    
    return 0;
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
    .publish_batch = publish_batch,
    .health_check = health_check,
};

// Plugin entry point
PUBLISHER_PLUGIN_DEFINE(kafka_publisher) {
    publisher_plugin_t *p = malloc(sizeof(publisher_plugin_t));
    if (!p) return -1;
    
    p->callbacks = &callbacks;
    p->plugin_data = NULL;
    
    *plugin = p;
    return 0;
}
