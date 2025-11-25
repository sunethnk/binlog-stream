// udp_publisher.c
// UDP Publisher Plugin - Send CDC events via UDP datagrams
//
// Build: gcc -shared -fPIC -o udp_publisher.so udp_publisher.c -I.
//
// Configuration:
//   udp_host: Target hostname or IP address (required)
//   udp_port: Target UDP port (required)
//   max_packet_size: Maximum UDP packet size in bytes (default: 65507)
//   add_newline: Add newline after each JSON event (default: yes)

#include "publisher_api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>

// Plugin private data
typedef struct {
    const char *host;
    int port;
    int sockfd;
    struct sockaddr_in server_addr;
    int max_packet_size;
    int add_newline;
    uint64_t events_sent;
    uint64_t events_failed;
    uint64_t bytes_sent;
    uint64_t packets_dropped;  // Packets too large to send
} udp_publisher_data_t;

// Plugin metadata
static const char* get_name(void) {
    return "udp_publisher";
}

static const char* get_version(void) {
    return "1.0.0";
}

static int get_api_version(void) {
    return PUBLISHER_API_VERSION;
}

// Resolve hostname to IP address
static int resolve_hostname(const char *hostname, struct in_addr *addr) {
    struct hostent *he;
    
    // Try direct IP address first
    if (inet_pton(AF_INET, hostname, addr) == 1) {
        return 0;
    }
    
    // Try DNS lookup
    he = gethostbyname(hostname);
    if (!he) {
        return -1;
    }
    
    memcpy(addr, he->h_addr_list[0], sizeof(struct in_addr));
    return 0;
}

// Initialize publisher
static int init(const publisher_config_t *config, void **plugin_data) {
    PLUGIN_LOG_INFO("Initializing UDP publisher");
    
    udp_publisher_data_t *data = calloc(1, sizeof(udp_publisher_data_t));
    if (!data) {
        PLUGIN_LOG_ERROR("Failed to allocate publisher data");
        return -1;
    }
    
    // Get required configuration
    data->host = PLUGIN_GET_CONFIG(config, "udp_host");
    if (!data->host) {
        PLUGIN_LOG_ERROR("Missing required configuration: udp_host");
        free(data);
        return -1;
    }
    
    const char *port_str = PLUGIN_GET_CONFIG(config, "udp_port");
    if (!port_str) {
        PLUGIN_LOG_ERROR("Missing required configuration: udp_port");
        free(data);
        return -1;
    }
    data->port = atoi(port_str);
    if (data->port <= 0 || data->port > 65535) {
        PLUGIN_LOG_ERROR("Invalid UDP port: %s", port_str);
        free(data);
        return -1;
    }
    
    // Get optional configuration
    const char *max_size_str = PLUGIN_GET_CONFIG(config, "max_packet_size");
    if (max_size_str) {
        data->max_packet_size = atoi(max_size_str);
        if (data->max_packet_size <= 0 || data->max_packet_size > 65507) {
            PLUGIN_LOG_WARN("Invalid max_packet_size, using default 65507");
            data->max_packet_size = 65507;
        }
    } else {
        data->max_packet_size = 65507;  // Max UDP payload size
    }
    
    const char *add_newline_str = PLUGIN_GET_CONFIG(config, "add_newline");
    if (add_newline_str && (strcmp(add_newline_str, "no") == 0 || 
                            strcmp(add_newline_str, "false") == 0 || 
                            strcmp(add_newline_str, "0") == 0)) {
        data->add_newline = 0;
    } else {
        data->add_newline = 1;  // Default: add newline
    }
    
    // Create UDP socket
    data->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (data->sockfd < 0) {
        PLUGIN_LOG_ERROR("Failed to create UDP socket: %s", strerror(errno));
        free(data);
        return -1;
    }
    
    // Resolve hostname
    struct in_addr server_ip;
    if (resolve_hostname(data->host, &server_ip) != 0) {
        PLUGIN_LOG_ERROR("Failed to resolve hostname: %s", data->host);
        close(data->sockfd);
        free(data);
        return -1;
    }
    
    // Setup server address
    memset(&data->server_addr, 0, sizeof(data->server_addr));
    data->server_addr.sin_family = AF_INET;
    data->server_addr.sin_port = htons(data->port);
    data->server_addr.sin_addr = server_ip;
    
    *plugin_data = data;
    
    PLUGIN_LOG_INFO("UDP publisher configured: host=%s, port=%d, max_packet_size=%d, add_newline=%s",
                   data->host, data->port, data->max_packet_size, 
                   data->add_newline ? "yes" : "no");
    
    return 0;
}

// Start publisher
static int start(void *plugin_data) {
    udp_publisher_data_t *data = (udp_publisher_data_t*)plugin_data;
    
    if (!data) {
        PLUGIN_LOG_ERROR("Invalid plugin data");
        return -1;
    }
    
    PLUGIN_LOG_INFO("Starting UDP publisher: %s:%d", data->host, data->port);
    
    // Test connectivity by sending a small test packet
    const char *test_msg = "{\"test\":\"connection\"}";
    ssize_t sent = sendto(data->sockfd, test_msg, strlen(test_msg), 0,
                         (struct sockaddr*)&data->server_addr, 
                         sizeof(data->server_addr));
    
    if (sent < 0) {
        PLUGIN_LOG_WARN("Failed to send test packet: %s (continuing anyway)", 
                       strerror(errno));
    } else {
        PLUGIN_LOG_INFO("Test packet sent successfully");
    }
    
    PLUGIN_LOG_INFO("UDP publisher started: %s:%d", data->host, data->port);
    return 0;
}

// Publish event
static int publish(void *plugin_data, const cdc_event_t *event) {
    udp_publisher_data_t *data = (udp_publisher_data_t*)plugin_data;
    
    if (!data || data->sockfd < 0) {
        PLUGIN_LOG_ERROR("Invalid plugin data or socket");
        return -1;
    }
    
    if (!event || !event->json) {
        PLUGIN_LOG_ERROR("Invalid event data");
        data->events_failed++;
        return -1;
    }
    
    // Prepare packet
    size_t json_len = strlen(event->json);
    size_t packet_len = json_len + (data->add_newline ? 1 : 0);
    
    // Check if packet fits
    if (packet_len > data->max_packet_size) {
        PLUGIN_LOG_WARN("Event too large for UDP packet: %zu bytes (max: %d) - dropping",
                       packet_len, data->max_packet_size);
        data->packets_dropped++;
        data->events_failed++;
        return -1;
    }
    
    // Allocate buffer
    char *packet = malloc(packet_len);
    if (!packet) {
        PLUGIN_LOG_ERROR("Failed to allocate packet buffer");
        data->events_failed++;
        return -1;
    }
    
    // Copy JSON and optionally add newline
    memcpy(packet, event->json, json_len);
    if (data->add_newline) {
        packet[json_len] = '\n';
    }
    
    // Send UDP packet
    ssize_t sent = sendto(data->sockfd, packet, packet_len, 0,
                         (struct sockaddr*)&data->server_addr,
                         sizeof(data->server_addr));
    
    free(packet);
    
    if (sent < 0) {
        PLUGIN_LOG_ERROR("Failed to send UDP packet: %s", strerror(errno));
        data->events_failed++;
        return -1;
    }
    
    if (sent != (ssize_t)packet_len) {
        PLUGIN_LOG_WARN("Partial UDP send: %zd of %zu bytes", sent, packet_len);
    }
    
    data->events_sent++;
    data->bytes_sent += sent;
    
    PLUGIN_LOG_TRACE("Published event to UDP %s:%d: txn=%s, db=%s, table=%s, size=%zu",
                    data->host, data->port,
                    event->txn ? event->txn : "",
                    event->db ? event->db : "",
                    event->table ? event->table : "",
                    packet_len);
    
    return 0;
}

// Stop publisher
static int stop(void *plugin_data) {
    udp_publisher_data_t *data = (udp_publisher_data_t*)plugin_data;
    
    if (!data) {
        PLUGIN_LOG_ERROR("Invalid plugin data");
        return -1;
    }
    
    PLUGIN_LOG_INFO("Stopping UDP publisher: %s:%d (sent=%llu, failed=%llu, dropped=%llu, bytes=%llu)",
                   data->host, data->port,
                   data->events_sent, data->events_failed, 
                   data->packets_dropped, data->bytes_sent);
    
    return 0;
}

// Cleanup
static void cleanup(void *plugin_data) {
    udp_publisher_data_t *data = (udp_publisher_data_t*)plugin_data;
    
    if (!data) {
        return;
    }
    
    if (data->sockfd >= 0) {
        close(data->sockfd);
    }
    
    free(data);
    
    PLUGIN_LOG_INFO("UDP publisher cleaned up");
}

// Health check
static int health_check(void *plugin_data) {
    udp_publisher_data_t *data = (udp_publisher_data_t*)plugin_data;
    
    if (!data || data->sockfd < 0) {
        return -1;
    }
    
    // Check if socket is still valid
    int error = 0;
    socklen_t len = sizeof(error);
    if (getsockopt(data->sockfd, SOL_SOCKET, SO_ERROR, &error, &len) < 0) {
        PLUGIN_LOG_ERROR("Socket health check failed: %s", strerror(errno));
        return -1;
    }
    
    if (error != 0) {
        PLUGIN_LOG_ERROR("Socket has error: %s", strerror(error));
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
    .publish_batch = NULL,  // Not implemented
    .health_check = health_check,
};

// Plugin entry point
PUBLISHER_PLUGIN_DEFINE(udp_publisher) {
    publisher_plugin_t *p = malloc(sizeof(publisher_plugin_t));
    if (!p) return -1;
    
    p->callbacks = &callbacks;
    p->plugin_data = NULL;
    
    *plugin = p;
    return 0;
}