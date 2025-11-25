# Plugin Development Guide

## Overview

The Binlog Streamer supports a flexible plugin architecture that allows you to create custom publishers for streaming MySQL binlog events. Plugins are dynamically loaded shared libraries (.so files) that implement the Publisher Plugin API.

## Plugin Types

### 1. Native C Plugins
Write publishers directly in C for maximum performance.

**Examples:**
- `file_publisher.c` - Write events to files
- `zmq_publisher.c` - Publish via ZeroMQ
- `kafka_publisher.c` - Publish to Apache Kafka
- `redis_publisher.c` - Publish to Redis streams/pubsub
- `webhook_publisher.c` - Send events via HTTP webhooks
- `syslog_publisher.c` - Send events to syslog

### 2. Scriptable Plugins
Embed scripting languages for rapid development.

**Examples:**
- `lua_publisher.c` - Execute Lua scripts
- `python_publisher.c` - Execute Python scripts
- `java_publisher.c` - Execute Java code

## Publisher Plugin API

All plugins must implement the following interface defined in `src/include/publisher_api.h`:

```c
typedef struct {
    const char *name;
    void *(*create)(const char *config_json);
    int (*publish)(void *context, const char *event_json);
    void (*destroy)(void *context);
} PublisherPlugin;

// Required export function
PublisherPlugin *publisher_plugin_create(void);
```

### API Functions

#### 1. `publisher_plugin_create()`
- **Purpose:** Entry point for plugin initialization
- **Returns:** Pointer to PublisherPlugin structure
- **Called:** Once when plugin is loaded

#### 2. `create(const char *config_json)`
- **Purpose:** Initialize publisher with configuration
- **Parameters:** JSON configuration string
- **Returns:** Plugin context pointer (opaque to core)
- **Called:** Once during startup per publisher instance

#### 3. `publish(void *context, const char *event_json)`
- **Purpose:** Publish a binlog event
- **Parameters:** 
  - `context`: Plugin context from create()
  - `event_json`: JSON-formatted binlog event
- **Returns:** 0 on success, non-zero on error
- **Called:** For each binlog event

#### 4. `destroy(void *context)`
- **Purpose:** Cleanup and free resources
- **Parameters:** Plugin context from create()
- **Returns:** void
- **Called:** Once during shutdown

## Creating a Simple Plugin

### Example: Hello World Publisher

```c
#include "publisher_api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *output_file;
    FILE *fp;
} HelloContext;

static void *hello_create(const char *config_json) {
    HelloContext *ctx = malloc(sizeof(HelloContext));
    ctx->output_file = strdup("/tmp/hello.log");
    ctx->fp = fopen(ctx->output_file, "a");
    if (!ctx->fp) {
        free(ctx->output_file);
        free(ctx);
        return NULL;
    }
    return ctx;
}

static int hello_publish(void *context, const char *event_json) {
    HelloContext *ctx = (HelloContext *)context;
    fprintf(ctx->fp, "Event: %s\n", event_json);
    fflush(ctx->fp);
    return 0;
}

static void hello_destroy(void *context) {
    HelloContext *ctx = (HelloContext *)context;
    if (ctx->fp) fclose(ctx->fp);
    free(ctx->output_file);
    free(ctx);
}

PublisherPlugin *publisher_plugin_create(void) {
    static PublisherPlugin plugin = {
        .name = "hello",
        .create = hello_create,
        .publish = hello_publish,
        .destroy = hello_destroy
    };
    return &plugin;
}
```

### Building Your Plugin

Add to `Makefile`:

```makefile
$(LIB_DIR)/hello_publisher.so: $(PLUGIN_DIR)/hello_publisher.c
	$(CC) $(CFLAGS) -shared -o $@ $<
```

Then build:

```bash
make build/lib/hello_publisher.so
```

## Event JSON Format

Events are passed to plugins as JSON strings with the following structure:

```json
{
    "timestamp": "2024-11-25 12:00:00",
    "database": "mydb",
    "table": "users",
    "event_type": "INSERT",
    "server_id": 1,
    "log_pos": 12345,
    "rows": [
        {
            "id": 100,
            "username": "john",
            "email": "john@example.com"
        }
    ]
}
```

### Event Types
- `INSERT` - New row inserted
- `UPDATE` - Existing row modified
- `DELETE` - Row deleted

## Configuration

Plugins receive configuration as JSON. The format is plugin-specific.

### Example Configuration

```json
{
    "publishers": [
        {
            "name": "my_publisher",
            "plugin_path": "./build/lib/hello_publisher.so",
            "config": {
                "output_file": "/var/log/binlog.log",
                "buffer_size": 4096
            }
        }
    ]
}
```

The `config` object is passed to your `create()` function as a JSON string.

## Best Practices

### 1. Error Handling
- Always check return values
- Handle NULL contexts gracefully
- Log errors appropriately
- Return proper error codes

### 2. Resource Management
- Free all allocated memory in `destroy()`
- Close file handles and network connections
- Use context struct to track resources

### 3. Thread Safety
- Assume `publish()` may be called from multiple threads
- Use mutexes if maintaining shared state
- Avoid global variables

### 4. Performance
- Minimize allocations in `publish()`
- Use buffering for I/O operations
- Consider async publishing for slow operations

### 5. Configuration Parsing
- Use json-c library for parsing
- Provide sensible defaults
- Validate configuration in `create()`

## Testing Your Plugin

### 1. Symbol Check
```bash
nm -D build/lib/your_publisher.so | grep publisher_plugin_create
```

### 2. Load Test
```bash
# Add to config.json
{
    "publishers": [{
        "name": "test",
        "plugin_path": "./build/lib/your_publisher.so",
        "config": {}
    }]
}

# Run application
./build/bin/binlog_stream config/config.json
```

### 3. Event Test
Create a test script to generate events and verify output.

## Advanced Topics

### Scriptable Plugins

For Lua/Python/Java plugins, see example scripts in `scripts/plugin-examples/`:

- `publish.lua` - Lua publisher example
- `publish.py` - Python publisher example
- `JavaPublisher.java` - Java publisher example

### External Dependencies

If your plugin requires external libraries:

1. Add to `Makefile`:
```makefile
$(LIB_DIR)/custom_publisher.so: $(PLUGIN_DIR)/custom_publisher.c
	$(CC) $(CFLAGS) -shared -o $@ $< -lcustom
```

2. Document dependencies in plugin header
3. Consider providing installation script

### Configuration Schema

Document your configuration schema:

```c
/*
 * Configuration Schema:
 * {
 *     "host": "string",        // Required: Server hostname
 *     "port": "integer",       // Optional: Port (default: 8080)
 *     "timeout": "integer"     // Optional: Timeout in seconds (default: 30)
 * }
 */
```

## Debugging

### Enable Debug Logging
```bash
export BINLOG_DEBUG=1
./build/bin/binlog_stream config/config.json
```

### GDB Debugging
```bash
gdb --args ./build/bin/binlog_stream config/config.json
(gdb) break hello_publish
(gdb) run
```

### Valgrind Memory Check
```bash
valgrind --leak-check=full ./build/bin/binlog_stream config/config.json
```

## Plugin Reference

See existing plugins in `src/plugins/` for complete examples:

- **Simple:** `example_publisher.c`, `file_publisher.c`
- **Network:** `zmq_publisher.c`, `webhook_publisher.c`
- **Message Queue:** `kafka_publisher.c`, `redis_publisher.c`
- **System:** `syslog_publisher.c`
- **Scriptable:** `lua_publisher.c`, `python_publisher.c`, `java_publisher.c`

## Support

For questions or issues:
1. Check existing plugins for examples
2. Review this documentation
3. Check logs for error messages
4. Open an issue on the project repository
