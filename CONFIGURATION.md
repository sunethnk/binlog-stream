# Configuration Guide

## Overview

The Binlog Streamer uses JSON configuration files to control its behavior. Configuration files are located in the `config/` directory.

## Configuration File Location

- **Development:** `config/config.json`
- **Production:** `/etc/binlog_streamer/config.json` (after `make install`)

## Complete Configuration Example

```json
{
    "mysql": {
        "host": "localhost",
        "port": 3306,
        "user": "binlog_user",
        "password": "secure_password",
        "server_id": 100,
        "binlog_format": "ROW"
    },
    "databases": [
        {
            "name": "radius",
            "tables": [
                {
                    "name": "radacct",
                    "columns": ["username", "acctstarttime", "acctstoptime", "acctinputoctets", "acctoutputoctets"]
                },
                {
                    "name": "radcheck",
                    "columns": ["username", "attribute", "value"]
                }
            ]
        },
        {
            "name": "scheduler",
            "tables": [
                {
                    "name": "tasks",
                    "columns": ["id", "name", "status", "scheduled_at"]
                }
            ]
        }
    ],
    "checkpoint": {
        "enabled": true,
        "file": "data/binlog_checkpoint.dat",
        "save_interval": 10
    },
    "publishers": [
        {
            "name": "file_output",
            "enabled": true,
            "plugin_path": "./build/lib/file_publisher.so",
            "config": {
                "output_file": "/var/log/binlog/events.json",
                "rotate_size": 104857600,
                "max_files": 10
            }
        },
        {
            "name": "zmq_stream",
            "enabled": true,
            "plugin_path": "./build/lib/zmq_publisher.so",
            "config": {
                "endpoint": "tcp://*:5555",
                "socket_type": "PUB",
                "high_water_mark": 1000
            }
        },
        {
            "name": "kafka_stream",
            "enabled": false,
            "plugin_path": "./build/lib/kafka_publisher.so",
            "config": {
                "brokers": "localhost:9092",
                "topic": "binlog.events",
                "partition": -1,
                "compression": "snappy"
            }
        }
    ],
    "logging": {
        "level": "INFO",
        "file": "/var/log/binlog/streamer.log",
        "max_size": 10485760,
        "max_files": 5
    }
}
```

## Configuration Sections

### 1. MySQL Connection

```json
{
    "mysql": {
        "host": "localhost",
        "port": 3306,
        "user": "binlog_user",
        "password": "secure_password",
        "server_id": 100,
        "binlog_format": "ROW"
    }
}
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| host | string | Yes | - | MySQL server hostname or IP |
| port | integer | No | 3306 | MySQL server port |
| user | string | Yes | - | MySQL user with REPLICATION SLAVE privilege |
| password | string | Yes | - | MySQL user password |
| server_id | integer | Yes | - | Unique server ID for replication |
| binlog_format | string | No | ROW | Binlog format (ROW, STATEMENT, MIXED) |

**MySQL User Setup:**
```sql
CREATE USER 'binlog_user'@'%' IDENTIFIED BY 'secure_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'binlog_user'@'%';
GRANT SELECT ON database_name.* TO 'binlog_user'@'%';
FLUSH PRIVILEGES;
```

### 2. Database and Table Filtering

```json
{
    "databases": [
        {
            "name": "mydb",
            "tables": [
                {
                    "name": "users",
                    "columns": ["id", "username", "email"]
                },
                {
                    "name": "orders",
                    "columns": ["*"]
                }
            ]
        }
    ]
}
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| databases | array | Yes | List of databases to monitor |
| name | string | Yes | Database name |
| tables | array | Yes | List of tables in the database |
| columns | array | Yes | List of columns to capture (use "*" for all) |

**Column Filtering:**
- Specify column names to capture only specific columns
- Use `["*"]` to capture all columns
- Column filtering reduces event size and improves performance

### 3. Checkpoint Management

```json
{
    "checkpoint": {
        "enabled": true,
        "file": "data/binlog_checkpoint.dat",
        "save_interval": 10
    }
}
```

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| enabled | boolean | No | true | Enable checkpoint persistence |
| file | string | No | binlog_checkpoint.dat | Checkpoint file path |
| save_interval | integer | No | 10 | Save checkpoint every N events |

**Checkpoints allow resuming from last position after restart.**

### 4. Publishers

```json
{
    "publishers": [
        {
            "name": "publisher_name",
            "enabled": true,
            "plugin_path": "./build/lib/plugin.so",
            "config": {
                "key": "value"
            }
        }
    ]
}
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| name | string | Yes | Unique publisher identifier |
| enabled | boolean | No | Enable/disable publisher |
| plugin_path | string | Yes | Path to publisher plugin (.so file) |
| config | object | Yes | Publisher-specific configuration |

## Publisher-Specific Configuration

### File Publisher

```json
{
    "plugin_path": "./build/lib/file_publisher.so",
    "config": {
        "output_file": "/var/log/binlog/events.json",
        "rotate_size": 104857600,
        "max_files": 10,
        "append": true
    }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| output_file | string | Required | Output file path |
| rotate_size | integer | 100MB | Rotate when file reaches size (bytes) |
| max_files | integer | 5 | Maximum number of rotated files |
| append | boolean | true | Append to existing file |

### ZeroMQ Publisher

```json
{
    "plugin_path": "./build/lib/zmq_publisher.so",
    "config": {
        "endpoint": "tcp://*:5555",
        "socket_type": "PUB",
        "high_water_mark": 1000
    }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| endpoint | string | Required | ZeroMQ endpoint (tcp://host:port) |
| socket_type | string | PUB | Socket type (PUB, PUSH) |
| high_water_mark | integer | 1000 | Message queue high water mark |

### Kafka Publisher

```json
{
    "plugin_path": "./build/lib/kafka_publisher.so",
    "config": {
        "brokers": "localhost:9092",
        "topic": "binlog.events",
        "partition": -1,
        "compression": "snappy",
        "batch_size": 100
    }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| brokers | string | Required | Kafka broker list (comma-separated) |
| topic | string | Required | Target topic name |
| partition | integer | -1 | Target partition (-1 for auto) |
| compression | string | none | Compression type (none, gzip, snappy, lz4) |
| batch_size | integer | 100 | Batch size for bulk sends |

### Redis Publisher

```json
{
    "plugin_path": "./build/lib/redis_publisher.so",
    "config": {
        "host": "localhost",
        "port": 6379,
        "password": "",
        "mode": "stream",
        "key": "binlog:events",
        "maxlen": 10000
    }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| host | string | localhost | Redis server hostname |
| port | integer | 6379 | Redis server port |
| password | string | "" | Redis password (if any) |
| mode | string | stream | Mode: stream, pubsub, or list |
| key | string | Required | Redis key/channel name |
| maxlen | integer | 10000 | Max stream length (stream mode only) |

### Webhook Publisher

```json
{
    "plugin_path": "./build/lib/webhook_publisher.so",
    "config": {
        "url": "http://example.com/webhook",
        "method": "POST",
        "timeout": 5,
        "retry_count": 3,
        "headers": {
            "Authorization": "Bearer token123",
            "Content-Type": "application/json"
        }
    }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| url | string | Required | Webhook URL |
| method | string | POST | HTTP method (POST, PUT) |
| timeout | integer | 5 | Request timeout (seconds) |
| retry_count | integer | 3 | Number of retries on failure |
| headers | object | {} | Custom HTTP headers |

### Syslog Publisher

```json
{
    "plugin_path": "./build/lib/syslog_publisher.so",
    "config": {
        "facility": "LOG_LOCAL0",
        "priority": "LOG_INFO",
        "ident": "binlog_stream",
        "option": "LOG_PID"
    }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| facility | string | LOG_LOCAL0 | Syslog facility |
| priority | string | LOG_INFO | Log priority level |
| ident | string | binlog_stream | Syslog identifier |
| option | string | LOG_PID | Syslog options |

### Lua Publisher

```json
{
    "plugin_path": "./build/lib/lua_publisher.so",
    "config": {
        "script_path": "scripts/plugin-examples/publish.lua",
        "function_name": "publish_event"
    }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| script_path | string | Required | Path to Lua script |
| function_name | string | publish_event | Lua function to call |

### Python Publisher

```json
{
    "plugin_path": "./build/lib/python_publisher.so",
    "config": {
        "script_path": "scripts/plugin-examples/publish.py",
        "module_name": "publish",
        "function_name": "publish_event"
    }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| script_path | string | Required | Path to Python script |
| module_name | string | Required | Python module name |
| function_name | string | publish_event | Python function to call |

### Java Publisher

```json
{
    "plugin_path": "./build/lib/java_publisher.so",
    "config": {
        "class_path": "scripts/plugin-examples",
        "class_name": "JavaPublisher",
        "method_name": "publishEvent"
    }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| class_path | string | Required | Java classpath |
| class_name | string | Required | Java class name |
| method_name | string | publishEvent | Java method to call |

### 5. Logging

```json
{
    "logging": {
        "level": "INFO",
        "file": "/var/log/binlog/streamer.log",
        "max_size": 10485760,
        "max_files": 5,
        "console": true
    }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| level | string | INFO | Log level (DEBUG, INFO, WARN, ERROR) |
| file | string | - | Log file path |
| max_size | integer | 10MB | Log file max size (bytes) |
| max_files | integer | 5 | Number of rotated log files |
| console | boolean | true | Also log to console |

## Environment Variables

Override configuration with environment variables:

```bash
export BINLOG_MYSQL_HOST=localhost
export BINLOG_MYSQL_PORT=3306
export BINLOG_MYSQL_USER=binlog_user
export BINLOG_MYSQL_PASSWORD=secure_password
export BINLOG_LOG_LEVEL=DEBUG
```

## Configuration Validation

Test your configuration:

```bash
# Validate config syntax
python3 -m json.tool config/config.json

# Test run with verbose output
./build/bin/binlog_stream config/config.json --validate

# Dry run (don't actually stream)
./build/bin/binlog_stream config/config.json --dry-run
```

## Performance Tuning

### High-Volume Environments

```json
{
    "checkpoint": {
        "save_interval": 100
    },
    "publishers": [
        {
            "config": {
                "batch_size": 1000,
                "buffer_size": 65536
            }
        }
    ]
}
```

### Low-Latency Requirements

```json
{
    "checkpoint": {
        "save_interval": 1
    },
    "publishers": [
        {
            "config": {
                "batch_size": 1,
                "flush_immediate": true
            }
        }
    ]
}
```

## Security Best Practices

1. **Secure Credentials:**
   - Use environment variables for passwords
   - Set file permissions: `chmod 600 config/config.json`
   - Consider using vault solutions

2. **Network Security:**
   - Use TLS for MySQL connections
   - Encrypt publisher connections when possible
   - Restrict MySQL user privileges

3. **File Permissions:**
   ```bash
   chown binlog:binlog config/config.json
   chmod 600 config/config.json
   ```

## Troubleshooting

### Connection Issues
- Verify MySQL user privileges
- Check network connectivity
- Confirm binlog is enabled: `SHOW VARIABLES LIKE 'log_bin';`

### Performance Issues
- Increase checkpoint save_interval
- Enable batch mode for publishers
- Filter unnecessary columns
- Monitor system resources

### Plugin Loading Errors
- Verify plugin path is correct
- Check plugin dependencies: `ldd build/lib/plugin.so`
- Review plugin configuration syntax

## Example Configurations

See `config/` directory for complete examples:
- `config.json` - Production configuration
- `config_with_plugins.json` - All publishers enabled

## Migration from Old Configuration

If upgrading from flat structure:

```bash
./scripts/update_config_paths.sh
```

This updates plugin paths from `./plugin.so` to `./build/lib/plugin.so`.
