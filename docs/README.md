# Binlog Streamer

A high-performance MySQL binlog streaming application with a flexible plugin architecture.

## Features

- Real-time MySQL binlog streaming
- Plugin-based publisher architecture
- Support for multiple output formats (File, ZeroMQ, Kafka, Redis, Webhooks, Syslog)
- Scriptable publishers (Lua, Python, Java)
- Column-level filtering
- Multi-publisher support
- Checkpoint management

## Directory Structure

```
binlog-streamer/
├── src/                # Source code
│   ├── core/          # Core application
│   ├── plugins/       # Publisher plugins
│   └── include/       # Header files
├── build/             # Build artifacts (generated)
├── config/            # Configuration files
├── scripts/           # Helper scripts and examples
├── data/              # Runtime data (generated)
├── docs/              # Documentation
└── tests/             # Test files
```

## Building

```bash
make all           # Build everything
make clean         # Clean build artifacts
make install       # Install system-wide
```

## Running

```bash
./build/bin/binlog_stream config/config.json
```

## Plugin Development

See [PLUGIN_DEVELOPMENT.md](PLUGIN_DEVELOPMENT.md) for details on creating custom publishers.

## Configuration

See [CONFIGURATION.md](CONFIGURATION.md) for configuration options.
