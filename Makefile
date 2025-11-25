# Makefile for Binlog Streamer with Plugin System
# Updated for organized folder structure

CC = gcc
CFLAGS = -O2 -Wall -fPIC -Isrc/include
CFLAGS += -DDEVELOPER_MODE 
CFLAGS += -DLOG_USE_COLOR
GIT_HASH  := $(shell git rev-parse --short HEAD)
BUILD_TS  := $(shell date +%Y-%m-%dT%H:%M:%S)
CFLAGS += -DBINLOG_STREAMER_VERSION=\"1.0.0\" \
          -DBINLOG_STREAMER_BUILD=\"$(BUILD_TS)-$(GIT_HASH)\"
CFLAGS += -DBANNER_STYLE=2

LDFLAGS = -rdynamic -lmysqlclient -lz -luuid -ljson-c -lpthread -ldl

# Directory structure
SRC_DIR = src
CORE_DIR = $(SRC_DIR)/core
PLUGIN_DIR = $(SRC_DIR)/plugins
INCLUDE_DIR = $(SRC_DIR)/include
BUILD_DIR = build
BIN_DIR = $(BUILD_DIR)/bin
OBJ_DIR = $(BUILD_DIR)/obj
LIB_DIR = $(BUILD_DIR)/lib
CONFIG_DIR = config
SCRIPTS_DIR = scripts
DATA_DIR = data

# Lua configuration
LUA_VERSION ?= 5.3
LUA_CFLAGS = $(shell pkg-config --cflags lua$(LUA_VERSION) 2>/dev/null || echo "-I/usr/include/lua$(LUA_VERSION)")
LUA_LIBS = $(shell pkg-config --libs lua$(LUA_VERSION) 2>/dev/null || echo "-llua$(LUA_VERSION)")

# Python configuration
PYTHON_VERSION ?= 3.10
PYTHON_CONFIG ?= python$(PYTHON_VERSION)-config
PYTHON_CFLAGS = $(shell $(PYTHON_CONFIG) --includes 2>/dev/null || echo "-I/usr/include/python$(PYTHON_VERSION)")
PYTHON_LIBS = $(shell $(PYTHON_CONFIG) --ldflags --embed 2>/dev/null || $(PYTHON_CONFIG) --ldflags 2>/dev/null || echo "-lpython$(PYTHON_VERSION)")

# Java configuration
JAVA_BIN := $(shell which java 2>/dev/null)
ifneq ($(JAVA_BIN),)
    JAVA_HOME := $(shell readlink -f $(JAVA_BIN) | sed 's:/bin/java::')
else
    JAVA_HOME := /usr/lib/jvm/default-java
endif

JAVA_CFLAGS = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
JAVA_LIBS = -L$(JAVA_HOME)/lib/server -ljvm -Wl,-rpath,$(JAVA_HOME)/lib/server

# Core application
CORE_TARGET = $(BIN_DIR)/binlog_stream
CORE_SOURCES = $(CORE_DIR)/binlog_stream_modular.c \
               $(CORE_DIR)/publisher_loader.c \
               $(CORE_DIR)/logger.c \
	       $(CORE_DIR)/banner.c
CORE_OBJECTS = $(patsubst $(CORE_DIR)/%.c,$(OBJ_DIR)/core/%.o,$(CORE_SOURCES))

# Publisher plugins
PLUGIN_NAMES = file_publisher zmq_publisher kafka_publisher example_publisher \
               webhook_publisher syslog_publisher redis_publisher lua_publisher \
               python_publisher java_publisher udp_publisher
PLUGIN_TARGETS = $(addprefix $(LIB_DIR)/,$(addsuffix .so,$(PLUGIN_NAMES)))
JAVA_CLASS = $(SCRIPTS_DIR)/plugin-examples/JavaPublisher.class

# Default target
all: directories $(CORE_TARGET) $(PLUGIN_TARGETS) $(JAVA_CLASS)

# Create directory structure
directories:
	@mkdir -p $(BIN_DIR) $(OBJ_DIR)/core $(OBJ_DIR)/plugins $(LIB_DIR) $(DATA_DIR)

# Build core application
$(CORE_TARGET): $(CORE_OBJECTS)
	$(CC) -o $@ $^ $(LDFLAGS)
	@echo "Built core application: $@"

# Core object files
$(OBJ_DIR)/core/%.o: $(CORE_DIR)/%.c
	@mkdir -p $(OBJ_DIR)/core
	$(CC) $(CFLAGS) -c -o $@ $<

# Publisher plugins
$(LIB_DIR)/file_publisher.so: $(PLUGIN_DIR)/file_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $<
	@echo "Built plugin: $@"

$(LIB_DIR)/zmq_publisher.so: $(PLUGIN_DIR)/zmq_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $< -lzmq
	@echo "Built plugin: $@"

$(LIB_DIR)/kafka_publisher.so: $(PLUGIN_DIR)/kafka_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $< -lrdkafka
	@echo "Built plugin: $@"

$(LIB_DIR)/webhook_publisher.so: $(PLUGIN_DIR)/webhook_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $< -lcurl
	@echo "Built plugin: $@"

$(LIB_DIR)/syslog_publisher.so: $(PLUGIN_DIR)/syslog_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $<
	@echo "Built plugin: $@"

$(LIB_DIR)/redis_publisher.so: $(PLUGIN_DIR)/redis_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $< -lhiredis
	@echo "Built plugin: $@"

$(LIB_DIR)/lua_publisher.so: $(PLUGIN_DIR)/lua_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) $(LUA_CFLAGS) -shared -o $@ $< $(LUA_LIBS)
	@echo "Built plugin: $@"

$(LIB_DIR)/python_publisher.so: $(PLUGIN_DIR)/python_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) $(PYTHON_CFLAGS) -shared -o $@ $< $(PYTHON_LIBS)
	@echo "Built plugin: $@"

$(LIB_DIR)/java_publisher.so: $(PLUGIN_DIR)/java_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) $(JAVA_CFLAGS) -shared -o $@ $< $(JAVA_LIBS)
	@echo "Built plugin: $@"

$(LIB_DIR)/example_publisher.so: $(PLUGIN_DIR)/example_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $<
	@echo "Built plugin: $@"
	
$(LIB_DIR)/udp_publisher.so: $(PLUGIN_DIR)/udp_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $<
	@echo "Built plugin: $@"

# Java publisher class
$(JAVA_CLASS): $(SCRIPTS_DIR)/plugin-examples/JavaPublisher.java
	cd $(SCRIPTS_DIR)/plugin-examples && javac JavaPublisher.java
	@echo "Compiled Java publisher class"

# Clean
clean:
	rm -rf $(BUILD_DIR)
	rm -f $(JAVA_CLASS)
	@echo "Cleaned build artifacts"

# Clean runtime data
clean-data:
	rm -rf $(DATA_DIR)/*
	@echo "Cleaned runtime data"

# Full clean
distclean: clean clean-data
	@echo "Full clean completed"

# Install plugins to system location
install-plugins: $(PLUGIN_TARGETS)
	mkdir -p /usr/local/lib/binlog_publishers
	cp $(PLUGIN_TARGETS) /usr/local/lib/binlog_publishers/
	@echo "Installed plugins to /usr/local/lib/binlog_publishers/"

# Install application
install: all install-plugins
	mkdir -p /usr/local/bin
	mkdir -p /etc/binlog_streamer
	cp $(CORE_TARGET) /usr/local/bin/
	cp -n $(CONFIG_DIR)/*.json /etc/binlog_streamer/ 2>/dev/null || true
	@echo "Installed binlog_stream to /usr/local/bin/"
	@echo "Installed configs to /etc/binlog_streamer/"

# Uninstall
uninstall:
	rm -f /usr/local/bin/binlog_stream
	rm -rf /usr/local/lib/binlog_publishers
	@echo "Uninstalled binlog_stream and plugins"

# Test individual plugins
test-lua: $(LIB_DIR)/lua_publisher.so
	@echo "Testing Lua publisher..."
	@nm -D $(LIB_DIR)/lua_publisher.so | grep -E "(publisher_plugin_create|lua)" | head -10

test-python: $(LIB_DIR)/python_publisher.so
	@echo "Testing Python publisher..."
	@nm -D $(LIB_DIR)/python_publisher.so | grep -E "(publisher_plugin_create|Py_)" | head -10

test-plugins: $(PLUGIN_TARGETS)
	@echo "Checking all plugins for required symbols..."
	@for plugin in $(PLUGIN_TARGETS); do \
		echo "$$(basename $$plugin):"; \
		nm -D $$plugin | grep publisher_plugin_create || echo "  ERROR: Missing publisher_plugin_create"; \
	done

# Run application (for testing)
run: all
	@echo "Running binlog_stream with config/config.json..."
	cd $(BUILD_DIR) && $(BIN_DIR)/binlog_stream $(realpath $(CONFIG_DIR)/config.json)

# Show build configuration
config:
	@echo "Build Configuration:"
	@echo "  CC: $(CC)"
	@echo "  CFLAGS: $(CFLAGS)"
	@echo "  LDFLAGS: $(LDFLAGS)"
	@echo ""
	@echo "Directory Structure:"
	@echo "  Source: $(SRC_DIR)"
	@echo "  Build: $(BUILD_DIR)"
	@echo "  Binary: $(BIN_DIR)"
	@echo "  Libraries: $(LIB_DIR)"
	@echo "  Config: $(CONFIG_DIR)"
	@echo ""
	@echo "Lua Configuration:"
	@echo "  Version: $(LUA_VERSION)"
	@echo "  CFLAGS: $(LUA_CFLAGS)"
	@echo "  LIBS: $(LUA_LIBS)"
	@echo ""
	@echo "Python Configuration:"
	@echo "  Version: $(PYTHON_VERSION)"
	@echo "  Config: $(PYTHON_CONFIG)"
	@echo "  CFLAGS: $(PYTHON_CFLAGS)"
	@echo "  LIBS: $(PYTHON_LIBS)"
	@echo ""
	@echo "Java Configuration:"
	@echo "  JAVA_HOME: $(JAVA_HOME)"
	@echo "  CFLAGS: $(JAVA_CFLAGS)"

# Show current structure
tree:
	@echo "Current directory structure:"
	@tree -L 3 -I '__pycache__|*.pyc|nbproject' || ls -R

# Help
help:
	@echo "Binlog Streamer Build System"
	@echo ""
	@echo "Targets:"
	@echo "  all              - Build core application and all plugins (default)"
	@echo "  $(CORE_TARGET)   - Build core application only"
	@echo "  directories      - Create build directory structure"
	@echo "  clean            - Remove build artifacts"
	@echo "  clean-data       - Remove runtime data"
	@echo "  distclean        - Full clean (build + data)"
	@echo "  install          - Install application and plugins system-wide"
	@echo "  install-plugins  - Install plugins to system location"
	@echo "  uninstall        - Uninstall application and plugins"
	@echo "  run              - Build and run with default config"
	@echo "  test-plugins     - Test all plugins for required symbols"
	@echo "  test-lua         - Test Lua publisher"
	@echo "  test-python      - Test Python publisher"
	@echo "  config           - Show build configuration"
	@echo "  tree             - Show directory structure"
	@echo "  help             - Show this help"
	@echo ""
	@echo "Variables:"
	@echo "  LUA_VERSION      - Lua version (default: $(LUA_VERSION))"
	@echo "  PYTHON_VERSION   - Python version (default: $(PYTHON_VERSION))"
	@echo "  JAVA_HOME        - Java home directory (default: $(JAVA_HOME))"

.PHONY: all directories clean clean-data distclean install install-plugins \
        uninstall run test-lua test-python test-plugins config tree help
