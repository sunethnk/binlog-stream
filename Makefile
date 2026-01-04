# Makefile for CDC Streamers (MySQL & PostgreSQL) with Plugin System
# Updated for organized folder structure with dual database support

CC = gcc
CFLAGS = -g -O0 -Wall -fPIC -Isrc/include
CFLAGS += -DDEVELOPER_MODE 
CFLAGS += -DLOG_USE_COLOR
GIT_HASH  := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TS  := $(shell date +%Y-%m-%dT%H:%M:%S)
CFLAGS += -DBINLOG_STREAMER_VERSION=\"1.0.0\" \
          -DBINLOG_STREAMER_BUILD=\"$(BUILD_TS)-$(GIT_HASH)\"
CFLAGS += -DBANNER_STYLE=2

# Base library flags
BASE_LDFLAGS = -rdynamic -lz -luuid -ljson-c -lpthread -ldl

# MySQL-specific flags
MYSQL_LDFLAGS = $(BASE_LDFLAGS) -lmysqlclient
MYSQL_CFLAGS = $(shell mysql_config --cflags 2>/dev/null || echo "")

# PostgreSQL-specific flags
PG_INCLUDE = $(shell pg_config --includedir 2>/dev/null || echo "/usr/include/postgresql")
PG_CFLAGS = -I$(PG_INCLUDE)
PG_LDFLAGS = $(BASE_LDFLAGS) -lpq

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

# ============================================================================
# MySQL BINLOG STREAMER
# ============================================================================
MYSQL_TARGET = $(BIN_DIR)/binlog_stream
MYSQL_SOURCES = $(CORE_DIR)/binlog_stream_modular.c
MYSQL_COMMON = $(CORE_DIR)/publisher_loader.c \
               $(CORE_DIR)/logger.c \
               $(CORE_DIR)/banner.c
MYSQL_OBJECTS = $(patsubst $(CORE_DIR)/%.c,$(OBJ_DIR)/mysql/%.o,$(MYSQL_SOURCES) $(MYSQL_COMMON))

# ============================================================================
# POSTGRESQL LOGICAL REPLICATION STREAMER
# ============================================================================
PG_TARGET = $(BIN_DIR)/pg_stream
PG_SOURCES = $(CORE_DIR)/pg_stream_modular.c
PG_COMMON = $(CORE_DIR)/publisher_loader.c \
            $(CORE_DIR)/logger.c \
            $(CORE_DIR)/banner.c
PG_OBJECTS = $(patsubst $(CORE_DIR)/%.c,$(OBJ_DIR)/pg/%.o,$(PG_SOURCES) $(PG_COMMON))

# ============================================================================
# PUBLISHER PLUGINS (Shared by both MySQL and PostgreSQL)
# ============================================================================
PLUGIN_NAMES = file_publisher zmq_publisher kafka_publisher example_publisher \
               webhook_publisher syslog_publisher redis_publisher lua_publisher \
               python_publisher java_publisher udp_publisher
PLUGIN_TARGETS = $(addprefix $(LIB_DIR)/,$(addsuffix .so,$(PLUGIN_NAMES)))
JAVA_CLASS = $(SCRIPTS_DIR)/plugin-examples/JavaPublisher.class

# ============================================================================
# DEFAULT TARGET
# ============================================================================
all: directories mysql postgres $(PLUGIN_TARGETS) $(JAVA_CLASS)

mysql: directories $(MYSQL_TARGET)

postgres: directories $(PG_TARGET)

# Create directory structure
directories:
	@mkdir -p $(BIN_DIR) $(OBJ_DIR)/mysql $(OBJ_DIR)/pg $(OBJ_DIR)/plugins $(LIB_DIR) $(DATA_DIR)

# ============================================================================
# MYSQL BUILD RULES
# ============================================================================
$(MYSQL_TARGET): $(MYSQL_OBJECTS)
	$(CC) -o $@ $^ $(MYSQL_LDFLAGS)
	@echo "✓ Built MySQL CDC streamer: $@"

# MySQL object files
$(OBJ_DIR)/mysql/%.o: $(CORE_DIR)/%.c
	@mkdir -p $(OBJ_DIR)/mysql
	$(CC) $(CFLAGS) $(MYSQL_CFLAGS) -c -o $@ $<

# ============================================================================
# POSTGRESQL BUILD RULES
# ============================================================================
$(PG_TARGET): $(PG_OBJECTS)
	$(CC) -o $@ $^ $(PG_LDFLAGS)
	@echo "✓ Built PostgreSQL CDC streamer: $@"

# PostgreSQL object files
$(OBJ_DIR)/pg/%.o: $(CORE_DIR)/%.c
	@mkdir -p $(OBJ_DIR)/pg
	$(CC) $(CFLAGS) $(PG_CFLAGS) -c -o $@ $<

# ============================================================================
# PUBLISHER PLUGINS
# ============================================================================
$(LIB_DIR)/file_publisher.so: $(PLUGIN_DIR)/file_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $<
	@echo "✓ Built plugin: $@"

$(LIB_DIR)/zmq_publisher.so: $(PLUGIN_DIR)/zmq_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $< -lzmq
	@echo "✓ Built plugin: $@"

$(LIB_DIR)/kafka_publisher.so: $(PLUGIN_DIR)/kafka_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $< -lrdkafka
	@echo "✓ Built plugin: $@"

$(LIB_DIR)/webhook_publisher.so: $(PLUGIN_DIR)/webhook_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $< -lcurl
	@echo "✓ Built plugin: $@"

$(LIB_DIR)/syslog_publisher.so: $(PLUGIN_DIR)/syslog_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $<
	@echo "✓ Built plugin: $@"

$(LIB_DIR)/redis_publisher.so: $(PLUGIN_DIR)/redis_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $< -lhiredis
	@echo "✓ Built plugin: $@"

$(LIB_DIR)/lua_publisher.so: $(PLUGIN_DIR)/lua_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) $(LUA_CFLAGS) -shared -o $@ $< $(LUA_LIBS)
	@echo "✓ Built plugin: $@"

$(LIB_DIR)/python_publisher.so: $(PLUGIN_DIR)/python_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) $(PYTHON_CFLAGS) -shared -o $@ $< $(PYTHON_LIBS)
	@echo "✓ Built plugin: $@"

$(LIB_DIR)/java_publisher.so: $(PLUGIN_DIR)/java_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) $(JAVA_CFLAGS) -shared -o $@ $< $(JAVA_LIBS)
	@echo "✓ Built plugin: $@"

$(LIB_DIR)/example_publisher.so: $(PLUGIN_DIR)/example_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $<
	@echo "✓ Built plugin: $@"
	
$(LIB_DIR)/udp_publisher.so: $(PLUGIN_DIR)/udp_publisher.c $(INCLUDE_DIR)/publisher_api.h
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) -shared -o $@ $<
	@echo "✓ Built plugin: $@"

# Java publisher class
$(JAVA_CLASS): $(SCRIPTS_DIR)/plugin-examples/JavaPublisher.java
	cd $(SCRIPTS_DIR)/plugin-examples && javac JavaPublisher.java
	@echo "✓ Compiled Java publisher class"

# ============================================================================
# CLEAN TARGETS
# ============================================================================
clean:
	rm -rf $(BUILD_DIR)
	rm -f $(JAVA_CLASS)
	@echo "✓ Cleaned build artifacts"

clean-data:
	rm -rf $(DATA_DIR)/*
	@echo "✓ Cleaned runtime data"

distclean: clean clean-data
	@echo "✓ Full clean completed"

# ============================================================================
# INSTALLATION TARGETS
# ============================================================================
# Install plugins to system location
install-plugins: $(PLUGIN_TARGETS)
	mkdir -p /usr/local/lib/cdc_publishers
	cp $(PLUGIN_TARGETS) /usr/local/lib/cdc_publishers/
	@echo "✓ Installed plugins to /usr/local/lib/cdc_publishers/"

# Install both MySQL and PostgreSQL CDC streamers
install: all install-plugins
	mkdir -p /usr/local/bin
	mkdir -p /etc/cdc_streamer
	cp $(MYSQL_TARGET) /usr/local/bin/
	cp $(PG_TARGET) /usr/local/bin/
	cp -n $(CONFIG_DIR)/*.json /etc/cdc_streamer/ 2>/dev/null || true
	@echo "✓ Installed binlog_stream to /usr/local/bin/"
	@echo "✓ Installed pg_stream to /usr/local/bin/"
	@echo "✓ Installed configs to /etc/cdc_streamer/"

# Install only MySQL version
install-mysql: mysql install-plugins
	mkdir -p /usr/local/bin
	mkdir -p /etc/cdc_streamer
	cp $(MYSQL_TARGET) /usr/local/bin/
	cp -n $(CONFIG_DIR)/config*.json /etc/cdc_streamer/ 2>/dev/null || true
	@echo "✓ Installed binlog_stream to /usr/local/bin/"

# Install only PostgreSQL version
install-postgres: postgres install-plugins
	mkdir -p /usr/local/bin
	mkdir -p /etc/cdc_streamer
	cp $(PG_TARGET) /usr/local/bin/
	cp -n $(CONFIG_DIR)/pg_*.json /etc/cdc_streamer/ 2>/dev/null || true
	@echo "✓ Installed pg_stream to /usr/local/bin/"

# Uninstall
uninstall:
	rm -f /usr/local/bin/binlog_stream
	rm -f /usr/local/bin/pg_stream
	rm -rf /usr/local/lib/cdc_publishers
	@echo "✓ Uninstalled CDC streamers and plugins"

# ============================================================================
# TESTING TARGETS
# ============================================================================
test-plugins: $(PLUGIN_TARGETS)
	@echo "Checking all plugins for required symbols..."
	@for plugin in $(PLUGIN_TARGETS); do \
		echo "$$(basename $$plugin):"; \
		nm -D $$plugin | grep publisher_plugin_create || echo "  ERROR: Missing publisher_plugin_create"; \
	done

test-lua: $(LIB_DIR)/lua_publisher.so
	@echo "Testing Lua publisher..."
	@nm -D $(LIB_DIR)/lua_publisher.so | grep -E "(publisher_plugin_create|lua)" | head -10

test-python: $(LIB_DIR)/python_publisher.so
	@echo "Testing Python publisher..."
	@nm -D $(LIB_DIR)/python_publisher.so | grep -E "(publisher_plugin_create|Py_)" | head -10

# Test MySQL CDC streamer
test-mysql: mysql
	@echo "Testing MySQL CDC streamer..."
	@if [ -f $(CONFIG_DIR)/config.json ]; then \
		echo "Found config file, running streamer..."; \
		cd $(BUILD_DIR) && timeout 5 $(BIN_DIR)/binlog_stream $(realpath $(CONFIG_DIR)/config.json) || echo "Test complete (timeout expected)"; \
	else \
		echo "No config file found at $(CONFIG_DIR)/config.json"; \
		echo "Run with: $(BIN_DIR)/binlog_stream <config.json>"; \
	fi

# Test PostgreSQL CDC streamer
test-postgres: postgres
	@echo "Testing PostgreSQL CDC streamer..."
	@if [ -f $(CONFIG_DIR)/pg_config.json ]; then \
		echo "Found config file, running streamer..."; \
		cd $(BUILD_DIR) && timeout 5 $(BIN_DIR)/pg_stream $(realpath $(CONFIG_DIR)/pg_config.json) || echo "Test complete (timeout expected)"; \
	else \
		echo "No config file found at $(CONFIG_DIR)/pg_config.json"; \
		echo "Run with: $(BIN_DIR)/pg_stream <pg_config.json>"; \
	fi

# ============================================================================
# RUN TARGETS
# ============================================================================
# Run MySQL CDC streamer
run-mysql: mysql
	@echo "Running MySQL CDC streamer with $(CONFIG_DIR)/config.json..."
	@if [ ! -f $(CONFIG_DIR)/config.json ]; then \
		echo "ERROR: Config file not found: $(CONFIG_DIR)/config.json"; \
		exit 1; \
	fi
	cd $(BUILD_DIR) && $(BIN_DIR)/binlog_stream $(realpath $(CONFIG_DIR)/config.json)

# Run PostgreSQL CDC streamer
run-postgres: postgres
	@echo "Running PostgreSQL CDC streamer with $(CONFIG_DIR)/pg_config.json..."
	@if [ ! -f $(CONFIG_DIR)/pg_config.json ]; then \
		echo "ERROR: Config file not found: $(CONFIG_DIR)/pg_config.json"; \
		exit 1; \
	fi
	cd $(BUILD_DIR) && $(BIN_DIR)/pg_stream $(realpath $(CONFIG_DIR)/pg_config.json)

# Legacy run target (defaults to MySQL for backward compatibility)
run: run-mysql

# ============================================================================
# CONFIGURATION & INFO TARGETS
# ============================================================================
config:
	@echo "═══════════════════════════════════════════════════════════"
	@echo "CDC Streamers Build Configuration"
	@echo "═══════════════════════════════════════════════════════════"
	@echo ""
	@echo "Build Information:"
	@echo "  Version:     1.0.0"
	@echo "  Build:       $(BUILD_TS)-$(GIT_HASH)"
	@echo "  CC:          $(CC)"
	@echo "  Base CFLAGS: $(CFLAGS)"
	@echo ""
	@echo "MySQL Configuration:"
	@echo "  Available:   $(shell mysql_config --version 2>/dev/null && echo 'Yes' || echo 'No')"
	@echo "  CFLAGS:      $(MYSQL_CFLAGS)"
	@echo "  LDFLAGS:     $(MYSQL_LDFLAGS)"
	@echo "  Target:      $(MYSQL_TARGET)"
	@echo ""
	@echo "PostgreSQL Configuration:"
	@echo "  Available:   $(shell pg_config --version 2>/dev/null && echo 'Yes' || echo 'No')"
	@echo "  Include:     $(PG_INCLUDE)"
	@echo "  CFLAGS:      $(PG_CFLAGS)"
	@echo "  LDFLAGS:     $(PG_LDFLAGS)"
	@echo "  Target:      $(PG_TARGET)"
	@echo ""
	@echo "Directory Structure:"
	@echo "  Source:      $(SRC_DIR)"
	@echo "  Core:        $(CORE_DIR)"
	@echo "  Plugins:     $(PLUGIN_DIR)"
	@echo "  Build:       $(BUILD_DIR)"
	@echo "  Binary:      $(BIN_DIR)"
	@echo "  Libraries:   $(LIB_DIR)"
	@echo "  Config:      $(CONFIG_DIR)"
	@echo ""
	@echo "Lua Configuration:"
	@echo "  Version:     $(LUA_VERSION)"
	@echo "  CFLAGS:      $(LUA_CFLAGS)"
	@echo "  LIBS:        $(LUA_LIBS)"
	@echo ""
	@echo "Python Configuration:"
	@echo "  Version:     $(PYTHON_VERSION)"
	@echo "  Config:      $(PYTHON_CONFIG)"
	@echo "  CFLAGS:      $(PYTHON_CFLAGS)"
	@echo "  LIBS:        $(PYTHON_LIBS)"
	@echo ""
	@echo "Java Configuration:"
	@echo "  JAVA_HOME:   $(JAVA_HOME)"
	@echo "  CFLAGS:      $(JAVA_CFLAGS)"
	@echo ""
	@echo "Plugin Count:  $(words $(PLUGIN_NAMES))"
	@echo "Plugins:       $(PLUGIN_NAMES)"
	@echo "═══════════════════════════════════════════════════════════"

# Check database client availability
check-deps:
	@echo "Checking database client availability..."
	@echo -n "MySQL:      "
	@command -v mysql_config >/dev/null 2>&1 && echo "✓ Found ($$(mysql_config --version))" || echo "✗ Not found"
	@echo -n "PostgreSQL: "
	@command -v pg_config >/dev/null 2>&1 && echo "✓ Found ($$(pg_config --version))" || echo "✗ Not found"
	@echo ""
	@echo "Checking library dependencies..."
	@echo -n "json-c:     "
	@pkg-config --exists json-c && echo "✓ Found" || echo "✗ Not found"
	@echo -n "uuid:       "
	@pkg-config --exists uuid && echo "✓ Found" || echo "✗ Not found"
	@echo -n "zlib:       "
	@pkg-config --exists zlib && echo "✓ Found" || echo "✗ Not found"
	@echo -n "ZeroMQ:     "
	@pkg-config --exists libzmq && echo "✓ Found" || echo "✗ Not found"
	@echo -n "librdkafka: "
	@pkg-config --exists rdkafka && echo "✓ Found" || echo "✗ Not found"
	@echo -n "libcurl:    "
	@pkg-config --exists libcurl && echo "✓ Found" || echo "✗ Not found"
	@echo -n "hiredis:    "
	@pkg-config --exists hiredis && echo "✓ Found" || echo "✗ Not found"

# Install build dependencies (Ubuntu/RHEL/Debian/Arch)
install-deps:
	@echo "═══════════════════════════════════════════════════════════"
	@echo "Installing CDC Streamers Build Dependencies"
	@echo "═══════════════════════════════════════════════════════════"
	@if [ -f /etc/os-release ]; then \
	  . /etc/os-release; \
	  echo "Detected OS: $$ID ($$NAME $$VERSION_ID)"; \
	  echo ""; \
	  case "$$ID" in \
	    ubuntu|debian) \
	      echo "==> Installing packages for Ubuntu/Debian..."; \
	      sudo apt-get update && sudo apt-get install -y \
	        build-essential git pkg-config \
	        default-libmysqlclient-dev postgresql-server-dev-all \
	        libjson-c-dev zlib1g-dev uuid-dev \
	        liblua5.3-dev python3-dev openjdk-17-jdk \
	        libzmq3-dev librdkafka-dev libcurl4-openssl-dev libhiredis-dev \
	        tree; \
	      echo ""; \
	      echo "✓ Dependencies installed successfully"; \
	      ;; \
	    rhel|centos|rocky|almalinux|ol|fedora) \
	      echo "==> Installing packages for RHEL/CentOS/Rocky/Alma/Fedora..."; \
	      sudo dnf install -y epel-release || sudo yum install -y epel-release || true; \
	      sudo dnf install -y \
	        gcc gcc-c++ make git pkgconfig \
	        mariadb-connector-c-devel postgresql-devel \
	        json-c-devel zlib-devel libuuid-devel \
	        lua-devel python3-devel java-11-openjdk-devel \
	        zeromq-devel librdkafka-devel libcurl-devel hiredis-devel \
	        tree || \
	      sudo yum install -y \
	        gcc gcc-c++ make git pkgconfig \
	        mariadb-connector-c-devel postgresql-devel \
	        json-c-devel zlib-devel libuuid-devel \
	        lua-devel python3-devel java-11-openjdk-devel \
	        zeromq-devel librdkafka-devel libcurl-devel hiredis-devel \
	        tree; \
	      echo ""; \
	      echo "✓ Dependencies installed successfully"; \
	      ;; \
	    arch|manjaro) \
	      echo "==> Installing packages for Arch/Manjaro..."; \
	      sudo pacman -Sy --needed \
	        base-devel git \
	        mariadb-libs postgresql-libs \
	        json-c zlib util-linux-libs \
	        lua python jdk-openjdk \
	        zeromq librdkafka curl hiredis \
	        tree; \
	      echo ""; \
	      echo "✓ Dependencies installed successfully"; \
	      ;; \
	    *) \
	      echo "!! Unsupported or unknown distro ID: $$ID"; \
	      echo ""; \
	      echo "Please install the following build dependencies manually:"; \
	      echo ""; \
	      echo "Core Tools:"; \
	      echo "  - Compiler & tools: gcc/g++, make, git, pkg-config"; \
	      echo ""; \
	      echo "Database Clients:"; \
	      echo "  - MySQL client dev:      libmysqlclient-dev or mariadb-connector-c-devel"; \
	      echo "  - PostgreSQL client dev: postgresql-server-dev-all or postgresql-devel"; \
	      echo ""; \
	      echo "Libraries:"; \
	      echo "  - JSON-C dev:    libjson-c-dev or json-c-devel"; \
	      echo "  - Zlib dev:      zlib1g-dev or zlib-devel"; \
	      echo "  - UUID dev:      uuid-dev or libuuid-devel"; \
	      echo "  - Lua dev:       liblua5.3-dev or lua-devel"; \
	      echo "  - Python dev:    python3-dev or python3-devel"; \
	      echo "  - Java dev:      openjdk-11/17-devel"; \
	      echo "  - ZeroMQ dev:    libzmq3-dev or zeromq-devel"; \
	      echo "  - Kafka client:  librdkafka-dev"; \
	      echo "  - cURL dev:      libcurl4-openssl-dev or libcurl-devel"; \
	      echo "  - Hiredis dev:   libhiredis-dev or hiredis-devel"; \
	      exit 1; \
	      ;; \
	  esac; \
	else \
	  echo "!! /etc/os-release not found. Cannot detect OS."; \
	  echo "   Please install the required build dependencies manually."; \
	  exit 1; \
	fi
	@echo "═══════════════════════════════════════════════════════════"
	@echo "Dependency installation complete!"
	@echo "═══════════════════════════════════════════════════════════"

# Show current structure
tree:
	@echo "Current directory structure:"
	@tree -L 3 -I '__pycache__|*.pyc|nbproject' || ls -R

# ============================================================================
# HELP
# ============================================================================
help:
	@echo "═══════════════════════════════════════════════════════════"
	@echo "CDC Streamers Build System (MySQL & PostgreSQL)"
	@echo "═══════════════════════════════════════════════════════════"
	@echo ""
	@echo "Main Targets:"
	@echo "  all              - Build both MySQL & PostgreSQL CDC streamers + plugins"
	@echo "  mysql            - Build MySQL binlog CDC streamer only"
	@echo "  postgres         - Build PostgreSQL logical replication streamer only"
	@echo ""
	@echo "Individual Components:"
	@echo "  $(MYSQL_TARGET)  - Build MySQL CDC streamer"
	@echo "  $(PG_TARGET)     - Build PostgreSQL CDC streamer"
	@echo "  directories      - Create build directory structure"
	@echo ""
	@echo "Installation:"
	@echo "  install          - Install both streamers and plugins system-wide"
	@echo "  install-mysql    - Install MySQL CDC streamer and plugins"
	@echo "  install-postgres - Install PostgreSQL CDC streamer and plugins"
	@echo "  install-plugins  - Install plugins to system location"
	@echo "  uninstall        - Uninstall all CDC streamers and plugins"
	@echo ""
	@echo "Running:"
	@echo "  run-mysql        - Build and run MySQL CDC streamer"
	@echo "  run-postgres     - Build and run PostgreSQL CDC streamer"
	@echo "  run              - Build and run MySQL CDC streamer (legacy)"
	@echo ""
	@echo "Testing:"
	@echo "  test-mysql       - Test MySQL CDC streamer"
	@echo "  test-postgres    - Test PostgreSQL CDC streamer"
	@echo "  test-plugins     - Test all plugins for required symbols"
	@echo "  test-lua         - Test Lua publisher plugin"
	@echo "  test-python      - Test Python publisher plugin"
	@echo ""
	@echo "Maintenance:"
	@echo "  clean            - Remove build artifacts"
	@echo "  clean-data       - Remove runtime data"
	@echo "  distclean        - Full clean (build + data)"
	@echo ""
	@echo "Information:"
	@echo "  config           - Show detailed build configuration"
	@echo "  check-deps       - Check database clients and library availability"
	@echo "  install-deps     - Auto-install dependencies (Ubuntu/RHEL/Debian/Arch)"
	@echo "  tree             - Show directory structure"
	@echo "  help             - Show this help message"
	@echo ""
	@echo "Variables:"
	@echo "  LUA_VERSION      - Lua version (default: $(LUA_VERSION))"
	@echo "  PYTHON_VERSION   - Python version (default: $(PYTHON_VERSION))"
	@echo "  JAVA_HOME        - Java home directory (detected: $(JAVA_HOME))"
	@echo ""
	@echo "Examples:"
	@echo "  make all                    # Build everything"
	@echo "  make mysql                  # Build MySQL CDC only"
	@echo "  make postgres               # Build PostgreSQL CDC only"
	@echo "  make install                # Install both streamers"
	@echo "  make run-mysql              # Run MySQL CDC streamer"
	@echo "  make run-postgres           # Run PostgreSQL CDC streamer"
	@echo "  make install-deps           # Install all dependencies"
	@echo "═══════════════════════════════════════════════════════════"

.PHONY: all mysql postgres directories clean clean-data distclean \
        install install-mysql install-postgres install-plugins uninstall \
        run run-mysql run-postgres \
        test-mysql test-postgres test-lua test-python test-plugins \
        config check-deps install-deps tree help