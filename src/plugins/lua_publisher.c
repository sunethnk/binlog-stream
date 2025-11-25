// lua_publisher.c
// Lua Publisher Plugin - Call configurable Lua method for CDC events
//
// Build: gcc -shared -fPIC -o lua_publisher.so lua_publisher.c -I. -llua5.3 -lm -ldl

#include "publisher_api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef HAVE_LUAJIT_H
#  include <luajit.h>
#else
#  include <lua5.3/lua.h>
#endif
#include <lua5.3/lualib.h>
#include <lua5.3/lauxlib.h>

typedef struct {
    lua_State *L;
    const char *script_path;
    const char *event_method;
    const char *init_method;
    const char *start_method;
    const char *stop_method;
    const char *cleanup_method;
    const char *health_method;
    const publisher_config_t *config;
    uint64_t events_published;
    uint64_t events_failed;
} lua_publisher_data_t;

static void push_event_to_lua(lua_State *L, const cdc_event_t *event);
static int call_lua_method(lua_State *L, const char *method, int nargs, int nresults);
static int call_lua_method_optional(lua_State *L, const char *method, int nargs, int nresults);

static const char* get_name(void) {
    return "lua_publisher";
}

static const char* get_version(void) {
    return "1.0.0";
}

static int get_api_version(void) {
    return PUBLISHER_API_VERSION;
}

static int init(const publisher_config_t *config, void **plugin_data) {
    PLUGIN_LOG_INFO("Initializing Lua publisher");
    
    lua_publisher_data_t *data = calloc(1, sizeof(lua_publisher_data_t));
    if (!data) {
        PLUGIN_LOG_ERROR("Failed to allocate publisher data");
        return -1;
    }
    
    data->config = config;
    
    data->script_path = PLUGIN_GET_CONFIG(config, "lua_script");
    if (!data->script_path) {
        PLUGIN_LOG_ERROR("Missing required configuration: lua_script");
        free(data);
        return -1;
    }
    
    data->event_method = PLUGIN_GET_CONFIG(config, "on_event_method");
    if (!data->event_method) {
        data->event_method = "on_event";
    }
    
    data->init_method = PLUGIN_GET_CONFIG(config, "on_init_method");
    data->start_method = PLUGIN_GET_CONFIG(config, "on_start_method");
    data->stop_method = PLUGIN_GET_CONFIG(config, "on_stop_method");
    data->cleanup_method = PLUGIN_GET_CONFIG(config, "on_cleanup_method");
    data->health_method = PLUGIN_GET_CONFIG(config, "on_health_method");
    
    PLUGIN_LOG_INFO("Loading Lua script: %s", data->script_path);
    PLUGIN_LOG_INFO("Event handler method: %s", data->event_method);
    
    data->L = luaL_newstate();
    if (!data->L) {
        PLUGIN_LOG_ERROR("Failed to create Lua state");
        free(data);
        return -1;
    }
    
    luaL_openlibs(data->L);
    
    if (luaL_loadfile(data->L, data->script_path) != LUA_OK) {
        const char *error = lua_tostring(data->L, -1);
        PLUGIN_LOG_ERROR("Failed to load Lua script: %s", error);
        lua_close(data->L);
        free(data);
        return -1;
    }
    
    if (lua_pcall(data->L, 0, 0, 0) != LUA_OK) {
        const char *error = lua_tostring(data->L, -1);
        PLUGIN_LOG_ERROR("Failed to execute Lua script: %s", error);
        lua_close(data->L);
        free(data);
        return -1;
    }
    
    if (data->init_method) {
        lua_getglobal(data->L, data->init_method);
        if (lua_isfunction(data->L, -1)) {
            lua_newtable(data->L);
            
            if (call_lua_method(data->L, data->init_method, 1, 1) != 0) {
                PLUGIN_LOG_ERROR("Lua %s method failed", data->init_method);
                lua_close(data->L);
                free(data);
                return -1;
            }
            
            int result = lua_tointeger(data->L, -1);
            lua_pop(data->L, 1);
            
            if (result != 0) {
                PLUGIN_LOG_ERROR("Lua %s returned error: %d", data->init_method, result);
                lua_close(data->L);
                free(data);
                return -1;
            }
        } else {
            lua_pop(data->L, 1);
            PLUGIN_LOG_INFO("No %s method found (optional)", data->init_method);
        }
    }
    
    *plugin_data = data;
    
    PLUGIN_LOG_INFO("Lua publisher initialized successfully");
    return 0;
}

static int start(void *plugin_data) {
    lua_publisher_data_t *data = (lua_publisher_data_t*)plugin_data;
    
    if (!data || !data->L) {
        PLUGIN_LOG_ERROR("Invalid plugin data");
        return -1;
    }
    
    PLUGIN_LOG_INFO("Starting Lua publisher: %s", data->script_path);
    
    if (data->start_method) {
        if (call_lua_method_optional(data->L, data->start_method, 0, 1) == 0) {
            int result = lua_tointeger(data->L, -1);
            lua_pop(data->L, 1);
            
            if (result != 0) {
                PLUGIN_LOG_ERROR("Lua %s returned error: %d", data->start_method, result);
                return -1;
            }
        }
    }
    
    PLUGIN_LOG_INFO("Lua publisher started successfully");
    return 0;
}

static int publish(void *plugin_data, const cdc_event_t *event) {
    lua_publisher_data_t *data = (lua_publisher_data_t*)plugin_data;
    
    if (!data || !data->L) {
        PLUGIN_LOG_ERROR("Invalid plugin data");
        return -1;
    }
    
    lua_getglobal(data->L, data->event_method);
    if (!lua_isfunction(data->L, -1)) {
        PLUGIN_LOG_ERROR("%s method not found in Lua script", data->event_method);
        lua_pop(data->L, 1);
        data->events_failed++;
        return -1;
    }
    
    push_event_to_lua(data->L, event);
    
    if (call_lua_method(data->L, data->event_method, 1, 1) != 0) {
        PLUGIN_LOG_ERROR("Lua %s method failed", data->event_method);
        data->events_failed++;
        return -1;
    }
    
    int result = lua_tointeger(data->L, -1);
    lua_pop(data->L, 1);
    
    if (result != 0) {
        PLUGIN_LOG_ERROR("Lua %s returned error: %d", data->event_method, result);
        data->events_failed++;
        return -1;
    }
    
    data->events_published++;
    PLUGIN_LOG_TRACE("Published event to Lua: txn=%s, db=%s, table=%s",
                    event->txn ? event->txn : "",
                    event->db ? event->db : "",
                    event->table ? event->table : "");
    
    return 0;
}

static int stop(void *plugin_data) {
    lua_publisher_data_t *data = (lua_publisher_data_t*)plugin_data;
    
    if (!data || !data->L) {
        PLUGIN_LOG_ERROR("Invalid plugin data");
        return -1;
    }
    
    PLUGIN_LOG_INFO("Stopping Lua publisher: %s (published=%llu, failed=%llu)",
                   data->script_path, data->events_published, data->events_failed);
    
    if (data->stop_method) {
        if (call_lua_method_optional(data->L, data->stop_method, 0, 1) == 0) {
            int result = lua_tointeger(data->L, -1);
            lua_pop(data->L, 1);
            
            if (result != 0) {
                PLUGIN_LOG_ERROR("Lua %s returned error: %d", data->stop_method, result);
                return -1;
            }
        }
    }
    
    return 0;
}

static void cleanup(void *plugin_data) {
    lua_publisher_data_t *data = (lua_publisher_data_t*)plugin_data;
    
    if (!data) {
        return;
    }
    
    if (data->L) {
        if (data->cleanup_method) {
            call_lua_method_optional(data->L, data->cleanup_method, 0, 0);
        }
        
        lua_close(data->L);
    }
    
    free(data);
    
    PLUGIN_LOG_INFO("Lua publisher cleaned up");
}

static int health_check(void *plugin_data) {
    lua_publisher_data_t *data = (lua_publisher_data_t*)plugin_data;
    
    if (!data || !data->L) {
        return -1;
    }
    
    if (data->health_method) {
        if (call_lua_method_optional(data->L, data->health_method, 0, 1) == 0) {
            int result = lua_tointeger(data->L, -1);
            lua_pop(data->L, 1);
            return result;
        }
    }
    
    return 0;
}

static void push_event_to_lua(lua_State *L, const cdc_event_t *event) {
    lua_newtable(L);
    
    if (event->txn) {
        lua_pushstring(L, event->txn);
        lua_setfield(L, -2, "txn");
    }
    
    if (event->db) {
        lua_pushstring(L, event->db);
        lua_setfield(L, -2, "db");
    }
    
    if (event->table) {
        lua_pushstring(L, event->table);
        lua_setfield(L, -2, "table");
    }
    
    if (event->json) {
        lua_pushstring(L, event->json);
        lua_setfield(L, -2, "json");
    }
}

static int call_lua_method(lua_State *L, const char *method, int nargs, int nresults) {
    if (lua_pcall(L, nargs, nresults, 0) != LUA_OK) {
        const char *error = lua_tostring(L, -1);
        PLUGIN_LOG_ERROR("Error calling Lua method '%s': %s", method, error);
        lua_pop(L, 1);
        return -1;
    }
    return 0;
}

static int call_lua_method_optional(lua_State *L, const char *method, int nargs, int nresults) {
    lua_getglobal(L, method);
    if (!lua_isfunction(L, -1)) {
        lua_pop(L, 1);
        PLUGIN_LOG_INFO("No %s method found (optional)", method);
        return -1;
    }
    
    return call_lua_method(L, method, nargs, nresults);
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

PUBLISHER_PLUGIN_DEFINE(lua_publisher) {
    publisher_plugin_t *p = malloc(sizeof(publisher_plugin_t));
    if (!p) return -1;
    
    p->callbacks = &callbacks;
    p->plugin_data = NULL;
    
    *plugin = p;
    return 0;
}