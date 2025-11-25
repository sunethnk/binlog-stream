// python_publisher.c
// Python Publisher Plugin - Call configurable Python method for CDC events
//
// Build: gcc -shared -fPIC -o python_publisher.so python_publisher.c -I. -I/usr/include/python3.8 -lpython3.8

#include "publisher_api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <python3.10/Python.h>

typedef struct {
    PyObject *module;
    PyObject *event_method;
    PyObject *init_method;
    PyObject *start_method;
    PyObject *stop_method;
    PyObject *cleanup_method;
    PyObject *health_method;
    const char *script_path;
    const char *event_method_name;
    uint64_t events_published;
    uint64_t events_failed;
} python_publisher_data_t;

static PyObject* create_event_dict(const cdc_event_t *event);
static int call_python_method(PyObject *method, PyObject *args, int *result);
static PyObject* get_method_optional(PyObject *module, const char *method_name);

static const char* get_name(void) {
    return "python_publisher";
}

static const char* get_version(void) {
    return "1.0.0";
}

static int get_api_version(void) {
    return PUBLISHER_API_VERSION;
}

static int init(const publisher_config_t *config, void **plugin_data) {
    PLUGIN_LOG_INFO("Initializing Python publisher");
    
    python_publisher_data_t *data = calloc(1, sizeof(python_publisher_data_t));
    if (!data) {
        PLUGIN_LOG_ERROR("Failed to allocate publisher data");
        return -1;
    }
    
    data->script_path = PLUGIN_GET_CONFIG(config, "python_script");
    if (!data->script_path) {
        PLUGIN_LOG_ERROR("Missing required configuration: python_script");
        free(data);
        return -1;
    }
    
    data->event_method_name = PLUGIN_GET_CONFIG(config, "on_event_method");
    if (!data->event_method_name) {
        data->event_method_name = "on_event";
    }
    
    const char *init_method_name = PLUGIN_GET_CONFIG(config, "on_init_method");
    const char *start_method_name = PLUGIN_GET_CONFIG(config, "on_start_method");
    const char *stop_method_name = PLUGIN_GET_CONFIG(config, "on_stop_method");
    const char *cleanup_method_name = PLUGIN_GET_CONFIG(config, "on_cleanup_method");
    const char *health_method_name = PLUGIN_GET_CONFIG(config, "on_health_method");
    
    PLUGIN_LOG_INFO("Loading Python script: %s", data->script_path);
    PLUGIN_LOG_INFO("Event handler method: %s", data->event_method_name);
    
    // Initialize Python interpreter
    if (!Py_IsInitialized()) {
        Py_Initialize();
    }
    
    // Add script directory to Python path
    PyObject *sys_path = PySys_GetObject("path");
    PyObject *path = PyUnicode_FromString(".");
    PyList_Append(sys_path, path);
    Py_DECREF(path);
    
    // Extract module name from script path
    const char *module_name = strrchr(data->script_path, '/');
    if (module_name) {
        module_name++;
    } else {
        module_name = data->script_path;
    }
    
    // Remove .py extension if present
    char *module_name_copy = strdup(module_name);
    char *dot = strrchr(module_name_copy, '.');
    if (dot && strcmp(dot, ".py") == 0) {
        *dot = '\0';
    }
    
    // Import the module
    PyObject *module_name_obj = PyUnicode_FromString(module_name_copy);
    data->module = PyImport_Import(module_name_obj);
    Py_DECREF(module_name_obj);
    free(module_name_copy);
    
    if (!data->module) {
        PyErr_Print();
        PLUGIN_LOG_ERROR("Failed to load Python module");
        free(data);
        return -1;
    }
    
    // Get event method (required)
    data->event_method = PyObject_GetAttrString(data->module, data->event_method_name);
    if (!data->event_method || !PyCallable_Check(data->event_method)) {
        PLUGIN_LOG_ERROR("Method '%s' not found or not callable in Python script", 
                        data->event_method_name);
        Py_XDECREF(data->event_method);
        Py_DECREF(data->module);
        free(data);
        return -1;
    }
    
    // Get optional methods
    if (init_method_name) {
        data->init_method = get_method_optional(data->module, init_method_name);
    }
    if (start_method_name) {
        data->start_method = get_method_optional(data->module, start_method_name);
    }
    if (stop_method_name) {
        data->stop_method = get_method_optional(data->module, stop_method_name);
    }
    if (cleanup_method_name) {
        data->cleanup_method = get_method_optional(data->module, cleanup_method_name);
    }
    if (health_method_name) {
        data->health_method = get_method_optional(data->module, health_method_name);
    }
    
    // Call init method if present
    if (data->init_method) {
        PyObject *config_dict = PyDict_New();
        PyObject *args = PyTuple_New(1);
        PyTuple_SetItem(args, 0, config_dict);
        
        int result = 0;
        if (call_python_method(data->init_method, args, &result) != 0) {
            PLUGIN_LOG_ERROR("Python init method failed");
            Py_DECREF(args);
            Py_XDECREF(data->init_method);
            Py_XDECREF(data->event_method);
            Py_DECREF(data->module);
            free(data);
            return -1;
        }
        Py_DECREF(args);
        
        if (result != 0) {
            PLUGIN_LOG_ERROR("Python init method returned error: %d", result);
            Py_XDECREF(data->init_method);
            Py_XDECREF(data->event_method);
            Py_DECREF(data->module);
            free(data);
            return -1;
        }
    }
    
    *plugin_data = data;
    
    PLUGIN_LOG_INFO("Python publisher initialized successfully");
    return 0;
}

static int start(void *plugin_data) {
    python_publisher_data_t *data = (python_publisher_data_t*)plugin_data;
    
    if (!data || !data->module) {
        PLUGIN_LOG_ERROR("Invalid plugin data");
        return -1;
    }
    
    PLUGIN_LOG_INFO("Starting Python publisher: %s", data->script_path);
    
    if (data->start_method) {
        PyObject *args = PyTuple_New(0);
        int result = 0;
        
        if (call_python_method(data->start_method, args, &result) != 0) {
            PLUGIN_LOG_ERROR("Python start method failed");
            Py_DECREF(args);
            return -1;
        }
        Py_DECREF(args);
        
        if (result != 0) {
            PLUGIN_LOG_ERROR("Python start method returned error: %d", result);
            return -1;
        }
    }
    
    PLUGIN_LOG_INFO("Python publisher started successfully");
    return 0;
}

static int publish(void *plugin_data, const cdc_event_t *event) {
    python_publisher_data_t *data = (python_publisher_data_t*)plugin_data;
    
    if (!data || !data->event_method) {
        PLUGIN_LOG_ERROR("Invalid plugin data");
        return -1;
    }
    
    PyObject *event_dict = create_event_dict(event);
    if (!event_dict) {
        PLUGIN_LOG_ERROR("Failed to create event dictionary");
        data->events_failed++;
        return -1;
    }
    
    PyObject *args = PyTuple_New(1);
    PyTuple_SetItem(args, 0, event_dict);
    
    int result = 0;
    if (call_python_method(data->event_method, args, &result) != 0) {
        PLUGIN_LOG_ERROR("Python %s method failed", data->event_method_name);
        Py_DECREF(args);
        data->events_failed++;
        return -1;
    }
    Py_DECREF(args);
    
    if (result != 0) {
        PLUGIN_LOG_ERROR("Python %s method returned error: %d", 
                        data->event_method_name, result);
        data->events_failed++;
        return -1;
    }
    
    data->events_published++;
    PLUGIN_LOG_TRACE("Published event to Python: txn=%s, db=%s, table=%s",
                    event->txn ? event->txn : "",
                    event->db ? event->db : "",
                    event->table ? event->table : "");
    
    return 0;
}

static int stop(void *plugin_data) {
    python_publisher_data_t *data = (python_publisher_data_t*)plugin_data;
    
    if (!data || !data->module) {
        PLUGIN_LOG_ERROR("Invalid plugin data");
        return -1;
    }
    
    PLUGIN_LOG_INFO("Stopping Python publisher: %s (published=%llu, failed=%llu)",
                   data->script_path, data->events_published, data->events_failed);
    
    if (data->stop_method) {
        PyObject *args = PyTuple_New(0);
        int result = 0;
        
        if (call_python_method(data->stop_method, args, &result) != 0) {
            PLUGIN_LOG_ERROR("Python stop method failed");
            Py_DECREF(args);
            return -1;
        }
        Py_DECREF(args);
        
        if (result != 0) {
            PLUGIN_LOG_ERROR("Python stop method returned error: %d", result);
            return -1;
        }
    }
    
    return 0;
}

static void cleanup(void *plugin_data) {
    python_publisher_data_t *data = (python_publisher_data_t*)plugin_data;
    
    if (!data) {
        return;
    }
    
    if (data->cleanup_method) {
        PyObject *args = PyTuple_New(0);
        int result = 0;
        call_python_method(data->cleanup_method, args, &result);
        Py_DECREF(args);
    }
    
    Py_XDECREF(data->cleanup_method);
    Py_XDECREF(data->health_method);
    Py_XDECREF(data->stop_method);
    Py_XDECREF(data->start_method);
    Py_XDECREF(data->init_method);
    Py_XDECREF(data->event_method);
    Py_XDECREF(data->module);
    
    free(data);
    
    PLUGIN_LOG_INFO("Python publisher cleaned up");
}

static int health_check(void *plugin_data) {
    python_publisher_data_t *data = (python_publisher_data_t*)plugin_data;
    
    if (!data || !data->module) {
        return -1;
    }
    
    if (data->health_method) {
        PyObject *args = PyTuple_New(0);
        int result = 0;
        
        if (call_python_method(data->health_method, args, &result) != 0) {
            PLUGIN_LOG_ERROR("Python health check method failed");
            Py_DECREF(args);
            return -1;
        }
        Py_DECREF(args);
        
        return result;
    }
    
    return 0;
}

static PyObject* create_event_dict(const cdc_event_t *event) {
    PyObject *dict = PyDict_New();
    if (!dict) {
        return NULL;
    }
    
    if (event->txn) {
        PyObject *txn = PyUnicode_FromString(event->txn);
        PyDict_SetItemString(dict, "txn", txn);
        Py_DECREF(txn);
    }
    
    if (event->db) {
        PyObject *db = PyUnicode_FromString(event->db);
        PyDict_SetItemString(dict, "db", db);
        Py_DECREF(db);
    }
    
    if (event->table) {
        PyObject *table = PyUnicode_FromString(event->table);
        PyDict_SetItemString(dict, "table", table);
        Py_DECREF(table);
    }
    
    if (event->json) {
        PyObject *json = PyUnicode_FromString(event->json);
        PyDict_SetItemString(dict, "json", json);
        Py_DECREF(json);
    }
    
    return dict;
}

static int call_python_method(PyObject *method, PyObject *args, int *result) {
    PyObject *ret = PyObject_CallObject(method, args);
    
    if (!ret) {
        PyErr_Print();
        return -1;
    }
    
    if (PyLong_Check(ret)) {
        *result = (int)PyLong_AsLong(ret);
    } else {
        *result = 0;
    }
    
    Py_DECREF(ret);
    return 0;
}

static PyObject* get_method_optional(PyObject *module, const char *method_name) {
    PyObject *method = PyObject_GetAttrString(module, method_name);
    
    if (!method || !PyCallable_Check(method)) {
        if (method) {
            Py_DECREF(method);
        } else {
            PyErr_Clear();
        }
        PLUGIN_LOG_INFO("Method '%s' not found or not callable (optional)", method_name);
        return NULL;
    }
    
    return method;
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

PUBLISHER_PLUGIN_DEFINE(python_publisher) {
    publisher_plugin_t *p = malloc(sizeof(publisher_plugin_t));
    if (!p) return -1;
    
    p->callbacks = &callbacks;
    p->plugin_data = NULL;
    
    *plugin = p;
    return 0;
}