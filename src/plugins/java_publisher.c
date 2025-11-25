// java_publisher.c - Fixed version with proper thread handling
// Build: gcc -shared -fPIC -o java_publisher.so java_publisher.c -I. -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux -L${JAVA_HOME}/lib/server -ljvm -Wl,-rpath,${JAVA_HOME}/lib/server

#include "publisher_api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <jni.h>

typedef struct {
    JavaVM *jvm;
    jobject publisher_obj;
    jclass publisher_class;
    jmethodID init_method;
    jmethodID start_method;
    jmethodID event_method;
    jmethodID stop_method;
    jmethodID cleanup_method;
    jmethodID health_method;
    const char *class_name;
    const char *event_method_name;
    pthread_mutex_t lock;
    uint64_t events_published;
    uint64_t events_failed;
} java_publisher_data_t;

static jobject create_event_object(JNIEnv *env, const cdc_event_t *event);
static jobject create_config_map(JNIEnv *env, const publisher_config_t *config);
static int call_java_method(JNIEnv *env, jobject obj, jmethodID method, jobject arg, int *result);
static jmethodID get_method_optional(JNIEnv *env, jclass clazz, const char *method_name, const char *signature);

// Get JNIEnv for current thread, attaching if necessary
static JNIEnv* get_jni_env(java_publisher_data_t *data) {
    JNIEnv *env;
    jint res = (*data->jvm)->GetEnv(data->jvm, (void**)&env, JNI_VERSION_1_8);
    
    if (res == JNI_EDETACHED) {
        // Thread not attached, attach it
        res = (*data->jvm)->AttachCurrentThread(data->jvm, (void**)&env, NULL);
        if (res != JNI_OK) {
            PLUGIN_LOG_ERROR("Failed to attach thread to JVM: %d", res);
            return NULL;
        }
    } else if (res != JNI_OK) {
        PLUGIN_LOG_ERROR("Failed to get JNI environment: %d", res);
        return NULL;
    }
    
    return env;
}

static jobject create_event_object(JNIEnv *env, const cdc_event_t *event);
static int call_java_method(JNIEnv *env, jobject obj, jmethodID method, jobject arg, int *result);
static jmethodID get_method_optional(JNIEnv *env, jclass clazz, const char *method_name, const char *signature);

static const char* get_name(void) {
    return "java_publisher";
}

static const char* get_version(void) {
    return "1.0.0";
}

static int get_api_version(void) {
    return PUBLISHER_API_VERSION;
}

static int init(const publisher_config_t *config, void **plugin_data) {
    PLUGIN_LOG_INFO("Initializing Java publisher");
    
    java_publisher_data_t *data = calloc(1, sizeof(java_publisher_data_t));
    if (!data) {
        PLUGIN_LOG_ERROR("Failed to allocate publisher data");
        return -1;
    }
    
    pthread_mutex_init(&data->lock, NULL);
    
    data->class_name = PLUGIN_GET_CONFIG(config, "java_class");
    if (!data->class_name) {
        PLUGIN_LOG_ERROR("Missing required configuration: java_class");
        pthread_mutex_destroy(&data->lock);
        free(data);
        return -1;
    }
    
    data->event_method_name = PLUGIN_GET_CONFIG(config, "on_event_method");
    if (!data->event_method_name) {
        data->event_method_name = "onEvent";
    }
    
    const char *init_method_name = PLUGIN_GET_CONFIG(config, "on_init_method");
    const char *start_method_name = PLUGIN_GET_CONFIG(config, "on_start_method");
    const char *stop_method_name = PLUGIN_GET_CONFIG(config, "on_stop_method");
    const char *cleanup_method_name = PLUGIN_GET_CONFIG(config, "on_cleanup_method");
    const char *health_method_name = PLUGIN_GET_CONFIG(config, "on_health_method");
    const char *classpath = PLUGIN_GET_CONFIG(config, "java_classpath");
    const char *jvm_args = PLUGIN_GET_CONFIG(config, "jvm_args");
    
    PLUGIN_LOG_INFO("Loading Java class: %s", data->class_name);
    PLUGIN_LOG_INFO("Event handler method: %s", data->event_method_name);
    
    // Parse custom JVM arguments
    int custom_arg_count = 0;
    char *jvm_args_copy = NULL;
    char *custom_args[32] = {NULL};  // Max 32 custom args
    
    if (jvm_args) {
        jvm_args_copy = strdup(jvm_args);
        char *token = strtok(jvm_args_copy, " ");
        while (token && custom_arg_count < 32) {
            custom_args[custom_arg_count++] = strdup(token);
            token = strtok(NULL, " ");
        }
        PLUGIN_LOG_INFO("Parsed %d custom JVM arguments", custom_arg_count);
    }
    
    // Create JVM with mandatory + custom options
    JavaVMInitArgs vm_args;
    
    // Mandatory options count: classpath + Xrs + headless
    int mandatory_count = 3;
    int total_options = mandatory_count + custom_arg_count;
    
    JavaVMOption *options = calloc(total_options, sizeof(JavaVMOption));
    if (!options) {
        PLUGIN_LOG_ERROR("Failed to allocate JVM options");
        for (int i = 0; i < custom_arg_count; i++) {
            free(custom_args[i]);
        }
        free(jvm_args_copy);
        pthread_mutex_destroy(&data->lock);
        free(data);
        return -1;
    }
    
    int opt_idx = 0;
    
    // Mandatory: Classpath
    char *classpath_opt = malloc(512);
    if (classpath) {
        snprintf(classpath_opt, 512, "-Djava.class.path=%s", classpath);
    } else {
        snprintf(classpath_opt, 512, "-Djava.class.path=.");
    }
    options[opt_idx++].optionString = classpath_opt;
    
    // Mandatory: Reduce signal usage (critical for Ctrl+C)
    options[opt_idx++].optionString = "-Xrs";
    
    // Mandatory: Headless mode
    options[opt_idx++].optionString = "-Djava.awt.headless=true";
    
    // Add custom arguments
    for (int i = 0; i < custom_arg_count; i++) {
        options[opt_idx++].optionString = custom_args[i];
        PLUGIN_LOG_INFO("JVM arg: %s", custom_args[i]);
    }
    
    vm_args.version = JNI_VERSION_1_8;
    vm_args.nOptions = total_options;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = JNI_FALSE;
    
    JNIEnv *env;
    jint res = JNI_CreateJavaVM(&data->jvm, (void**)&env, &vm_args);
    
    // Cleanup allocated strings
    free(classpath_opt);
    for (int i = 0; i < custom_arg_count; i++) {
        free(custom_args[i]);
    }
    free(jvm_args_copy);
    free(options);
    
    if (res != JNI_OK) {
        PLUGIN_LOG_ERROR("Failed to create Java VM: %d", res);
        pthread_mutex_destroy(&data->lock);
        free(data);
        return -1;
    }
    
    // Convert class name from dot notation to slash notation
    char *class_path = strdup(data->class_name);
    for (char *p = class_path; *p; p++) {
        if (*p == '.') *p = '/';
    }
    
    // Load class
    jclass local_class = (*env)->FindClass(env, class_path);
    free(class_path);
    
    if (!local_class) {
        PLUGIN_LOG_ERROR("Failed to find Java class: %s", data->class_name);
        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);
        (*data->jvm)->DestroyJavaVM(data->jvm);
        pthread_mutex_destroy(&data->lock);
        free(data);
        return -1;
    }
    
    // Create global reference to class
    data->publisher_class = (*env)->NewGlobalRef(env, local_class);
    (*env)->DeleteLocalRef(env, local_class);
    
    // Get constructor
    jmethodID constructor = (*env)->GetMethodID(env, data->publisher_class, "<init>", "()V");
    if (!constructor) {
        PLUGIN_LOG_ERROR("Failed to find default constructor");
        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);
        (*env)->DeleteGlobalRef(env, data->publisher_class);
        (*data->jvm)->DestroyJavaVM(data->jvm);
        pthread_mutex_destroy(&data->lock);
        free(data);
        return -1;
    }
    
    // Create instance
    jobject local_obj = (*env)->NewObject(env, data->publisher_class, constructor);
    if (!local_obj) {
        PLUGIN_LOG_ERROR("Failed to create Java object instance");
        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);
        (*env)->DeleteGlobalRef(env, data->publisher_class);
        (*data->jvm)->DestroyJavaVM(data->jvm);
        pthread_mutex_destroy(&data->lock);
        free(data);
        return -1;
    }
    
    data->publisher_obj = (*env)->NewGlobalRef(env, local_obj);
    (*env)->DeleteLocalRef(env, local_obj);
    
    // Get event method (required)
    data->event_method = (*env)->GetMethodID(env, data->publisher_class, 
                                            data->event_method_name, 
                                            "(Ljava/util/Map;)I");
    if (!data->event_method) {
        PLUGIN_LOG_ERROR("Method '%s' not found or has wrong signature (Ljava/util/Map;)I", 
                        data->event_method_name);
        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);
        (*env)->DeleteGlobalRef(env, data->publisher_obj);
        (*env)->DeleteGlobalRef(env, data->publisher_class);
        (*data->jvm)->DestroyJavaVM(data->jvm);
        pthread_mutex_destroy(&data->lock);
        free(data);
        return -1;
    }
    
    // Get optional methods
    if (init_method_name) {
        data->init_method = get_method_optional(env, data->publisher_class, 
                                              init_method_name, "(Ljava/util/Map;)I");
    }
    if (start_method_name) {
        data->start_method = get_method_optional(env, data->publisher_class, 
                                               start_method_name, "()I");
    }
    if (stop_method_name) {
        data->stop_method = get_method_optional(env, data->publisher_class, 
                                             stop_method_name, "()I");
    }
    if (cleanup_method_name) {
        data->cleanup_method = get_method_optional(env, data->publisher_class, 
                                                 cleanup_method_name, "()V");
    }
    if (health_method_name) {
        data->health_method = get_method_optional(env, data->publisher_class, 
                                                health_method_name, "()I");
    }
    
    // Call init method if present
// Call init method if present
    if (data->init_method) {
        jobject config_map = create_config_map(env, config);
        
        int result = 0;
        if (call_java_method(env, data->publisher_obj, data->init_method, config_map, &result) != 0) {
            PLUGIN_LOG_ERROR("Java init method failed");
            (*env)->DeleteLocalRef(env, config_map);
            (*env)->DeleteGlobalRef(env, data->publisher_obj);
            (*env)->DeleteGlobalRef(env, data->publisher_class);
            (*data->jvm)->DestroyJavaVM(data->jvm);
            pthread_mutex_destroy(&data->lock);
            free(data);
            return -1;
        }
        (*env)->DeleteLocalRef(env, config_map);
        
        if (result != 0) {
            PLUGIN_LOG_ERROR("Java init method returned error: %d", result);
            (*env)->DeleteGlobalRef(env, data->publisher_obj);
            (*env)->DeleteGlobalRef(env, data->publisher_class);
            (*data->jvm)->DestroyJavaVM(data->jvm);
            pthread_mutex_destroy(&data->lock);
            free(data);
            return -1;
        }
    }
    
    *plugin_data = data;
    
    PLUGIN_LOG_INFO("Java publisher initialized successfully");
    return 0;
}

static int start(void *plugin_data) {
    java_publisher_data_t *data = (java_publisher_data_t*)plugin_data;
    
    if (!data || !data->jvm) {
        PLUGIN_LOG_ERROR("Invalid plugin data");
        return -1;
    }
    
    PLUGIN_LOG_INFO("Starting Java publisher: %s", data->class_name);
    
    JNIEnv *env = get_jni_env(data);
    if (!env) {
        return -1;
    }
    
    if (data->start_method) {
        jint ret = (*env)->CallIntMethod(env, data->publisher_obj, data->start_method);
        
        if ((*env)->ExceptionCheck(env)) {
            (*env)->ExceptionDescribe(env);
            (*env)->ExceptionClear(env);
            PLUGIN_LOG_ERROR("Java start method threw exception");
            return -1;
        }
        
        if (ret != 0) {
            PLUGIN_LOG_ERROR("Java start method returned error: %d", ret);
            return -1;
        }
    }
    
    PLUGIN_LOG_INFO("Java publisher started successfully");
    return 0;
}

static int publish(void *plugin_data, const cdc_event_t *event) {
    java_publisher_data_t *data = (java_publisher_data_t*)plugin_data;
    
    if (!data) {
        PLUGIN_LOG_ERROR("Invalid plugin data");
        return -1;
    }
    
    pthread_mutex_lock(&data->lock);
    
    JNIEnv *env = get_jni_env(data);
    if (!env) {
        pthread_mutex_unlock(&data->lock);
        data->events_failed++;
        return -1;
    }
    
    jobject event_map = create_event_object(env, event);
    if (!event_map) {
        PLUGIN_LOG_ERROR("Failed to create event map");
        pthread_mutex_unlock(&data->lock);
        data->events_failed++;
        return -1;
    }
    
    int result = 0;
    if (call_java_method(env, data->publisher_obj, data->event_method, event_map, &result) != 0) {
        PLUGIN_LOG_ERROR("Java %s method failed", data->event_method_name);
        (*env)->DeleteLocalRef(env, event_map);
        pthread_mutex_unlock(&data->lock);
        data->events_failed++;
        return -1;
    }
    (*env)->DeleteLocalRef(env, event_map);
    
    pthread_mutex_unlock(&data->lock);
    
    if (result != 0) {
        PLUGIN_LOG_ERROR("Java %s method returned error: %d", data->event_method_name, result);
        data->events_failed++;
        return -1;
    }
    
    data->events_published++;
    PLUGIN_LOG_TRACE("Published event to Java: txn=%s, db=%s, table=%s",
                    event->txn ? event->txn : "",
                    event->db ? event->db : "",
                    event->table ? event->table : "");
    
    return 0;
}

static int stop(void *plugin_data) {
    java_publisher_data_t *data = (java_publisher_data_t*)plugin_data;
    
    if (!data || !data->jvm) {
        PLUGIN_LOG_ERROR("Invalid plugin data");
        return -1;
    }
    
    PLUGIN_LOG_INFO("Stopping Java publisher: %s (published=%llu, failed=%llu)",
                   data->class_name, data->events_published, data->events_failed);
    
    JNIEnv *env = get_jni_env(data);
    if (!env) {
        return -1;
    }
    
    if (data->stop_method) {
        jint ret = (*env)->CallIntMethod(env, data->publisher_obj, data->stop_method);
        
        if ((*env)->ExceptionCheck(env)) {
            (*env)->ExceptionDescribe(env);
            (*env)->ExceptionClear(env);
            PLUGIN_LOG_ERROR("Java stop method threw exception");
            return -1;
        }
        
        if (ret != 0) {
            PLUGIN_LOG_ERROR("Java stop method returned error: %d", ret);
            return -1;
        }
    }
    
    return 0;
}

static void cleanup(void *plugin_data) {
    java_publisher_data_t *data = (java_publisher_data_t*)plugin_data;
    
    if (!data) {
        return;
    }
    
    JNIEnv *env = get_jni_env(data);
    
    if (data->cleanup_method && env) {
        (*env)->CallVoidMethod(env, data->publisher_obj, data->cleanup_method);
        if ((*env)->ExceptionCheck(env)) {
            (*env)->ExceptionDescribe(env);
            (*env)->ExceptionClear(env);
        }
    }
    
    if (env) {
        if (data->publisher_obj) {
            (*env)->DeleteGlobalRef(env, data->publisher_obj);
        }
        if (data->publisher_class) {
            (*env)->DeleteGlobalRef(env, data->publisher_class);
        }
    }
    
//    if (data->jvm) {
//        (*data->jvm)->DestroyJavaVM(data->jvm);
//    }
    
    pthread_mutex_destroy(&data->lock);
    free(data);
    
    PLUGIN_LOG_INFO("Java publisher cleaned up");
}

static int health_check(void *plugin_data) {
    java_publisher_data_t *data = (java_publisher_data_t*)plugin_data;
    
    if (!data || !data->jvm) {
        return -1;
    }
    
    JNIEnv *env = get_jni_env(data);
    if (!env) {
        return -1;
    }
    
    if (data->health_method) {
        jint ret = (*env)->CallIntMethod(env, data->publisher_obj, data->health_method);
        
        if ((*env)->ExceptionCheck(env)) {
            (*env)->ExceptionDescribe(env);
            (*env)->ExceptionClear(env);
            return -1;
        }
        
        return ret;
    }
    
    return 0;
}

static jobject create_event_object(JNIEnv *env, const cdc_event_t *event) {
    jclass map_class = (*env)->FindClass(env, "java/util/HashMap");
    jmethodID map_init = (*env)->GetMethodID(env, map_class, "<init>", "()V");
    jmethodID map_put = (*env)->GetMethodID(env, map_class, "put", 
                                            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    
    jobject map = (*env)->NewObject(env, map_class, map_init);
    
    if (event->txn) {
        jstring key = (*env)->NewStringUTF(env, "txn");
        jstring value = (*env)->NewStringUTF(env, event->txn);
        jobject old = (*env)->CallObjectMethod(env, map, map_put, key, value);
        if (old) (*env)->DeleteLocalRef(env, old);
        (*env)->DeleteLocalRef(env, key);
        (*env)->DeleteLocalRef(env, value);
    }
    
    if (event->db) {
        jstring key = (*env)->NewStringUTF(env, "db");
        jstring value = (*env)->NewStringUTF(env, event->db);
        jobject old = (*env)->CallObjectMethod(env, map, map_put, key, value);
        if (old) (*env)->DeleteLocalRef(env, old);
        (*env)->DeleteLocalRef(env, key);
        (*env)->DeleteLocalRef(env, value);
    }
    
    if (event->table) {
        jstring key = (*env)->NewStringUTF(env, "table");
        jstring value = (*env)->NewStringUTF(env, event->table);
        jobject old = (*env)->CallObjectMethod(env, map, map_put, key, value);
        if (old) (*env)->DeleteLocalRef(env, old);
        (*env)->DeleteLocalRef(env, key);
        (*env)->DeleteLocalRef(env, value);
    }
    
    if (event->json) {
        jstring key = (*env)->NewStringUTF(env, "json");
        jstring value = (*env)->NewStringUTF(env, event->json);
        jobject old = (*env)->CallObjectMethod(env, map, map_put, key, value);
        if (old) (*env)->DeleteLocalRef(env, old);
        (*env)->DeleteLocalRef(env, key);
        (*env)->DeleteLocalRef(env, value);
    }
    
    return map;
}

static int call_java_method(JNIEnv *env, jobject obj, jmethodID method, jobject arg, int *result) {
    jint ret = (*env)->CallIntMethod(env, obj, method, arg);
    
    if ((*env)->ExceptionCheck(env)) {
        (*env)->ExceptionDescribe(env);
        (*env)->ExceptionClear(env);
        return -1;
    }
    
    *result = ret;
    return 0;
}

// Add this helper function after get_method_optional
static jobject create_config_map(JNIEnv *env, const publisher_config_t *config) {
    jclass map_class = (*env)->FindClass(env, "java/util/HashMap");
    jmethodID map_init = (*env)->GetMethodID(env, map_class, "<init>", "()V");
    jmethodID map_put = (*env)->GetMethodID(env, map_class, "put", 
                                            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    
    jobject map = (*env)->NewObject(env, map_class, map_init);
    
    // We need to manually add known config keys since we can't iterate publisher_config_t
    // Add common config keys that might be present
    const char *config_keys[] = {
        "java_class",
        "java_classpath", 
        "on_event_method",
        "on_init_method",
        "on_start_method",
        "on_stop_method",
        "on_cleanup_method",
        "on_health_method",
        "jvm_args",
        // Add any custom keys your application might use
        "output_file",
        "batch_size",
        "timeout",
        "max_retries",
        NULL  // Sentinel
    };
    
    for (int i = 0; config_keys[i] != NULL; i++) {
        const char *value = PLUGIN_GET_CONFIG(config, config_keys[i]);
        if (value) {
            jstring key = (*env)->NewStringUTF(env, config_keys[i]);
            jstring val = (*env)->NewStringUTF(env, value);
            jobject old = (*env)->CallObjectMethod(env, map, map_put, key, val);
            if (old) (*env)->DeleteLocalRef(env, old);
            (*env)->DeleteLocalRef(env, key);
            (*env)->DeleteLocalRef(env, val);
        }
    }
    
    return map;
}

static jmethodID get_method_optional(JNIEnv *env, jclass clazz, const char *method_name, const char *signature) {
    jmethodID method = (*env)->GetMethodID(env, clazz, method_name, signature);
    
    if (!method) {
        (*env)->ExceptionClear(env);
        PLUGIN_LOG_INFO("Method '%s' not found (optional)", method_name);
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

PUBLISHER_PLUGIN_DEFINE(java_publisher) {
    publisher_plugin_t *p = malloc(sizeof(publisher_plugin_t));
    if (!p) return -1;
    
    p->callbacks = &callbacks;
    p->plugin_data = NULL;
    
    *plugin = p;
    return 0;
}