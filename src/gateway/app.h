#pragma once
#include "../../lib/net/http.h"
#include "../../lib/storage/storage.h"
#include "../../lib/scheduler/scheduler.h"
#include "../../lib/observ/observ.h"
#include "../../lib/core/hashmap.h"
#include "../../lib/core/threadpool.h"
#include <pthread.h>

#define DATA_DIR_DEFAULT "./data"
#define DB_PATH_DEFAULT  "./data/catalog.db"
#define DEFAULT_PORT     8080
#define WORKER_THREADS   4

typedef struct {
    /* subsystems */
    Catalog    *catalog;
    Scheduler  *scheduler;
    Metrics    *metrics;
    ThreadPool *workers;

    /* in-memory table store: name → Table* */
    HashMap     tables;
    pthread_mutex_t tables_mu;

    /* WebSocket client fds for live push */
    int         ws_clients[256];
    int         nws_clients;
    pthread_mutex_t ws_mu;

    /* config */
    char        data_dir[512];
    char        db_path[512];
    int         port;

    /* server */
    Router      router;
    HttpServer *server;
} App;

extern App g_app;

void app_init(App *app, const char *config_json);
void app_run(App *app);
void app_stop(App *app);
void app_ws_broadcast(App *app, const char *json_msg);

/* route registration */
void api_register_routes(Router *r);

/* pipeline step execution (runs transform_sql → target_table for each step) */
void pipeline_execute_steps(Pipeline *p, App *app);
