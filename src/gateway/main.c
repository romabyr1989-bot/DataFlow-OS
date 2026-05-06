#include "app.h"
#include "../../lib/core/log.h"
#include "../../lib/auth/auth.h"
#include "../../lib/core/json.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/socket.h>
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

App g_app;

/* WebSocket broadcast to all connected clients */
void app_ws_broadcast(App *app, const char *json_msg) {
    if (!json_msg) return;
    pthread_mutex_lock(&app->ws_mu);
    size_t len = strlen(json_msg);
    if (len > 65535) { pthread_mutex_unlock(&app->ws_mu); return; }
    /* Frame: FIN=1, opcode=1 (text), mask=0 */
    uint8_t frame[16+65536]; int flen=0;
    frame[flen++]=0x81; /* FIN + text */
    if(len<126) frame[flen++]=(uint8_t)len;
    else{ frame[flen++]=126; frame[flen++]=(uint8_t)(len>>8); frame[flen++]=(uint8_t)len; }
    memcpy(frame+flen,json_msg,len); flen+=(int)len;
    int dead[256]; int ndead=0;
    for(int i=0;i<app->nws_clients;i++){
        if(send(app->ws_clients[i],frame,(size_t)flen,MSG_NOSIGNAL)<0)
            dead[ndead++]=i;
    }
    for(int i=ndead-1;i>=0;i--){
        int di = dead[i];
        if (di < 0 || di >= app->nws_clients) continue;
        app->ws_clients[di]=app->ws_clients[--app->nws_clients];
    }
    pthread_mutex_unlock(&app->ws_mu);
}

static void on_pipeline_run(Pipeline *p, void *ud) {
    App *app=(App*)ud;
    LOG_INFO("running pipeline %s (%s)", p->name, p->id);
    app->metrics->total_pipelines_run++;
    Arena *a=arena_create(256);
    app_ws_broadcast(app,arena_sprintf(a,"{\"event\":\"pipeline_triggered\",\"id\":\"%s\"}",p->id));
    arena_destroy(a);
    pipeline_execute_steps(p, app);
}

void app_init(App *app, const char *config_json) {
    memset(app, 0, sizeof(App));
    strncpy(app->data_dir,    DATA_DIR_DEFAULT,    sizeof(app->data_dir)-1);
    strncpy(app->db_path,     DB_PATH_DEFAULT,     sizeof(app->db_path)-1);
    strncpy(app->plugins_dir, "./build/release/lib", sizeof(app->plugins_dir)-1);
    app->port = DEFAULT_PORT;

    /* parse config */
    if (config_json && *config_json) {
        Arena *a=arena_create(4096);
        JVal *cfg=json_parse(a,config_json,strlen(config_json));
        if(cfg&&cfg->type==JV_OBJECT){
            const char *dp=json_str(json_get(cfg,"data_dir"),NULL);
            if(dp) strncpy(app->data_dir,dp,sizeof(app->data_dir)-1);
            const char *pd=json_str(json_get(cfg,"plugins_dir"),NULL);
            if(pd) strncpy(app->plugins_dir,pd,sizeof(app->plugins_dir)-1);
            int port=(int)json_int(json_get(cfg,"port"),0);
            if(port>0) app->port=port;
            app->auth_enabled = json_int(json_get(cfg,"auth_enabled"),1);  // default true
            const char *js=json_str(json_get(cfg,"jwt_secret"),NULL);
            if(js) strncpy(app->jwt_secret,js,sizeof(app->jwt_secret)-1);
            const char *ap=json_str(json_get(cfg,"admin_password"),NULL);
            if(ap) strncpy(app->admin_password,ap,sizeof(app->admin_password)-1);
        }
        arena_destroy(a);
    }
    snprintf(app->db_path,sizeof(app->db_path),"%s/catalog.db",app->data_dir);

    /* create data dir */
    mkdir(app->data_dir, 0755);

    log_init(&g_log, stderr, LOG_INFO, 0);
    LOG_INFO("DataFlow OS starting — data_dir=%s port=%d", app->data_dir, app->port);

    /* subsystems */
    app->catalog  = catalog_open(app->db_path);
    app->auth_store = auth_store_create(app->db_path);
    if (!app->auth_store) {
        LOG_ERROR("failed to create auth store");
        exit(1);
    }
    if (strlen(app->jwt_secret) == 0) {
        /* Generate random secret as 64 hex chars (no null bytes in HMAC key) */
        uint8_t raw[32];
        FILE *f = fopen("/dev/urandom", "rb");
        if (!f || fread(raw, 1, sizeof(raw), f) != sizeof(raw)) {
            LOG_ERROR("failed to generate JWT secret");
            if (f) fclose(f);
            exit(1);
        }
        fclose(f);
        for (int i = 0; i < 32; i++)
            snprintf(app->jwt_secret + i*2, 3, "%02x", raw[i]);
        app->jwt_secret[64] = '\0';
        LOG_WARN("jwt_secret not set in config — generated random (tokens won't survive restart)");
    }
    if (strlen(app->admin_password) == 0) {
        strncpy(app->admin_password, "admin", sizeof(app->admin_password)-1);
        LOG_WARN("admin_password not set in config — using default 'admin'");
    }
    app->metrics  = calloc(1, sizeof(Metrics)); metrics_init(app->metrics);
    app->workers  = tp_create(WORKER_THREADS, 256);
    app->scheduler= scheduler_create(on_pipeline_run, app);

    pthread_mutex_init(&app->tables_mu, NULL);
    pthread_mutex_init(&app->ws_mu, NULL);

    /* load tables from catalog */
    Arena *la=arena_create(16384);
    char **tnames; int tn;
    catalog_list_tables(app->catalog,&tnames,&tn,la);
    hm_init(&app->tables,NULL,64);
    for(int i=0;i<tn;i++){
        Table *t=table_open(tnames[i],app->data_dir);
        hm_set(&app->tables,tnames[i],t);
    }
    LOG_INFO("loaded %d tables from catalog", tn);

    /* load pipelines */
    char **pids; int pn;
    catalog_list_pipelines(app->catalog,&pids,&pn,la);
    for(int i=0;i<pn;i++){
        char *pjson=NULL;
        if(catalog_load_pipeline(app->catalog,pids[i],&pjson,la)==0){
            Pipeline p; memset(&p,0,sizeof(p));
            if(pipeline_from_json(&p,pjson)==0)
                scheduler_add(app->scheduler,&p);
        }
    }
    LOG_INFO("loaded %d pipelines", pn);
    arena_destroy(la);

    /* register routes */
    api_register_routes(&app->router);
    app->router.userdata = app;

    /* create HTTP server */
    app->server = http_server_create(&app->router, app->port, 128);

    /* start scheduler */
    scheduler_start(app->scheduler);
}

void app_run(App *app) {
    LOG_INFO("DataFlow OS ready — http://localhost:%d", app->port);
    http_server_run(app->server);
}

void app_stop(App *app) {
    scheduler_stop(app->scheduler);
    http_server_stop(app->server);
    tp_destroy(app->workers);
    catalog_close(app->catalog);
    LOG_INFO("DataFlow OS stopped");
}

static volatile sig_atomic_t g_shutdown = 0;

static void sig_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
    http_server_stop(g_app.server); /* safe: sets volatile int */
}

int main(int argc, char **argv) {
    char *config_json = NULL;
    for(int i=1;i<argc;i++){
        if(strcmp(argv[i],"-c")==0&&i+1<argc){
            /* read config file */
            FILE *f=fopen(argv[++i],"r"); if(!f){fprintf(stderr,"can't open config\n");return 1;}
            fseek(f,0,SEEK_END); long sz=ftell(f); rewind(f);
            config_json=malloc((size_t)sz+1);
            fread(config_json,1,(size_t)sz,f); config_json[sz]='\0'; fclose(f);
        }
        if(strcmp(argv[i],"-p")==0&&i+1<argc){
            /* inline config */
            char pbuf[64]; snprintf(pbuf,sizeof(pbuf),"{\"port\":%s}",argv[++i]);
            config_json=strdup(pbuf);
        }
    }

    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGUSR1, SIG_IGN); /* graceful reload placeholder */

    app_init(&g_app, config_json);
    app_run(&g_app);  /* blocks until http_server_stop() */
    if (g_shutdown) {
        LOG_INFO("received signal, shutting down...");
        app_stop(&g_app);
    }
    return 0;
}
