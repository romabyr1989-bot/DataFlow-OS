#pragma once
#include "../core/arena.h"
#include "../core/json.h"
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

#define MAX_STEPS 64

typedef enum { STEP_PENDING=0, STEP_RUNNING, STEP_SUCCESS, STEP_FAILED } StepStatus;
typedef enum { RUN_PENDING=0, RUN_RUNNING, RUN_SUCCESS, RUN_FAILED, RUN_CANCELLED } RunStatus;

typedef struct {
    char        id[64];
    char        name[128];
    char        connector_type[64];
    char        connector_config[1024];
    char        transform_sql[4096];
    char        target_table[128];
    int         deps[MAX_STEPS];
    int         ndeps;
    StepStatus  status;

    /* NEW: retry policy */
    int         max_retries;       /* default: 3 */
    int         retry_count;       /* current retry counter */
    int         retry_delay_sec;   /* base delay, doubles on each retry */
    int64_t     retry_after;       /* unix ts — when to retry */
} PipelineStep;

typedef struct {
    char          id[64];
    char          name[128];
    char          cron[64];      /* cron expression, e.g. every-5-min or @hourly */
    PipelineStep  steps[MAX_STEPS];
    int           nsteps;
    bool          enabled;
    int64_t       last_run;
    int64_t       next_run;
    RunStatus     run_status;
    char          error_msg[512];

    /* NEW: alerting */
    char          webhook_url[512];  /* Slack/Telegram/custom webhook */
    char          webhook_on[32];    /* "failure", "success", "all" */
    int           alert_cooldown;    /* seconds between alerts */
    int64_t       last_alert_at;
} Pipeline;

/* Callback invoked in scheduler thread when a pipeline should run */
typedef void (*RunCallback)(Pipeline *p, void *userdata);

typedef struct {
    Pipeline       pipelines[256];
    int            npipelines;
    pthread_t      thread;
    pthread_mutex_t mu;
    volatile int   running;
    RunCallback    on_run;
    void          *on_run_data;
} Scheduler;

/* Cron */
int64_t cron_next(const char *expr, int64_t after_ts);  /* returns unix ts */

/* Pipeline JSON de/serialization */
int  pipeline_from_json(Pipeline *p, const char *json);
char *pipeline_to_json(const Pipeline *p, Arena *a);

/* Scheduler */
Scheduler *scheduler_create(RunCallback cb, void *userdata);
void       scheduler_add(Scheduler *s, Pipeline *p);
void       scheduler_remove(Scheduler *s, const char *id);
Pipeline  *scheduler_find(Scheduler *s, const char *id);
void       scheduler_start(Scheduler *s);
void       scheduler_stop(Scheduler *s);
