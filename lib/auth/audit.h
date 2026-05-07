#pragma once
#include "auth.h"
#include "../core/arena.h"
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>

typedef enum {
    AUDIT_QUERY         = 1,
    AUDIT_INGEST        = 2,
    AUDIT_PIPELINE_RUN  = 3,
    AUDIT_AUTH_LOGIN    = 4,
    AUDIT_AUTH_FAIL     = 5,
    AUDIT_SCHEMA_CHANGE = 6,
    AUDIT_POLICY_CHANGE = 7,
} AuditEventType;

typedef struct {
    AuditEventType type;
    const char    *user_id;
    AuthRole       role;
    const char    *resource;
    const char    *action_detail;  /* SQL, обрезается до 4096 */
    const char    *correlation_id;
    const char    *client_ip;
    int            result_code;
    int64_t        duration_ms;
} AuditEvent;

#define AUDIT_RING_SIZE 1024

typedef struct {
    AuditEventType type;
    char  user_id[64];
    int   role;
    char  resource[128];
    char  action_detail[4096];
    char  correlation_id[37];
    char  client_ip[64];
    int   result_code;
    int64_t duration_ms;
    int64_t ts;
} AuditRecord;

typedef struct AuditLog {
    sqlite3        *db;
    pthread_mutex_t db_mu;

    /* Ring buffer */
    AuditRecord     ring[AUDIT_RING_SIZE];
    volatile int    head; /* producer writes here */
    volatile int    tail; /* consumer reads from here */
    pthread_mutex_t ring_mu;
    pthread_cond_t  ring_cond;

    /* Background flush thread */
    pthread_t       flush_thread;
    volatile int    running;

    /* Optional JSONL file */
    FILE           *log_file;
    pthread_mutex_t file_mu;
} AuditLog;

AuditLog *audit_log_create(const char *db_path, const char *log_file_path);
void      audit_log_destroy(AuditLog *l);

/* Non-blocking: кладёт в кольцевой буфер */
void audit_log_event(AuditLog *l, const AuditEvent *ev);

int audit_log_query(AuditLog *l, const char *user_id_filter,
                    int64_t from_ts, int64_t to_ts,
                    int limit, char **json_out, Arena *a);
