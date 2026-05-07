#pragma once
#include "storage_client.h"
#include "../storage/storage.h"
#include <stdbool.h>
#include <pthread.h>
#include <stdint.h>

#define MAX_REPLICAS   8
#define REPL_QUEUE_CAP 1024

typedef struct {
    char     table_name[128];
    uint64_t lsn;
    void    *data;  /* malloc'd copy of WAL payload */
    size_t   len;
} ReplItem;

typedef struct {
    StorageClient  *replicas[MAX_REPLICAS];
    int             nreplicas;
    bool            is_leader;
    char            node_id[37];

    /* async work queue */
    ReplItem        queue[REPL_QUEUE_CAP];
    int             head, tail, count;
    pthread_mutex_t q_mu;
    pthread_cond_t  q_cv;
    pthread_t       worker;
    volatile int    running;

    /* metrics */
    uint64_t        last_acked_lsn;
    int             lag_count;
} Replicator;

Replicator *replicator_create(bool is_leader, const char *node_id);
void        replicator_destroy(Replicator *r);

int  replicator_add_replica(Replicator *r, const char *host, int port);

/* Called from WAL callback — enqueues (non-blocking, drops on overflow) */
void replicator_enqueue(Replicator *r, const char *table_name,
                        uint64_t lsn, const void *data, size_t len);

/* WalWriteCallback-compatible signature — can be passed to table_set_wal_callback */
void replicator_wal_cb(const char *table_name, uint64_t lsn,
                       const void *data, size_t len, void *userdata);

void replicator_get_status(Replicator *r, char *json_out, size_t len);
