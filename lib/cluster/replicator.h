#pragma once
#include "storage_client.h"
#include <stdbool.h>
#include <pthread.h>
#include <stdint.h>

#define MAX_REPLICAS 8

typedef struct {
    StorageClient  *replicas[MAX_REPLICAS];
    int             nreplicas;
    bool            is_leader;
    char            node_id[37];
    uint64_t        wal_offset;
    pthread_mutex_t mu;
    pthread_t       hb_thread;
    volatile int    running;
} Replicator;

Replicator *replicator_create(bool is_leader, const char *node_id);
void        replicator_destroy(Replicator *r);

int  replicator_add_replica(Replicator *r, const char *host, int port);
int  replicator_replicate(Replicator *r, const char *table_name,
                          uint64_t offset, ColBatch *batch);
void replicator_get_status(Replicator *r, char *json_out, size_t len);
