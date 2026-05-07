#include "replicator.h"
#include "../core/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

static void *heartbeat_thread_fn(void *arg) {
    Replicator *r = (Replicator *)arg;
    while (r->running) {
        struct timespec ts = { .tv_sec = 5, .tv_nsec = 0 };
        nanosleep(&ts, NULL);
        if (!r->is_leader || !r->running) continue;
        pthread_mutex_lock(&r->mu);
        for (int i = 0; i < r->nreplicas; i++) {
            if (storage_client_ping(r->replicas[i]) < 0)
                LOG_WARN("replicator: replica %s:%d unreachable",
                         r->replicas[i]->host, r->replicas[i]->port);
        }
        pthread_mutex_unlock(&r->mu);
    }
    return NULL;
}

Replicator *replicator_create(bool is_leader, const char *node_id) {
    Replicator *r = calloc(1, sizeof(Replicator));
    r->is_leader = is_leader;
    if (node_id) strncpy(r->node_id, node_id, sizeof(r->node_id) - 1);
    pthread_mutex_init(&r->mu, NULL);
    r->running = 1;
    pthread_create(&r->hb_thread, NULL, heartbeat_thread_fn, r);
    return r;
}

void replicator_destroy(Replicator *r) {
    if (!r) return;
    r->running = 0;
    pthread_join(r->hb_thread, NULL);
    pthread_mutex_lock(&r->mu);
    for (int i = 0; i < r->nreplicas; i++)
        storage_client_destroy(r->replicas[i]);
    pthread_mutex_unlock(&r->mu);
    pthread_mutex_destroy(&r->mu);
    free(r);
}

int replicator_add_replica(Replicator *r, const char *host, int port) {
    pthread_mutex_lock(&r->mu);
    if (r->nreplicas >= MAX_REPLICAS) {
        pthread_mutex_unlock(&r->mu);
        return -1;
    }
    r->replicas[r->nreplicas++] = storage_client_create(host, port);
    pthread_mutex_unlock(&r->mu);
    return 0;
}

int replicator_replicate(Replicator *r, const char *table_name,
                         uint64_t offset, ColBatch *batch) {
    if (!r->is_leader) return 0;
    int errors = 0;
    pthread_mutex_lock(&r->mu);
    for (int i = 0; i < r->nreplicas; i++) {
        if (storage_client_replicate(r->replicas[i], table_name, offset, batch) < 0) {
            LOG_WARN("replicator: failed to replicate to %s:%d",
                     r->replicas[i]->host, r->replicas[i]->port);
            errors++;
        }
    }
    r->wal_offset = offset;
    pthread_mutex_unlock(&r->mu);
    return (r->nreplicas > 0 && errors == r->nreplicas) ? -1 : 0;
}

void replicator_get_status(Replicator *r, char *json_out, size_t len) {
    pthread_mutex_lock(&r->mu);
    int off = snprintf(json_out, len,
        "{\"is_leader\":%s,\"node_id\":\"%s\","
        "\"wal_offset\":%llu,\"replica_count\":%d,\"replicas\":[",
        r->is_leader ? "true" : "false", r->node_id,
        (unsigned long long)r->wal_offset, r->nreplicas);
    for (int i = 0; i < r->nreplicas && off < (int)len - 10; i++) {
        StorageClient *sc = r->replicas[i];
        if (i > 0) off += snprintf(json_out + off, len - (size_t)off, ",");
        off += snprintf(json_out + off, len - (size_t)off,
            "{\"host\":\"%s\",\"port\":%d,\"connected\":%s}",
            sc->host, sc->port, sc->connected ? "true" : "false");
    }
    snprintf(json_out + off, len - (size_t)off, "]}");
    pthread_mutex_unlock(&r->mu);
}
