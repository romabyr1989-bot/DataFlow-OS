#pragma once
#include "proto.h"
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

typedef struct {
    int      fd;
    char     host[256];
    int      port;
    bool     connected;
    uint32_t next_req_id;
} StorageClient;

StorageClient *storage_client_create(const char *host, int port);
void           storage_client_destroy(StorageClient *c);
int            storage_client_connect(StorageClient *c);
void           storage_client_disconnect(StorageClient *c);
int            storage_client_ping(StorageClient *c);
int            storage_client_replicate(StorageClient *c, const char *table_name,
                                        uint64_t lsn, const void *wal_data, size_t wal_len);
int            storage_client_status(StorageClient *c, ProtoStatusBody *out);
