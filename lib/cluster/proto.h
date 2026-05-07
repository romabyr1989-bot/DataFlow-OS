#pragma once
#include <stdint.h>
#include <stddef.h>

#define PROTO_MAGIC   0xDF0A
#define PROTO_VERSION 1

typedef enum {
    MSG_PING        = 1,
    MSG_PONG        = 2,
    MSG_REPLICATE   = 3,
    MSG_ACK         = 4,
    MSG_PROMOTE     = 5,
    MSG_STATUS_REQ  = 6,
    MSG_STATUS_RESP = 7,
} ProtoMsgType;

typedef struct __attribute__((packed)) {
    uint16_t magic;       /* 0xDF0A */
    uint8_t  version;     /* PROTO_VERSION */
    uint8_t  msg_type;    /* ProtoMsgType */
    uint32_t request_id;
    uint32_t body_len;
    uint32_t reserved;
} ProtoHeader;

typedef struct __attribute__((packed)) {
    char     table_name[128];
    uint64_t offset;
    uint32_t nrows;
} ProtoReplicateHdr;

typedef struct __attribute__((packed)) {
    uint8_t  is_leader;
    uint64_t wal_offset;
    uint32_t replica_count;
    char     node_id[37];
} ProtoStatusBody;

int  proto_send(int fd, ProtoMsgType type, uint32_t req_id,
                const void *body, uint32_t body_len);
int  proto_recv(int fd, ProtoHeader *hdr, void **body_out, size_t *body_len_out);
void proto_free_body(void *body);
