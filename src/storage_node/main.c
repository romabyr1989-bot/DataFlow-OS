#include "../../lib/cluster/proto.h"
#include "../../lib/storage/storage.h"
#include "../../lib/core/log.h"
#include "../../lib/core/arena.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>

static volatile sig_atomic_t g_shutdown = 0;
static void sig_handler(int sig) { (void)sig; g_shutdown = 1; }

static char     g_data_dir[512] = "./data_node";
static Catalog *g_catalog       = NULL;

typedef struct { int fd; } ClientCtx;

static void handle_replicate(int fd, ProtoHeader *hdr, void *body) {
    if (!body || hdr->body_len < sizeof(ProtoReplicateHdr)) {
        proto_send(fd, MSG_ACK, hdr->request_id, NULL, 0);
        return;
    }
    ProtoReplicateHdr *rhdr = (ProtoReplicateHdr *)body;
    LOG_INFO("storage_node: replicate table=%s offset=%llu nrows=%u",
             rhdr->table_name,
             (unsigned long long)rhdr->offset,
             rhdr->nrows);
    proto_send(fd, MSG_ACK, hdr->request_id, NULL, 0);
}

static void *client_thread_fn(void *arg) {
    ClientCtx *ctx = (ClientCtx *)arg;
    int fd = ctx->fd;
    free(ctx);
    LOG_INFO("storage_node: client connected fd=%d", fd);
    while (!g_shutdown) {
        ProtoHeader hdr;
        void *body = NULL;
        if (proto_recv(fd, &hdr, &body, NULL) < 0) break;
        switch ((ProtoMsgType)hdr.msg_type) {
        case MSG_PING:
            proto_send(fd, MSG_PONG, hdr.request_id, NULL, 0);
            break;
        case MSG_REPLICATE:
            handle_replicate(fd, &hdr, body);
            break;
        case MSG_STATUS_REQ: {
            ProtoStatusBody sb;
            memset(&sb, 0, sizeof(sb));
            sb.is_leader     = 0;
            sb.wal_offset    = 0;
            sb.replica_count = 0;
            proto_send(fd, MSG_STATUS_RESP, hdr.request_id, &sb, sizeof(sb));
            break;
        }
        default:
            LOG_WARN("storage_node: unknown msg_type=%d", hdr.msg_type);
            break;
        }
        proto_free_body(body);
    }
    close(fd);
    return NULL;
}

int main(int argc, char **argv) {
    int port = 9090;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) port = atoi(argv[++i]);
        if (strcmp(argv[i], "-d") == 0 && i + 1 < argc)
            strncpy(g_data_dir, argv[++i], sizeof(g_data_dir) - 1);
    }

    log_init(&g_log, stderr, LOG_INFO, 0);
    LOG_INFO("storage_node starting — port=%d data_dir=%s", port, g_data_dir);

    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);
    signal(SIGPIPE, SIG_IGN);

    char db_path[512];
    snprintf(db_path, sizeof(db_path), "%s/catalog.db", g_data_dir);
    mkdir(g_data_dir, 0755);
    g_catalog = catalog_open(db_path);

    int srv_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(srv_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons((uint16_t)port);

    if (bind(srv_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        LOG_ERROR("storage_node: bind failed");
        return 1;
    }
    listen(srv_fd, 16);
    LOG_INFO("storage_node listening on :%d", port);

    while (!g_shutdown) {
        struct sockaddr_in peer;
        socklen_t plen = sizeof(peer);
        int cfd = accept(srv_fd, (struct sockaddr *)&peer, &plen);
        if (cfd < 0) continue;
        ClientCtx *ctx = malloc(sizeof(ClientCtx));
        ctx->fd = cfd;
        pthread_t t;
        pthread_create(&t, NULL, client_thread_fn, ctx);
        pthread_detach(t);
    }

    close(srv_fd);
    catalog_close(g_catalog);
    LOG_INFO("storage_node stopped");
    return 0;
}
