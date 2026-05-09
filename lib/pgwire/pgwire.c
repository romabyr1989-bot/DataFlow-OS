/* pgwire.c — see pgwire.h.
 *
 * Per-connection threading: the accept loop spawns a detached pthread for
 * each socket. Connection state lives on its stack frame, and the wire
 * protocol is fully synchronous, so no locking is needed within a
 * connection. The server-level state (running flag) is protected by an
 * atomic. */
#include "pgwire.h"
#include "../core/log.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

#define PG_PROTOCOL_V3   196608        /* 0x00030000 */
#define PG_SSL_REQUEST   80877103      /* 0x04D2162F */
#define PG_CANCEL_REQUEST 80877102

#define PG_BUF_MAX (1024 * 1024)       /* per-message ceiling */

/* ── Server state ─────────────────────────────────────────────── */
struct PgWireServer {
    int               port;
    int               listen_fd;
    PgWireCallbacks   cbs;
    void             *ud;
    pthread_t         accept_thread;
    volatile int      running;
};

/* ── Per-connection state ─────────────────────────────────────── */
struct PgConn {
    int               fd;
    PgWireServer     *srv;
    char              user[64];
    char              database[64];
    int               authenticated;
    /* Cancel keys (we just emit zeroes; we don't yet support cancel) */
    int32_t           proc_id;
    int32_t           secret_key;
};

/* ── I/O helpers ──────────────────────────────────────────────── */
static int read_full(int fd, void *buf, size_t n) {
    char *p = buf;
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(fd, p + got, n - got);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return -1;
        got += (size_t)r;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t n) {
    const char *p = buf;
    size_t sent = 0;
    while (sent < n) {
        ssize_t w = send(fd, p + sent, n - sent, MSG_NOSIGNAL);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return -1;
        sent += (size_t)w;
    }
    return 0;
}

static uint32_t be32(const uint8_t *p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] <<  8) |  (uint32_t)p[3];
}

static void put_be32(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >>  8); p[3] = (uint8_t) v;
}

static void put_be16(uint8_t *p, uint16_t v) {
    p[0] = (uint8_t)(v >> 8); p[1] = (uint8_t)v;
}

/* ── Message construction ─────────────────────────────────────── */
typedef struct { uint8_t *buf; size_t len, cap; } MsgBuf;

static void mb_init(MsgBuf *m) {
    m->cap = 256;
    m->len = 5;                /* reserve type + length */
    m->buf = malloc(m->cap);
}

static void mb_reserve(MsgBuf *m, size_t add) {
    if (m->len + add <= m->cap) return;
    while (m->len + add > m->cap) m->cap *= 2;
    m->buf = realloc(m->buf, m->cap);
}

static void mb_u8 (MsgBuf *m, uint8_t v)  { mb_reserve(m, 1); m->buf[m->len++] = v; }
static void mb_u16(MsgBuf *m, uint16_t v) { mb_reserve(m, 2); put_be16(m->buf + m->len, v); m->len += 2; }
static void mb_u32(MsgBuf *m, uint32_t v) { mb_reserve(m, 4); put_be32(m->buf + m->len, v); m->len += 4; }
static void mb_bytes(MsgBuf *m, const void *p, size_t n) {
    mb_reserve(m, n); memcpy(m->buf + m->len, p, n); m->len += n;
}
static void mb_cstr(MsgBuf *m, const char *s) {
    size_t n = strlen(s) + 1; mb_bytes(m, s, n);
}

/* Finalize and write to fd. type==0 means typeless (StartupMessage style). */
static int mb_send(int fd, MsgBuf *m, uint8_t type) {
    int rc;
    if (type == 0) {
        /* No type byte; length is over the entire body including length field */
        uint32_t L = (uint32_t)(m->len - 1);  /* slot 0 unused, body in [1..len) */
        put_be32(m->buf + 1, L);
        rc = write_full(fd, m->buf + 1, m->len - 1);
    } else {
        m->buf[0] = type;
        uint32_t L = (uint32_t)(m->len - 1);  /* length excludes the type byte */
        put_be32(m->buf + 1, L);
        rc = write_full(fd, m->buf, m->len);
    }
    free(m->buf);
    return rc;
}

/* ── Outgoing protocol messages ───────────────────────────────── */
static int send_auth_cleartext(int fd) {
    MsgBuf m; mb_init(&m); mb_u32(&m, 3); /* AuthenticationCleartextPassword */
    return mb_send(fd, &m, 'R');
}

static int send_auth_ok(int fd) {
    MsgBuf m; mb_init(&m); mb_u32(&m, 0);
    return mb_send(fd, &m, 'R');
}

static int send_param_status(int fd, const char *k, const char *v) {
    MsgBuf m; mb_init(&m); mb_cstr(&m, k); mb_cstr(&m, v);
    return mb_send(fd, &m, 'S');
}

static int send_backend_key_data(int fd, int32_t pid, int32_t key) {
    MsgBuf m; mb_init(&m);
    mb_u32(&m, (uint32_t)pid);
    mb_u32(&m, (uint32_t)key);
    return mb_send(fd, &m, 'K');
}

static int send_ready_for_query(int fd, char status) {
    MsgBuf m; mb_init(&m); mb_u8(&m, (uint8_t)status);
    return mb_send(fd, &m, 'Z');
}

/* ── Public helpers — emit query results ──────────────────────── */
void pgwire_send_row_description(PgConn *c, int ncols, const PgColumn *cols) {
    MsgBuf m; mb_init(&m);
    mb_u16(&m, (uint16_t)ncols);
    for (int i = 0; i < ncols; i++) {
        mb_cstr(&m, cols[i].name);
        mb_u32(&m, 0);                       /* table OID */
        mb_u16(&m, 0);                       /* column attr number */
        mb_u32(&m, (uint32_t)cols[i].type_oid);
        mb_u16(&m, (uint16_t)cols[i].type_size);
        mb_u32(&m, 0xFFFFFFFFu);             /* type modifier (-1) */
        mb_u16(&m, 0);                       /* format code = text */
    }
    mb_send(c->fd, &m, 'T');
}

void pgwire_send_data_row(PgConn *c, int ncols, const char *const *values) {
    MsgBuf m; mb_init(&m);
    mb_u16(&m, (uint16_t)ncols);
    for (int i = 0; i < ncols; i++) {
        if (!values[i]) {
            mb_u32(&m, 0xFFFFFFFFu);          /* NULL marker (-1) */
        } else {
            uint32_t L = (uint32_t)strlen(values[i]);
            mb_u32(&m, L);
            mb_bytes(&m, values[i], L);
        }
    }
    mb_send(c->fd, &m, 'D');
}

void pgwire_send_command_complete(PgConn *c, const char *tag) {
    MsgBuf m; mb_init(&m); mb_cstr(&m, tag);
    mb_send(c->fd, &m, 'C');
}

void pgwire_send_error(PgConn *c, const char *sqlstate, const char *msg) {
    MsgBuf m; mb_init(&m);
    mb_u8(&m, 'S'); mb_cstr(&m, "ERROR");
    mb_u8(&m, 'C'); mb_cstr(&m, sqlstate ? sqlstate : "XX000");
    mb_u8(&m, 'M'); mb_cstr(&m, msg ? msg : "internal error");
    mb_u8(&m, 0);                              /* terminator */
    mb_send(c->fd, &m, 'E');
}

const char *pgwire_user    (PgConn *c) { return c->user; }
const char *pgwire_database(PgConn *c) { return c->database; }

/* ── Startup handshake ────────────────────────────────────────── */
/* Returns 0 to continue, 1 if the client just probed SSL and should
 * be re-read, -1 on fatal error. */
static int read_startup(PgConn *c, uint8_t *body, uint32_t *body_len_out) {
    uint8_t hdr[4];
    if (read_full(c->fd, hdr, 4) != 0) return -1;
    uint32_t total = be32(hdr);
    if (total < 8 || total > PG_BUF_MAX) return -1;
    uint32_t body_len = total - 4;
    if (read_full(c->fd, body, body_len) != 0) return -1;
    *body_len_out = body_len;

    uint32_t code = be32(body);
    if (code == PG_SSL_REQUEST) {
        /* Reply 'N' — we don't speak SSL yet; client falls back to plaintext. */
        char n = 'N';
        if (write_full(c->fd, &n, 1) != 0) return -1;
        return 1;
    }
    if (code == PG_CANCEL_REQUEST) {
        /* No-op for now; clients connect, send cancel, close. */
        return -1;
    }
    if (code != PG_PROTOCOL_V3) {
        LOG_WARN("pgwire: unsupported protocol version %u", code);
        return -1;
    }
    return 0;
}

static void parse_startup_kv(PgConn *c, const uint8_t *body, uint32_t body_len) {
    /* body = 4-byte version (already validated) + key/value cstrings + null */
    const char *p   = (const char *)body + 4;
    const char *end = (const char *)body + body_len;
    while (p < end && *p) {
        const char *key = p;
        while (p < end && *p) p++;
        if (p >= end) break;
        p++;                                /* skip key NUL */
        const char *val = p;
        while (p < end && *p) p++;
        if (p >= end) break;
        if      (strcmp(key, "user")     == 0) strncpy(c->user,     val, sizeof(c->user)     - 1);
        else if (strcmp(key, "database") == 0) strncpy(c->database, val, sizeof(c->database) - 1);
        p++;                                /* skip value NUL */
    }
    if (!c->database[0]) strncpy(c->database, c->user, sizeof(c->database) - 1);
}

/* ── Main per-connection loop ─────────────────────────────────── */
static int handle_password(PgConn *c) {
    uint8_t  hdr[5];
    if (read_full(c->fd, hdr, 5) != 0) return -1;
    if (hdr[0] != 'p') return -1;
    uint32_t L = be32(hdr + 1);
    if (L < 5 || L - 4 > PG_BUF_MAX) return -1;
    uint32_t body_len = L - 4;
    char *pw = malloc(body_len + 1);
    if (!pw) return -1;
    if (read_full(c->fd, pw, body_len) != 0) { free(pw); return -1; }
    pw[body_len] = '\0';                    /* defensive */
    /* PasswordMessage body: cstring */
    int rc = -1;
    if (c->srv->cbs.authenticate) {
        rc = c->srv->cbs.authenticate(c->user, pw, c->database, c->srv->ud);
    }
    /* zero-out the password buffer ASAP */
    memset(pw, 0, body_len);
    free(pw);
    return rc;
}

static int run_query_loop(PgConn *c) {
    while (c->srv->running) {
        uint8_t hdr[5];
        if (read_full(c->fd, hdr, 5) != 0) return -1;
        uint8_t  type = hdr[0];
        uint32_t L    = be32(hdr + 1);
        if (L < 4 || L - 4 > PG_BUF_MAX) return -1;
        uint32_t body_len = L - 4;
        uint8_t *body = body_len ? malloc(body_len) : NULL;
        if (body_len && !body) return -1;
        if (body_len && read_full(c->fd, body, body_len) != 0) { free(body); return -1; }

        switch (type) {
            case 'Q': {
                /* Query: cstring */
                const char *sql = body_len ? (const char *)body : "";
                if (c->srv->cbs.query) {
                    c->srv->cbs.query(c, sql, c->srv->ud);
                } else {
                    pgwire_send_error(c, "0A000", "no query handler installed");
                }
                send_ready_for_query(c->fd, 'I');
                break;
            }
            case 'X':                       /* Terminate */
                free(body);
                return 0;
            case 'P': case 'B': case 'E': case 'D':
            case 'C': case 'S': case 'H': case 'F': case 'd': case 'f':
                /* Extended-query / COPY messages — Week 2/4 territory.
                 * Send a clear error and continue; psql falls back gracefully. */
                pgwire_send_error(c, "0A000",
                    "extended query / COPY not yet implemented (Week 2/4)");
                send_ready_for_query(c->fd, 'I');
                break;
            default:
                LOG_WARN("pgwire: unknown message type 0x%02X (%c)", type,
                         (type >= 32 && type < 127) ? type : '?');
                pgwire_send_error(c, "08P01", "unknown protocol message");
                send_ready_for_query(c->fd, 'I');
                break;
        }
        free(body);
    }
    return 0;
}

static void *connection_thread(void *arg) {
    PgConn *c = arg;
    LOG_INFO("pgwire: client connected fd=%d", c->fd);

    uint8_t body[8192];
    uint32_t body_len = 0;
    int sr = read_startup(c, body, &body_len);
    if (sr == 1) {
        /* SSLRequest: we replied 'N', read the real StartupMessage now */
        sr = read_startup(c, body, &body_len);
    }
    if (sr != 0) goto done;

    parse_startup_kv(c, body, body_len);
    if (!c->user[0]) {
        pgwire_send_error(c, "28000", "missing user in startup");
        goto done;
    }

    /* Cleartext auth round-trip */
    if (send_auth_cleartext(c->fd) != 0) goto done;
    if (handle_password(c) != 0) {
        pgwire_send_error(c, "28P01", "password authentication failed");
        goto done;
    }
    c->authenticated = 1;
    c->proc_id    = (int32_t)(intptr_t)c & 0x7FFFFFFF;
    c->secret_key = 0;

    if (send_auth_ok(c->fd)                                      != 0 ||
        send_param_status(c->fd, "client_encoding", "UTF8")      != 0 ||
        send_param_status(c->fd, "DateStyle",       "ISO, MDY")  != 0 ||
        send_param_status(c->fd, "TimeZone",        "UTC")       != 0 ||
        send_param_status(c->fd, "server_version",  "16.0 (DataFlow OS)") != 0 ||
        send_backend_key_data(c->fd, c->proc_id, c->secret_key)  != 0 ||
        send_ready_for_query(c->fd, 'I')                         != 0)
        goto done;

    LOG_INFO("pgwire: auth ok user=%s db=%s", c->user, c->database);
    run_query_loop(c);

done:
    close(c->fd);
    LOG_INFO("pgwire: client disconnected");
    free(c);
    return NULL;
}

/* ── Accept loop ──────────────────────────────────────────────── */
static void *accept_thread_fn(void *arg) {
    PgWireServer *s = arg;
    LOG_INFO("pgwire: listening on :%d", s->port);
    while (s->running) {
        struct sockaddr_in peer;
        socklen_t plen = sizeof(peer);
        int cfd = accept(s->listen_fd, (struct sockaddr *)&peer, &plen);
        if (cfd < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
            if (!s->running) break;
            LOG_WARN("pgwire: accept failed: %s", strerror(errno));
            continue;
        }
        PgConn *c = calloc(1, sizeof(*c));
        if (!c) { close(cfd); continue; }
        c->fd  = cfd;
        c->srv = s;
        pthread_t t;
        if (pthread_create(&t, NULL, connection_thread, c) != 0) {
            close(cfd); free(c); continue;
        }
        pthread_detach(t);
    }
    LOG_INFO("pgwire: accept loop stopped");
    return NULL;
}

/* ── Public lifecycle ─────────────────────────────────────────── */
PgWireServer *pgwire_create(int port, PgWireCallbacks cbs, void *ud) {
    PgWireServer *s = calloc(1, sizeof(*s));
    if (!s) return NULL;
    s->port      = port;
    s->cbs       = cbs;
    s->ud        = ud;
    s->listen_fd = -1;
    return s;
}

int pgwire_start(PgWireServer *s) {
    if (!s) return -1;
    s->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (s->listen_fd < 0) { LOG_ERROR("pgwire: socket failed"); return -1; }
    int opt = 1;
    setsockopt(s->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons((uint16_t)s->port);
    if (bind(s->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        LOG_ERROR("pgwire: bind :%d failed: %s", s->port, strerror(errno));
        close(s->listen_fd); s->listen_fd = -1; return -1;
    }
    if (listen(s->listen_fd, 32) < 0) {
        LOG_ERROR("pgwire: listen failed");
        close(s->listen_fd); s->listen_fd = -1; return -1;
    }
    s->running = 1;
    if (pthread_create(&s->accept_thread, NULL, accept_thread_fn, s) != 0) {
        LOG_ERROR("pgwire: pthread_create failed");
        s->running = 0;
        close(s->listen_fd); s->listen_fd = -1; return -1;
    }
    return 0;
}

void pgwire_stop(PgWireServer *s) {
    if (!s || !s->running) return;
    s->running = 0;
    if (s->listen_fd >= 0) {
        shutdown(s->listen_fd, SHUT_RDWR);
        close(s->listen_fd);
        s->listen_fd = -1;
    }
    pthread_join(s->accept_thread, NULL);
}

void pgwire_destroy(PgWireServer *s) {
    if (!s) return;
    pgwire_stop(s);
    free(s);
}
