/* airbyte_runner.c — DFO connector that runs any Airbyte source image.
 *
 * Airbyte Connector Protocol (https://docs.airbyte.com/understanding-airbyte/airbyte-protocol):
 *   Each connector is a Docker image. Commands: spec, check, discover, read.
 *   All I/O is JSON-on-stdio, one message per line.
 *
 * This plugin shells out to `docker run` (or `podman run`) for each command
 * and adapts the responses to DFO's connector ABI v1.
 *
 * Config format (JSON):
 *   {
 *     "image":   "airbyte/source-postgres:3.6.16",
 *     "config":  { … inner config matching the source's spec … },
 *     "runtime": "docker"|"podman"|"auto"   (optional; default auto)
 *   }
 *
 * Security:
 *   - image must start with "airbyte/source-" or "airbyte/destination-"
 *   - container runs with --memory=512m --cpus=1 --rm
 *   - config is mounted read-only into /secrets
 *   - never --privileged, never extra mounts
 */
#include "../../connector.h"
#include "../../../core/log.h"
#include "../../../core/json.h"
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <signal.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>

#define AB_LINE_BUF       (256 * 1024)   /* one Airbyte message ≤ 256 KB */
#define AB_CATALOG_BUF    (256 * 1024)
#define AB_STATE_BUF      (16 * 1024)
#define AB_DISCOVER_TIMEOUT 60            /* seconds */

typedef enum { CR_DOCKER, CR_PODMAN } ContainerRuntime;

typedef struct {
    /* Configuration */
    char  image[256];
    char  config_json[8192];     /* serialized inner config */
    ContainerRuntime runtime;

    /* Workspace — created in airbyte_create, deleted in airbyte_destroy */
    char  workdir[256];
    int   workdir_created;

    /* Cached catalog (filled by list_entities, reused by describe & read) */
    char *catalog_json;
    size_t catalog_len;

    /* State.json contents for incremental reads */
    char  state_json[AB_STATE_BUF];

    /* Active read subprocess (NULL between batches) */
    pid_t read_pid;
    int   read_fd;
    char  reading_entity[128];
    Schema *reading_schema;

    /* Line-buffered reader state for read_fd */
    char *line_buf;
    int   line_pos;

    Arena *arena;                /* persistent arena (lives for ctx lifetime) */
} AirbyteCtx;

/* ── Whitelist ─────────────────────────────────────────────────── */
static int is_whitelisted_image(const char *image) {
    return strncmp(image, "airbyte/source-",      15) == 0
        || strncmp(image, "airbyte/destination-", 20) == 0;
}

/* ── Container runtime detection ───────────────────────────────── */
static int file_is_executable(const char *path) {
    return access(path, X_OK) == 0;
}

static ContainerRuntime detect_runtime(const char *prefer) {
    if (prefer && strcmp(prefer, "docker") == 0) return CR_DOCKER;
    if (prefer && strcmp(prefer, "podman") == 0) return CR_PODMAN;
    /* auto */
    if (file_is_executable("/usr/local/bin/docker") ||
        file_is_executable("/opt/homebrew/bin/docker") ||
        file_is_executable("/usr/bin/docker")) return CR_DOCKER;
    if (file_is_executable("/usr/local/bin/podman") ||
        file_is_executable("/opt/homebrew/bin/podman") ||
        file_is_executable("/usr/bin/podman")) return CR_PODMAN;
    return CR_DOCKER; /* fallback — exec will fail with a clear error */
}

static const char *runtime_name(ContainerRuntime r) {
    return r == CR_PODMAN ? "podman" : "docker";
}

/* ── JVal → JSON text (recursive) ─────────────────────────────── */
static void jval_emit(JBuf *b, JVal *v) {
    if (!v) { jb_null(b); return; }
    switch (v->type) {
        case JV_NULL:   jb_null(b); break;
        case JV_BOOL:   jb_bool(b, v->b); break;
        case JV_NUMBER: {
            double d = v->n;
            if (d == (long long)d) jb_int(b, (long long)d);
            else                   jb_double(b, d);
            break;
        }
        case JV_STRING: jb_strn(b, v->s, v->len); break;
        case JV_ARRAY:
            jb_arr_begin(b);
            for (size_t i = 0; i < v->nitems; i++) jval_emit(b, v->items[i]);
            jb_arr_end(b);
            break;
        case JV_OBJECT:
            jb_obj_begin(b);
            for (size_t i = 0; i < v->nkeys; i++) {
                jb_key(b, v->keys[i]);
                jval_emit(b, v->vals[i]);
            }
            jb_obj_end(b);
            break;
        default: jb_null(b); break;
    }
}

/* ── File helpers ─────────────────────────────────────────────── */
static int write_text_file(const char *path, const char *content, size_t len) {
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    size_t w = fwrite(content, 1, len, f);
    fclose(f);
    return w == len ? 0 : -1;
}

/* Recursive `rm -rf` via system() — safe because path is under /tmp/airbyte_<pid>_… */
static void rm_rf_workdir(const char *path) {
    if (!path || !path[0]) return;
    if (strncmp(path, "/tmp/airbyte_", 13) != 0) return; /* paranoia */
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    int rc = system(cmd);
    (void)rc;
}

/* ── Pipe-based line reader ───────────────────────────────────── */
/* Read one '\n'-terminated line into a static-ish buffer (ctx-owned).
 * Returns NUL-terminated pointer on success, NULL on EOF/error. */
static char *read_line(AirbyteCtx *c, char *out, size_t out_cap) {
    while (1) {
        /* scan for newline in current buffer */
        for (int i = 0; i < c->line_pos; i++) {
            if (c->line_buf[i] == '\n') {
                size_t n = (size_t)i;
                if (n + 1 > out_cap) n = out_cap - 1;
                memcpy(out, c->line_buf, n);
                out[n] = '\0';
                int remain = c->line_pos - (i + 1);
                if (remain > 0) memmove(c->line_buf, c->line_buf + i + 1, remain);
                c->line_pos = remain;
                return out;
            }
        }
        if (c->line_pos >= AB_LINE_BUF - 1) {
            /* line too long — discard and return error */
            c->line_pos = 0;
            return NULL;
        }
        ssize_t n = read(c->read_fd, c->line_buf + c->line_pos,
                         AB_LINE_BUF - c->line_pos - 1);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            /* EOF — flush whatever's buffered as the final line */
            if (c->line_pos > 0) {
                size_t cn = (size_t)c->line_pos;
                if (cn + 1 > out_cap) cn = out_cap - 1;
                memcpy(out, c->line_buf, cn);
                out[cn] = '\0';
                c->line_pos = 0;
                return out;
            }
            return NULL;
        }
        c->line_pos += (int)n;
    }
}

/* ── Run a one-shot docker command, capture stdout into arena buf ── */
/* Returns NUL-terminated buffer or NULL. Combined stdout+stderr. */
static char *run_one_shot(AirbyteCtx *c, const char *subcmd, Arena *a, size_t *len_out) {
    char cmd[2048];
    snprintf(cmd, sizeof(cmd),
        "%s run --rm "
        "--memory=512m --cpus=1 "
        "-v %s:/secrets:ro "
        "%s %s --config /secrets/config.json 2>&1",
        runtime_name(c->runtime), c->workdir, c->image, subcmd);
    LOG_INFO("airbyte: $ %s", cmd);

    FILE *fp = popen(cmd, "r");
    if (!fp) return NULL;

    size_t cap = 64 * 1024, len = 0;
    char *buf = arena_alloc(a, cap);
    char chunk[8192];
    size_t n;
    while ((n = fread(chunk, 1, sizeof(chunk), fp)) > 0) {
        if (len + n + 1 > cap) {
            size_t new_cap = cap * 2;
            while (len + n + 1 > new_cap) new_cap *= 2;
            char *nb = arena_alloc(a, new_cap);
            memcpy(nb, buf, len);
            buf = nb;
            cap = new_cap;
        }
        memcpy(buf + len, chunk, n);
        len += n;
    }
    buf[len] = '\0';
    int rc = pclose(fp);
    if (rc != 0) LOG_WARN("airbyte: %s exited with %d", subcmd, WEXITSTATUS(rc));
    if (len_out) *len_out = len;
    return buf;
}

/* Iterate JSON-RPC-style line messages from a buffer; call cb for each parsed JVal. */
typedef int (*line_cb_fn)(JVal *msg, void *ud);
static int iterate_json_lines(char *buf, line_cb_fn cb, void *ud) {
    int found = 0;
    char *line = strtok(buf, "\n");
    while (line) {
        size_t l = strlen(line);
        if (l > 0) {
            Arena *la = arena_create(8192);
            JVal *msg = json_parse(la, line, l);
            if (msg && msg->type == JV_OBJECT) {
                if (cb(msg, ud)) found = 1;
            }
            arena_destroy(la);
        }
        line = strtok(NULL, "\n");
    }
    return found;
}

/* ── lifecycle ─────────────────────────────────────────────────── */
static void *airbyte_create(const char *config_json, Arena *a) {
    AirbyteCtx *c = arena_calloc(a, sizeof(AirbyteCtx));
    c->arena = a;
    c->read_pid = 0;
    c->read_fd = -1;
    c->line_buf = arena_alloc(a, AB_LINE_BUF);
    c->line_pos = 0;

    JVal *cfg = json_parse(a, config_json, strlen(config_json));
    if (!cfg || cfg->type != JV_OBJECT) {
        LOG_ERROR("airbyte: invalid config JSON");
        return NULL;
    }
    const char *image = json_str(json_get(cfg, "image"), NULL);
    JVal *inner = json_get(cfg, "config");
    if (!image || !inner) {
        LOG_ERROR("airbyte: config must include 'image' and 'config'");
        return NULL;
    }
    if (!is_whitelisted_image(image)) {
        LOG_ERROR("airbyte: image '%s' is not whitelisted "
                  "(must start with airbyte/source- or airbyte/destination-)", image);
        return NULL;
    }
    strncpy(c->image, image, sizeof(c->image) - 1);

    const char *prefer_rt = json_str(json_get(cfg, "runtime"), "auto");
    c->runtime = detect_runtime(prefer_rt);

    /* Serialize inner config to write to file */
    JBuf jb; jb_init(&jb, a, 4096);
    jval_emit(&jb, inner);
    const char *inner_text = jb_done(&jb);
    strncpy(c->config_json, inner_text, sizeof(c->config_json) - 1);

    /* Create unique workdir */
    snprintf(c->workdir, sizeof(c->workdir), "/tmp/airbyte_%d_%d",
             (int)getpid(), rand());
    if (mkdir(c->workdir, 0700) != 0) {
        LOG_ERROR("airbyte: mkdir %s failed: %s", c->workdir, strerror(errno));
        return NULL;
    }
    c->workdir_created = 1;

    char cfg_path[512];
    snprintf(cfg_path, sizeof(cfg_path), "%s/config.json", c->workdir);
    if (write_text_file(cfg_path, c->config_json, strlen(c->config_json)) != 0) {
        LOG_ERROR("airbyte: cannot write config to %s", cfg_path);
        rm_rf_workdir(c->workdir);
        c->workdir_created = 0;
        return NULL;
    }

    LOG_INFO("airbyte: image=%s workdir=%s runtime=%s",
             c->image, c->workdir, runtime_name(c->runtime));
    return c;
}

static void airbyte_destroy(void *ctx) {
    AirbyteCtx *c = ctx;
    if (!c) return;
    if (c->read_pid > 0) {
        kill(c->read_pid, SIGTERM);
        int s; waitpid(c->read_pid, &s, 0);
        c->read_pid = 0;
    }
    if (c->read_fd >= 0) { close(c->read_fd); c->read_fd = -1; }
    if (c->workdir_created) {
        rm_rf_workdir(c->workdir);
        c->workdir_created = 0;
    }
}

/* ── ping (check) ──────────────────────────────────────────────── */
static int check_cb(JVal *msg, void *ud) {
    int *found = ud;
    const char *type = json_str(json_get(msg, "type"), NULL);
    if (!type || strcmp(type, "CONNECTION_STATUS") != 0) return 0;
    const char *st = json_str(json_get(json_get(msg, "connectionStatus"), "status"), NULL);
    if (st && strcmp(st, "SUCCEEDED") == 0) { *found = 1; return 1; }
    return 0;
}

static int airbyte_ping(void *ctx) {
    AirbyteCtx *c = ctx;
    Arena *tmp = arena_create(128 * 1024);
    char *out = run_one_shot(c, "check", tmp, NULL);
    int succeeded = 0;
    if (out) iterate_json_lines(out, check_cb, &succeeded);
    arena_destroy(tmp);
    return succeeded ? 0 : -1;
}

/* ── list_entities (discover) ─────────────────────────────────── */
typedef struct { Arena *a; char **out_json; size_t *out_len; } catalog_capture_t;

static int discover_cb(JVal *msg, void *ud) {
    catalog_capture_t *cap = ud;
    const char *type = json_str(json_get(msg, "type"), NULL);
    if (!type || strcmp(type, "CATALOG") != 0) return 0;
    JVal *catalog = json_get(msg, "catalog");
    if (!catalog) return 0;
    JBuf jb; jb_init(&jb, cap->a, 32 * 1024);
    jval_emit(&jb, catalog);
    const char *txt = jb_done(&jb);
    size_t len = strlen(txt);
    *cap->out_json = arena_alloc(cap->a, len + 1);
    memcpy(*cap->out_json, txt, len + 1);
    *cap->out_len = len;
    return 1;
}

static int airbyte_list_entities(void *vctx, Arena *a, DfoEntityList *out) {
    AirbyteCtx *c = vctx;
    out->items = NULL; out->count = 0;

    /* Use ctx arena so the cached catalog survives across calls */
    Arena *tmp = arena_create(256 * 1024);
    char *raw_out = run_one_shot(c, "discover", tmp, NULL);
    if (!raw_out) { arena_destroy(tmp); return -1; }

    catalog_capture_t cap = { .a = c->arena,
                              .out_json = &c->catalog_json,
                              .out_len  = &c->catalog_len };
    if (!iterate_json_lines(raw_out, discover_cb, &cap)) {
        arena_destroy(tmp);
        LOG_ERROR("airbyte: discover did not return a CATALOG message");
        return -1;
    }
    arena_destroy(tmp);

    /* Parse the saved catalog and return stream names */
    JVal *catalog = json_parse(a, c->catalog_json, c->catalog_len);
    JVal *streams = json_get(catalog, "streams");
    if (!streams || streams->type != JV_ARRAY) return -1;

    out->items = arena_calloc(a, streams->nitems * sizeof(DfoEntity));
    for (size_t i = 0; i < streams->nitems; i++) {
        const char *name = json_str(json_get(streams->items[i], "name"), NULL);
        if (name) {
            out->items[out->count].entity = arena_strdup(a, name);
            out->items[out->count].type   = "stream";
            out->count++;
        }
    }
    return 0;
}

/* ── describe (json_schema → Schema) ──────────────────────────── */
static ColType json_schema_type_to_coltype(JVal *prop) {
    JVal *type = json_get(prop, "type");
    const char *t = NULL;
    if (type && type->type == JV_STRING) t = type->s;
    else if (type && type->type == JV_ARRAY) {
        for (size_t j = 0; j < type->nitems; j++) {
            const char *cand = json_str(type->items[j], NULL);
            if (cand && strcmp(cand, "null") != 0) { t = cand; break; }
        }
    }
    if (!t) return COL_TEXT;
    /* Airbyte uses JSON Schema with optional `format` for date-time etc.
     * date-time → INT64 (unix epoch) */
    const char *fmt = json_str(json_get(prop, "format"), NULL);
    if (fmt && (strcmp(fmt, "date-time") == 0 || strcmp(fmt, "datetime") == 0))
        return COL_INT64;
    if      (strcmp(t, "integer") == 0) return COL_INT64;
    else if (strcmp(t, "number")  == 0) return COL_DOUBLE;
    else if (strcmp(t, "boolean") == 0) return COL_BOOL;
    else                                return COL_TEXT;
}

static int airbyte_describe(void *vctx, Arena *a, const char *entity, Schema **out) {
    AirbyteCtx *c = vctx;
    if (!c->catalog_json) {
        DfoEntityList el = {0};
        if (airbyte_list_entities(vctx, c->arena, &el) != 0) return -1;
    }

    JVal *catalog = json_parse(a, c->catalog_json, c->catalog_len);
    JVal *streams = json_get(catalog, "streams");
    if (!streams || streams->type != JV_ARRAY) return -1;

    JVal *stream = NULL;
    for (size_t i = 0; i < streams->nitems; i++) {
        const char *n = json_str(json_get(streams->items[i], "name"), NULL);
        if (n && strcmp(n, entity) == 0) { stream = streams->items[i]; break; }
    }
    if (!stream) return -1;

    JVal *js    = json_get(stream, "json_schema");
    JVal *props = json_get(js,     "properties");
    if (!props || props->type != JV_OBJECT) return -1;

    Schema *s = arena_calloc(a, sizeof(Schema));
    s->ncols = (int)props->nkeys;
    s->cols  = arena_calloc(a, (size_t)s->ncols * sizeof(ColDef));
    for (size_t i = 0; i < props->nkeys; i++) {
        s->cols[i].name     = arena_strdup(a, props->keys[i]);
        s->cols[i].type     = json_schema_type_to_coltype(props->vals[i]);
        s->cols[i].nullable = true;
    }
    *out = s;
    return 0;
}

/* ── read_batch ────────────────────────────────────────────────── */
/* Build a ConfiguredAirbyteCatalog file containing only the requested stream. */
static int write_configured_catalog(AirbyteCtx *c, const char *entity) {
    if (!c->catalog_json) return -1;
    Arena *tmp = arena_create(64 * 1024);
    JVal *catalog = json_parse(tmp, c->catalog_json, c->catalog_len);
    JVal *streams = json_get(catalog, "streams");
    if (!streams || streams->type != JV_ARRAY) { arena_destroy(tmp); return -1; }

    JVal *stream = NULL;
    for (size_t i = 0; i < streams->nitems; i++) {
        const char *n = json_str(json_get(streams->items[i], "name"), NULL);
        if (n && strcmp(n, entity) == 0) { stream = streams->items[i]; break; }
    }
    if (!stream) { arena_destroy(tmp); return -1; }

    JBuf jb; jb_init(&jb, tmp, 16 * 1024);
    jb_obj_begin(&jb);
        jb_key(&jb, "streams");
        jb_arr_begin(&jb);
            jb_obj_begin(&jb);
                jb_key(&jb, "stream");
                jval_emit(&jb, stream);
                jb_key(&jb, "sync_mode");             jb_str(&jb, "full_refresh");
                jb_key(&jb, "destination_sync_mode"); jb_str(&jb, "append");
            jb_obj_end(&jb);
        jb_arr_end(&jb);
    jb_obj_end(&jb);
    const char *txt = jb_done(&jb);

    char path[512];
    snprintf(path, sizeof(path), "%s/catalog.json", c->workdir);
    int rc = write_text_file(path, txt, strlen(txt));
    arena_destroy(tmp);
    return rc;
}

static int start_read_subprocess(AirbyteCtx *c, const char *entity) {
    if (write_configured_catalog(c, entity) != 0) return -1;

    char state_path[512];
    snprintf(state_path, sizeof(state_path), "%s/state.json", c->workdir);
    const char *state_txt = c->state_json[0] ? c->state_json : "{}";
    if (write_text_file(state_path, state_txt, strlen(state_txt)) != 0) return -1;

    int pipefd[2];
    if (pipe(pipefd) < 0) return -1;
    pid_t pid = fork();
    if (pid < 0) { close(pipefd[0]); close(pipefd[1]); return -1; }

    if (pid == 0) {
        /* CHILD */
        dup2(pipefd[1], STDOUT_FILENO);
        /* keep stderr connected to parent's stderr for log forwarding */
        close(pipefd[0]);
        close(pipefd[1]);
        char vmount[1024];
        snprintf(vmount, sizeof(vmount), "%s:/secrets:ro", c->workdir);
        const char *rt = runtime_name(c->runtime);
        execlp(rt, rt,
               "run", "--rm", "-i",
               "--memory=512m", "--cpus=1",
               "-v", vmount,
               c->image,
               "read",
               "--config",  "/secrets/config.json",
               "--catalog", "/secrets/catalog.json",
               "--state",   "/secrets/state.json",
               (char *)NULL);
        _exit(127);
    }
    /* PARENT */
    close(pipefd[1]);
    c->read_pid = pid;
    c->read_fd  = pipefd[0];
    c->line_pos = 0;
    return 0;
}

/* Convert one Airbyte RECORD `data` object into batch row. */
static void record_to_row(ColBatch *batch, int row, JVal *data, Arena *a) {
    if (!data || data->type != JV_OBJECT) return;
    Schema *s = batch->schema;
    for (int col = 0; col < s->ncols; col++) {
        const char *name = s->cols[col].name;
        JVal *v = json_get(data, name);
        if (!v || v->type == JV_NULL) {
            batch->null_bitmap[col][row / 8] |= (uint8_t)(1u << (row % 8));
            continue;
        }
        switch (s->cols[col].type) {
            case COL_INT64:
                ((int64_t *)batch->values[col])[row] = (int64_t)json_int(v, 0);
                break;
            case COL_DOUBLE:
                ((double  *)batch->values[col])[row] = json_dbl(v, 0);
                break;
            case COL_BOOL:
                ((int     *)batch->values[col])[row] = json_bool(v, false) ? 1 : 0;
                break;
            default: {
                /* TEXT — string as-is, anything else serialized as JSON */
                if (v->type == JV_STRING) {
                    ((char **)batch->values[col])[row] = arena_strndup(a, v->s, v->len);
                } else {
                    JBuf jb; jb_init(&jb, a, 256);
                    jval_emit(&jb, v);
                    ((char **)batch->values[col])[row] = (char *)jb_done(&jb);
                }
                break;
            }
        }
    }
}

static int airbyte_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                              const char *entity, ColBatch **out) {
    AirbyteCtx *c = vctx;
    (void)req;

    if (c->read_pid == 0) {
        Schema *schema = NULL;
        if (airbyte_describe(vctx, c->arena, entity, &schema) != 0) return -1;
        c->reading_schema = schema;
        strncpy(c->reading_entity, entity, sizeof(c->reading_entity) - 1);
        if (start_read_subprocess(c, entity) != 0) return -1;
    }

    Schema *schema = c->reading_schema;
    int ncols = schema->ncols;
    ColBatch *batch = arena_calloc(a, sizeof(ColBatch));
    batch->schema = schema; batch->ncols = ncols;
    for (int col = 0; col < ncols; col++) {
        switch (schema->cols[col].type) {
            case COL_INT64:  batch->values[col] = arena_alloc(a, BATCH_SIZE * sizeof(int64_t)); break;
            case COL_DOUBLE: batch->values[col] = arena_alloc(a, BATCH_SIZE * sizeof(double));  break;
            case COL_BOOL:   batch->values[col] = arena_alloc(a, BATCH_SIZE * sizeof(int));     break;
            default:         batch->values[col] = arena_calloc(a, BATCH_SIZE * sizeof(char *)); break;
        }
        batch->null_bitmap[col] = arena_calloc(a, (BATCH_SIZE + 7) / 8);
    }

    char *line = arena_alloc(a, AB_LINE_BUF);
    int row = 0;
    while (row < BATCH_SIZE) {
        if (!read_line(c, line, AB_LINE_BUF)) break;   /* EOF */
        if (!line[0]) continue;
        Arena *la = arena_create(8 * 1024);
        JVal *msg = json_parse(la, line, strlen(line));
        if (msg && msg->type == JV_OBJECT) {
            const char *type = json_str(json_get(msg, "type"), NULL);
            if (type && strcmp(type, "RECORD") == 0) {
                JVal *record = json_get(msg, "record");
                const char *sn = json_str(json_get(record, "stream"), NULL);
                if (sn && strcmp(sn, entity) == 0) {
                    record_to_row(batch, row, json_get(record, "data"), a);
                    row++;
                }
            } else if (type && strcmp(type, "STATE") == 0) {
                JVal *state = json_get(msg, "state");
                if (state) {
                    JBuf jb; jb_init(&jb, la, 4096);
                    jval_emit(&jb, state);
                    const char *st = jb_done(&jb);
                    strncpy(c->state_json, st, sizeof(c->state_json) - 1);
                }
            } else if (type && strcmp(type, "LOG") == 0) {
                JVal *log = json_get(msg, "log");
                LOG_INFO("airbyte[%s]: [%s] %s", entity,
                         json_str(json_get(log, "level"),   "INFO"),
                         json_str(json_get(log, "message"), ""));
            } else if (type && strcmp(type, "TRACE") == 0) {
                /* Airbyte 0.2.x error tracing — surface message to logs */
                JVal *trace = json_get(msg, "trace");
                JVal *err   = json_get(trace, "error");
                if (err) {
                    LOG_WARN("airbyte[%s]: TRACE error: %s", entity,
                             json_str(json_get(err, "message"), ""));
                }
            }
        }
        arena_destroy(la);
    }

    if (row == 0) {
        /* Stream finished — reap subprocess */
        if (c->read_pid > 0) {
            int s; waitpid(c->read_pid, &s, 0);
            c->read_pid = 0;
            if (c->read_fd >= 0) { close(c->read_fd); c->read_fd = -1; }
        }
    }

    batch->nrows = row;
    *out = batch;
    return 0;
}

/* ── Plugin entry point ───────────────────────────────────────── */
const DfoConnector dfo_connector_entry = {
    .abi_version    = DFO_CONNECTOR_ABI_VERSION,
    .name           = "airbyte",
    .version        = "0.1.0",
    .description    = "Airbyte Connector Protocol runner — runs any whitelisted "
                      "airbyte/source-* image as a DFO data source",
    .create         = airbyte_create,
    .destroy        = airbyte_destroy,
    .list_entities  = airbyte_list_entities,
    .describe       = airbyte_describe,
    .read_batch     = airbyte_read_batch,
    .cdc_start      = NULL,
    .cdc_stop       = NULL,
    .ping           = airbyte_ping,
};
