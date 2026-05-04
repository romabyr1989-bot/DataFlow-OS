#include "storage.h"
#include "../core/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <sqlite3.h>
#include <time.h>

/* ── WAL ── */
struct WAL { int fd; char path[512]; };

WAL *wal_open(const char *path) {
    WAL *w = calloc(1, sizeof(WAL));
    strncpy(w->path, path, sizeof(w->path)-1);
    w->fd = open(path, O_WRONLY|O_CREAT|O_APPEND|O_CLOEXEC, 0644);
    if (w->fd < 0) { LOG_ERROR("wal_open %s: %s", path, strerror(errno)); free(w); return NULL; }
    return w;
}

int wal_append(WAL *w, const void *data, size_t len) {
    /* TLV: [4-byte len][data] */
    uint32_t l = (uint32_t)len;
    if (write(w->fd, &l, 4) != 4) return -1;
    if (write(w->fd, data, len) != (ssize_t)len) return -1;
    return 0;
}

int wal_sync(WAL *w)  { return fdatasync(w->fd); }
void wal_close(WAL *w){ close(w->fd); free(w); }

/* ── ColBatch helpers ── */
static bool bit_get(const uint8_t *bm, int i) {
    return !!(bm[i/8] & (1u << (i%8)));
}
static void bit_set(uint8_t *bm, int i) { bm[i/8] |= (1u << (i%8)); }

/* ── Table (simple flat-file, one file per batch) ── */
struct Table {
    char    name[128];
    char    dir[512];
    Schema *schema;
    int64_t row_count;
    WAL    *wal;
};

Table *table_create(const char *name, Schema *schema, const char *dir) {
    Table *t = calloc(1, sizeof(Table));
    strncpy(t->name, name, sizeof(t->name)-1);
    snprintf(t->dir, sizeof(t->dir), "%s/%s", dir, name);
    mkdir(t->dir, 0755);
    t->schema = schema;
    char wal_path[600]; snprintf(wal_path, sizeof(wal_path), "%s/wal.bin", t->dir);
    t->wal = wal_open(wal_path);
    return t;
}

Table *table_open(const char *name, const char *dir) {
    Table *t = calloc(1, sizeof(Table));
    strncpy(t->name, name, sizeof(t->name)-1);
    snprintf(t->dir, sizeof(t->dir), "%s/%s", dir, name);
    /* minimal: just open WAL */
    char wal_path[600]; snprintf(wal_path, sizeof(wal_path), "%s/wal.bin", t->dir);
    t->wal = wal_open(wal_path);
    return t;
}

int table_append(Table *t, ColBatch *batch) {
    if (!batch || batch->nrows == 0) return 0;
    /* Serialize batch as CSV into WAL for simplicity */
    enum { ROW_BUF_CAP = 262144 };
    char *row_buf = malloc(ROW_BUF_CAP);
    if (!row_buf) return -1;
    for (int r = 0; r < batch->nrows; r++) {
        int off = 0;
        for (int c = 0; c < batch->ncols; c++) {
            if (off >= ROW_BUF_CAP - 2) { free(row_buf); return -1; }
            if (c) row_buf[off++] = ',';
            if (batch->null_bitmap[c] && bit_get(batch->null_bitmap[c], r)) {
                int n = snprintf(row_buf+off, ROW_BUF_CAP-off, "NULL");
                if (n < 0 || off + n >= ROW_BUF_CAP) { free(row_buf); return -1; }
                off += n;
                continue;
            }
            switch (batch->schema->cols[c].type) {
                case COL_INT64: {
                    int n = snprintf(row_buf+off, ROW_BUF_CAP-off, "%lld", ((int64_t*)batch->values[c])[r]);
                    if (n < 0 || off + n >= ROW_BUF_CAP) { free(row_buf); return -1; }
                    off += n; break;
                }
                case COL_DOUBLE: {
                    int n = snprintf(row_buf+off, ROW_BUF_CAP-off, "%.10g", ((double*)batch->values[c])[r]);
                    if (n < 0 || off + n >= ROW_BUF_CAP) { free(row_buf); return -1; }
                    off += n; break;
                }
                case COL_TEXT: {
                    int n = snprintf(row_buf+off, ROW_BUF_CAP-off, "%s", ((char**)batch->values[c])[r]?:"");
                    if (n < 0 || off + n >= ROW_BUF_CAP) { free(row_buf); return -1; }
                    off += n; break;
                }
                case COL_BOOL: {
                    int n = snprintf(row_buf+off, ROW_BUF_CAP-off, "%s", ((int*)batch->values[c])[r]?"true":"false");
                    if (n < 0 || off + n >= ROW_BUF_CAP) { free(row_buf); return -1; }
                    off += n; break;
                }
                default: break;
            }
        }
        if (off >= ROW_BUF_CAP - 1) { free(row_buf); return -1; }
        row_buf[off++] = '\n';
        wal_append(t->wal, row_buf, (size_t)off);
        t->row_count++;
    }
    free(row_buf);
    return wal_sync(t->wal);
}

int64_t table_row_count(Table *t) { return t->row_count; }
Schema *table_schema(Table *t)    { return t->schema; }

int table_scan(Table *t, ColBatch **out, Arena *a) {
    /* Minimal: count rows from WAL file */
    (void)t; (void)out; (void)a;
    return (int)t->row_count;
}

void table_close(Table *t) { if (t->wal) wal_close(t->wal); free(t); }

/* ── Catalog ── */
struct Catalog { sqlite3 *db; };

static const char *CATALOG_SCHEMA =
    "CREATE TABLE IF NOT EXISTS tables("
    "  name TEXT PRIMARY KEY, schema_json TEXT, created_at INTEGER);"
    "CREATE TABLE IF NOT EXISTS pipelines("
    "  id TEXT PRIMARY KEY, json TEXT, updated_at INTEGER);"
    "CREATE TABLE IF NOT EXISTS pipeline_runs("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  pipeline_id TEXT, started_at INTEGER, finished_at INTEGER,"
    "  status INTEGER, error_msg TEXT);"
    "CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON pipeline_runs(pipeline_id);"
    "CREATE TABLE IF NOT EXISTS saved_results("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  name TEXT NOT NULL,"
    "  sql_text TEXT,"
    "  columns_json TEXT,"
    "  rows_json TEXT,"
    "  row_count INTEGER,"
    "  created_at INTEGER);"
    "CREATE INDEX IF NOT EXISTS idx_saved_results_created ON saved_results(created_at DESC);";

Catalog *catalog_open(const char *path) {
    Catalog *c = calloc(1, sizeof(Catalog));
    if (sqlite3_open(path, &c->db) != SQLITE_OK) {
        LOG_ERROR("catalog_open %s: %s", path, sqlite3_errmsg(c->db));
        free(c); return NULL;
    }
    sqlite3_exec(c->db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(c->db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
    char *err = NULL;
    if (sqlite3_exec(c->db, CATALOG_SCHEMA, NULL, NULL, &err) != SQLITE_OK) {
        LOG_ERROR("catalog schema: %s", err); sqlite3_free(err);
    }
    /* migrate: add columns if they don't exist yet */
    sqlite3_exec(c->db, "ALTER TABLE tables ADD COLUMN source TEXT DEFAULT 'ingest';", NULL, NULL, NULL);
    sqlite3_exec(c->db, "ALTER TABLE tables ADD COLUMN row_count INTEGER DEFAULT 0;", NULL, NULL, NULL);
    return c;
}

void catalog_close(Catalog *c) { sqlite3_close(c->db); free(c); }

/* Schema → JSON helper */
static char *schema_to_json(Schema *s, Arena *a) {
    JBuf jb; jb_init(&jb, a, 512);
    jb_arr_begin(&jb);
    for (int i = 0; i < s->ncols; i++) {
        if (!s->cols[i].name) continue;
        jb_obj_begin(&jb);
        jb_key(&jb,"name"); jb_str(&jb, s->cols[i].name);
        const char *t = "text";
        if (s->cols[i].type==COL_INT64) t="int64";
        else if (s->cols[i].type==COL_DOUBLE) t="double";
        else if (s->cols[i].type==COL_BOOL) t="bool";
        jb_key(&jb,"type"); jb_str(&jb, t);
        jb_key(&jb,"nullable"); jb_bool(&jb, s->cols[i].nullable);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);
    return (char*)jb_done(&jb);
}

int catalog_register_table(Catalog *c, const char *name, Schema *schema) {
    Arena *a = arena_create(4096);
    char *sj = schema_to_json(schema, a);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,
        "INSERT OR REPLACE INTO tables(name,schema_json,created_at) VALUES(?,?,?)", -1, &st, NULL);
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,sj,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int64(st,3,(int64_t)time(NULL));
    int rc = sqlite3_step(st); sqlite3_finalize(st); arena_destroy(a);
    return rc == SQLITE_DONE ? 0 : -1;
}

int catalog_list_tables(Catalog *c, char ***names_out, int *count_out, Arena *a) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,"SELECT name FROM tables ORDER BY name",-1,&st,NULL);
    int cap=16,n=0;
    char **names = arena_alloc(a, cap*sizeof(char*));
    while (sqlite3_step(st)==SQLITE_ROW) {
        if (n==cap){cap*=2;char**nb=arena_alloc(a,cap*sizeof(char*));memcpy(nb,names,n*sizeof(char*));names=nb;}
        names[n++] = arena_strdup(a,(const char*)sqlite3_column_text(st,0));
    }
    sqlite3_finalize(st);
    *names_out = names; *count_out = n;
    return 0;
}

int catalog_get_schema(Catalog *c, const char *table, Schema **out, Arena *a) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,"SELECT schema_json FROM tables WHERE name=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,table,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW){sqlite3_finalize(st);return -1;}
    const char *sj=(const char*)sqlite3_column_text(st,0);
    JVal *arr = json_parse(a, sj, strlen(sj));
    sqlite3_finalize(st);
    if (!arr || arr->type!=JV_ARRAY) return -1;
    Schema *schema = arena_calloc(a, sizeof(Schema));
    schema->ncols = (int)arr->nitems;
    schema->cols  = arena_alloc(a, schema->ncols * sizeof(ColDef));
    for (int i=0;i<schema->ncols;i++){
        JVal *col = arr->items[i];
        schema->cols[i].name = arena_strdup(a, json_str(json_get(col,"name"),""));
        const char *t = json_str(json_get(col,"type"),"text");
        if (!strcmp(t,"int64"))  schema->cols[i].type=COL_INT64;
        else if (!strcmp(t,"double")) schema->cols[i].type=COL_DOUBLE;
        else if (!strcmp(t,"bool"))   schema->cols[i].type=COL_BOOL;
        else schema->cols[i].type=COL_TEXT;
        schema->cols[i].nullable = json_bool(json_get(col,"nullable"),true);
    }
    *out = schema;
    return 0;
}

int catalog_drop_table(Catalog *c, const char *name) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,"DELETE FROM tables WHERE name=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC);
    int rc=sqlite3_step(st); sqlite3_finalize(st);
    return rc==SQLITE_DONE?0:-1;
}

int catalog_update_table_meta(Catalog *c, const char *name, const char *source, int64_t row_count) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,
        "UPDATE tables SET source=?, row_count=? WHERE name=?", -1, &st, NULL);
    sqlite3_bind_text(st, 1, source, -1, SQLITE_STATIC);
    sqlite3_bind_int64(st, 2, row_count);
    sqlite3_bind_text(st, 3, name, -1, SQLITE_STATIC);
    int rc = sqlite3_step(st); sqlite3_finalize(st);
    return rc == SQLITE_DONE ? 0 : -1;
}

int catalog_list_tables_full(Catalog *c, char **json_out, Arena *a) {
    /* Returns JSON array: [{name, source, row_count, columns:[{name,type}]}] */
    JBuf jb; jb_init(&jb, a, 4096);
    jb_arr_begin(&jb);
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,
        "SELECT name, COALESCE(source,'ingest'), COALESCE(row_count,0), schema_json"
        " FROM tables ORDER BY name", -1, &st, NULL);
    while (sqlite3_step(st) == SQLITE_ROW) {
        const char *name  = (const char*)sqlite3_column_text(st, 0);
        const char *src   = (const char*)sqlite3_column_text(st, 1);
        int64_t     rows  = sqlite3_column_int64(st, 2);
        const char *sjson = (const char*)sqlite3_column_text(st, 3);
        jb_obj_begin(&jb);
        jb_key(&jb,"name");   jb_str(&jb, name ? name : "");
        jb_key(&jb,"source"); jb_str(&jb, src  ? src  : "ingest");
        jb_key(&jb,"rows");   jb_int(&jb, rows);
        /* parse schema_json to emit columns array */
        if (sjson && *sjson) {
            Arena *ta = arena_create(4096);
            JVal *arr = json_parse(ta, sjson, strlen(sjson));
            if (arr && arr->type == JV_ARRAY) {
                jb_key(&jb,"columns"); jb_arr_begin(&jb);
                for (size_t i = 0; i < arr->nitems; i++) {
                    JVal *col = arr->items[i];
                    const char *cname = json_str(json_get(col,"name"),"");
                    const char *ctype = json_str(json_get(col,"type"),"text");
                    if (!cname || !*cname) continue;
                    jb_obj_begin(&jb);
                    jb_key(&jb,"name"); jb_str(&jb, cname);
                    jb_key(&jb,"type"); jb_str(&jb, ctype);
                    jb_obj_end(&jb);
                }
                jb_arr_end(&jb);
            }
            arena_destroy(ta);
        }
        jb_obj_end(&jb);
    }
    sqlite3_finalize(st);
    jb_arr_end(&jb);
    *json_out = (char*)jb_done(&jb);
    return 0;
}

int catalog_save_pipeline(Catalog *c, const char *id, const char *json) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,
        "INSERT OR REPLACE INTO pipelines(id,json,updated_at) VALUES(?,?,?)",-1,&st,NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,json,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,3,(int64_t)time(NULL));
    int rc=sqlite3_step(st); sqlite3_finalize(st);
    return rc==SQLITE_DONE?0:-1;
}

int catalog_load_pipeline(Catalog *c, const char *id, char **out, Arena *a) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,"SELECT json FROM pipelines WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    if (sqlite3_step(st)!=SQLITE_ROW){sqlite3_finalize(st);return -1;}
    *out = arena_strdup(a,(const char*)sqlite3_column_text(st,0));
    sqlite3_finalize(st); return 0;
}

int catalog_list_pipelines(Catalog *c, char ***ids_out, int *count_out, Arena *a) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,"SELECT id FROM pipelines ORDER BY updated_at DESC",-1,&st,NULL);
    int cap=16,n=0; char **ids=arena_alloc(a,cap*sizeof(char*));
    while(sqlite3_step(st)==SQLITE_ROW){
        if(n==cap){cap*=2;char**nb=arena_alloc(a,cap*sizeof(char*));memcpy(nb,ids,n*sizeof(char*));ids=nb;}
        ids[n++]=arena_strdup(a,(const char*)sqlite3_column_text(st,0));
    }
    sqlite3_finalize(st); *ids_out=ids; *count_out=n; return 0;
}

int catalog_delete_pipeline(Catalog *c, const char *id) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,"DELETE FROM pipelines WHERE id=?",-1,&st,NULL);
    sqlite3_bind_text(st,1,id,-1,SQLITE_STATIC);
    int rc=sqlite3_step(st); sqlite3_finalize(st);
    return rc==SQLITE_DONE?0:-1;
}

int catalog_log_run(Catalog *c, const char *pid, int64_t s, int64_t f, int status, const char *err) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,
        "INSERT INTO pipeline_runs(pipeline_id,started_at,finished_at,status,error_msg) VALUES(?,?,?,?,?)",
        -1,&st,NULL);
    sqlite3_bind_text(st,1,pid,-1,SQLITE_STATIC);
    sqlite3_bind_int64(st,2,s); sqlite3_bind_int64(st,3,f);
    sqlite3_bind_int(st,4,status);
    sqlite3_bind_text(st,5,err?err:"",-1,SQLITE_STATIC);
    int rc=sqlite3_step(st); sqlite3_finalize(st);
    return rc==SQLITE_DONE?0:-1;
}

int catalog_list_runs(Catalog *c, const char *pid, char **out, Arena *a) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,
        "SELECT id,started_at,finished_at,status,error_msg FROM pipeline_runs "
        "WHERE pipeline_id=? ORDER BY started_at DESC LIMIT 50",-1,&st,NULL);
    sqlite3_bind_text(st,1,pid,-1,SQLITE_STATIC);
    JBuf jb; jb_init(&jb,a,1024); jb_arr_begin(&jb);
    while(sqlite3_step(st)==SQLITE_ROW){
        jb_obj_begin(&jb);
        jb_key(&jb,"id");     jb_int(&jb,sqlite3_column_int64(st,0));
        jb_key(&jb,"started");jb_int(&jb,sqlite3_column_int64(st,1));
        jb_key(&jb,"finished");jb_int(&jb,sqlite3_column_int64(st,2));
        jb_key(&jb,"status"); jb_int(&jb,sqlite3_column_int(st,3));
        jb_key(&jb,"error");  jb_str(&jb,(const char*)sqlite3_column_text(st,4));
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb); sqlite3_finalize(st);
    *out=(char*)jb_done(&jb); return 0;
}

int catalog_save_result(Catalog *c, const char *name, const char *sql_text,
                        const char *columns_json, const char *rows_json,
                        int row_count, int64_t *out_id) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,
        "INSERT INTO saved_results(name,sql_text,columns_json,rows_json,row_count,created_at)"
        " VALUES(?,?,?,?,?,?)", -1, &st, NULL);
    sqlite3_bind_text(st,1,name,-1,SQLITE_STATIC);
    sqlite3_bind_text(st,2,sql_text?sql_text:"",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,3,columns_json?columns_json:"[]",-1,SQLITE_STATIC);
    sqlite3_bind_text(st,4,rows_json?rows_json:"[]",-1,SQLITE_STATIC);
    sqlite3_bind_int(st,5,row_count);
    sqlite3_bind_int64(st,6,(int64_t)time(NULL));
    int rc = sqlite3_step(st); sqlite3_finalize(st);
    if (rc != SQLITE_DONE) return -1;
    if (out_id) *out_id = sqlite3_last_insert_rowid(c->db);
    return 0;
}

int catalog_list_results(Catalog *c, char **out, Arena *a) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,
        "SELECT id,name,sql_text,columns_json,row_count,created_at FROM saved_results"
        " ORDER BY created_at DESC LIMIT 100", -1, &st, NULL);
    JBuf jb; jb_init(&jb,a,4096); jb_arr_begin(&jb);
    while(sqlite3_step(st)==SQLITE_ROW){
        jb_obj_begin(&jb);
        jb_key(&jb,"id");           jb_int(&jb,sqlite3_column_int64(st,0));
        jb_key(&jb,"name");         jb_str(&jb,(const char*)sqlite3_column_text(st,1));
        jb_key(&jb,"sql_text");     jb_str(&jb,(const char*)sqlite3_column_text(st,2));
        { const char *cj = (const char*)sqlite3_column_text(st,3);
          jb_key(&jb,"columns_json"); jb_raw(&jb, cj ? cj : "[]"); }
        jb_key(&jb,"row_count");    jb_int(&jb,sqlite3_column_int(st,4));
        jb_key(&jb,"created_at");   jb_int(&jb,sqlite3_column_int64(st,5));
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb); sqlite3_finalize(st);
    *out=(char*)jb_done(&jb); return 0;
}

int catalog_get_result(Catalog *c, int64_t id, char **out, Arena *a) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,
        "SELECT id,name,sql_text,columns_json,rows_json,row_count,created_at"
        " FROM saved_results WHERE id=?", -1, &st, NULL);
    sqlite3_bind_int64(st,1,id);
    if (sqlite3_step(st)!=SQLITE_ROW){ sqlite3_finalize(st); return -1; }
    JBuf jb; jb_init(&jb,a,8192); jb_obj_begin(&jb);
    jb_key(&jb,"id");           jb_int(&jb,sqlite3_column_int64(st,0));
    jb_key(&jb,"name");         jb_str(&jb,(const char*)sqlite3_column_text(st,1));
    jb_key(&jb,"sql_text");     jb_str(&jb,(const char*)sqlite3_column_text(st,2));
    const char *cj=(const char*)sqlite3_column_text(st,3);
    const char *rj=(const char*)sqlite3_column_text(st,4);
    jb_key(&jb,"columns_json"); jb_str(&jb,cj?cj:"[]");
    jb_key(&jb,"rows_json");    jb_str(&jb,rj?rj:"[]");
    jb_key(&jb,"row_count");    jb_int(&jb,sqlite3_column_int(st,5));
    jb_key(&jb,"created_at");   jb_int(&jb,sqlite3_column_int64(st,6));
    jb_obj_end(&jb); sqlite3_finalize(st);
    *out=(char*)jb_done(&jb); return 0;
}

int catalog_delete_result(Catalog *c, int64_t id) {
    sqlite3_stmt *st;
    sqlite3_prepare_v2(c->db,"DELETE FROM saved_results WHERE id=?",-1,&st,NULL);
    sqlite3_bind_int64(st,1,id);
    int rc=sqlite3_step(st); sqlite3_finalize(st);
    return rc==SQLITE_DONE?0:-1;
}
