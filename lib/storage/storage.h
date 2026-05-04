#pragma once
#include "../core/arena.h"
#include "../core/json.h"
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/* ── Column types ── */
typedef enum { COL_INT64=0, COL_DOUBLE, COL_TEXT, COL_BOOL, COL_NULL } ColType;

typedef struct {
    const char *name;
    ColType     type;
    bool        nullable;
} ColDef;

typedef struct {
    ColDef *cols;
    int     ncols;
} Schema;

/* ── Columnar batch (8192 rows per batch) ── */
#define BATCH_SIZE 8192
#define MAX_COLS   2048

typedef struct {
    Schema     *schema;
    int         ncols;
    int         nrows;
    /* per column */
    uint8_t    *null_bitmap[MAX_COLS]; /* bit per row */
    void       *values[MAX_COLS];      /* int64_t*, double*, char** */
} ColBatch;

/* ── WAL ── */
typedef struct WAL WAL;
WAL  *wal_open(const char *path);
int   wal_append(WAL *w, const void *data, size_t len);
int   wal_sync(WAL *w);
void  wal_close(WAL *w);

/* ── Table ── */
typedef struct Table Table;
Table  *table_create(const char *name, Schema *schema, const char *dir);
Table  *table_open(const char *name, const char *dir);
int     table_append(Table *t, ColBatch *batch);
int     table_scan(Table *t, ColBatch **out, Arena *a);  /* returns nrows */
int64_t table_row_count(Table *t);
Schema *table_schema(Table *t);
void    table_close(Table *t);

/* ── Catalog (SQLite-backed) ── */
typedef struct Catalog Catalog;
Catalog *catalog_open(const char *db_path);
void     catalog_close(Catalog *c);

int catalog_register_table(Catalog *c, const char *name, Schema *schema);
int catalog_update_table_meta(Catalog *c, const char *name, const char *source, int64_t row_count);
int catalog_list_tables(Catalog *c, char ***names_out, int *count_out, Arena *a);
int catalog_list_tables_full(Catalog *c, char **json_out, Arena *a);
int catalog_get_schema(Catalog *c, const char *table, Schema **out, Arena *a);
int catalog_drop_table(Catalog *c, const char *name);

/* Pipeline metadata */
int catalog_save_pipeline(Catalog *c, const char *id, const char *json);
int catalog_load_pipeline(Catalog *c, const char *id, char **json_out, Arena *a);
int catalog_list_pipelines(Catalog *c, char ***ids_out, int *count_out, Arena *a);
int catalog_delete_pipeline(Catalog *c, const char *id);

/* Run history */
int catalog_log_run(Catalog *c, const char *pipeline_id,
                    int64_t started_at, int64_t finished_at,
                    int status, const char *error_msg);
int catalog_list_runs(Catalog *c, const char *pipeline_id,
                      char **json_out, Arena *a);

/* Saved analytics results */
int catalog_save_result(Catalog *c, const char *name, const char *sql_text,
                        const char *columns_json, const char *rows_json,
                        int row_count, int64_t *out_id);
int catalog_list_results(Catalog *c, char **out, Arena *a);
int catalog_get_result(Catalog *c, int64_t id, char **out, Arena *a);
int catalog_delete_result(Catalog *c, int64_t id);
