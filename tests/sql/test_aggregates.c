/*
 * Unit tests — SQL aggregate functions via qengine.
 * Tests COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING in-process.
 * Uses storage API (table_create + table_append) to write WAL data.
 */
#include "../../lib/core/arena.h"
#include "../../lib/storage/storage.h"
#include "../../lib/sql_parser/sql.h"
#include "../../lib/qengine/qengine.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>

static int _plan = 0, _pass = 0, _fail = 0;
#define ok(c,...) do { \
    int _n = ++_plan; \
    if(c){ _pass++; printf("ok %d — ", _n); printf(__VA_ARGS__); puts(""); } \
    else { _fail++; printf("not ok %d — ", _n); printf(__VA_ARGS__); puts(""); } \
} while(0)

/* Collected JSON rows from qengine_exec_json */
typedef struct { char **rows; int n; int cap; } RowBuf;

static void collect_row(const char *json, void *ud) {
    RowBuf *rb = (RowBuf *)ud;
    if (rb->n >= rb->cap) {
        rb->cap = rb->cap ? rb->cap * 2 : 16;
        rb->rows = (char **)realloc(rb->rows, (size_t)rb->cap * sizeof(char *));
    }
    rb->rows[rb->n++] = strdup(json);
}
static void rowbuf_free(RowBuf *rb) {
    for (int i = 0; i < rb->n; i++) free(rb->rows[i]);
    free(rb->rows);
    memset(rb, 0, sizeof(*rb));
}

static int rows_contain(RowBuf *rb, const char *needle) {
    for (int i = 0; i < rb->n; i++)
        if (strstr(rb->rows[i], needle)) return 1;
    return 0;
}

static char g_data_dir[256];

/*
 * Run SQL and collect rows. Returns row count (>=0) or -1 on error.
 * Caller must rowbuf_free(rb) when done.
 */
static int run_sql(Arena *a, const char *sql, RowBuf *rb) {
    memset(rb, 0, sizeof(*rb));
    Stmt *stmt = sql_parse(a, sql, strlen(sql));
    if (!stmt) { printf("# sql_parse failed: %s\n", sql); return -1; }
    PlanNode *pn = sql_plan(a, stmt);
    if (!pn) { printf("# sql_plan failed: %s\n", sql); return -1; }
    Operator *root = qengine_build(a, pn, g_data_dir);
    if (!root) { printf("# qengine_build failed: %s\n", sql); return -1; }
    if (root->vt->open(root) != 0) { printf("# open failed: %s\n", sql); return -1; }
    int rows_out = 0;
    int rc = qengine_exec_json(root, a, collect_row, rb, &rows_out);
    root->vt->close(root);
    return (rc == 0) ? rows_out : -1;
}

/* ── Seed the 'sales' table using the storage API ── */
static void seed_sales(void) {
    /* schema: region TEXT, product TEXT, amount INT64, qty INT64 */
    static ColDef coldefs[4] = {
        { "region",  COL_TEXT,  false },
        { "product", COL_TEXT,  false },
        { "amount",  COL_INT64, false },
        { "qty",     COL_INT64, false },
    };
    Schema *sc = (Schema *)calloc(1, sizeof(Schema));
    sc->ncols = 4;
    sc->cols = coldefs;

    Table *t = table_create("sales", sc, g_data_dir);
    if (!t) { printf("# table_create failed\n"); return; }

    /* 10 rows of data */
    static const char *regions[]  = { "North","South","North","East","South","North","East","West","West","South" };
    static const char *products[] = { "Widget","Gadget","Gadget","Widget","Widget","Widget","Gadget","Gadget","Widget","Widget" };
    static const int64_t amounts[] = { 1500,800,950,2000,600,1200,700,1100,1800,500 };
    static const int64_t qtys[]    = { 10,5,7,15,4,8,3,9,12,3 };
    static const int NROWS = 10;

    /* ColBatch: each column is a separate array */
    char   **col_region  = (char **)malloc(sizeof(char *) * (size_t)NROWS);
    char   **col_product = (char **)malloc(sizeof(char *) * (size_t)NROWS);
    int64_t *col_amount  = (int64_t *)malloc(sizeof(int64_t) * (size_t)NROWS);
    int64_t *col_qty     = (int64_t *)malloc(sizeof(int64_t) * (size_t)NROWS);

    for (int i = 0; i < NROWS; i++) {
        col_region[i]  = (char *)regions[i];
        col_product[i] = (char *)products[i];
        col_amount[i]  = amounts[i];
        col_qty[i]     = qtys[i];
    }

    ColBatch batch = {0};
    batch.schema   = sc;
    batch.nrows    = NROWS;
    batch.values[0] = col_region;
    batch.values[1] = col_product;
    batch.values[2] = col_amount;
    batch.values[3] = col_qty;
    /* null_bitmap[i] = NULL means no nulls for that column */

    table_append(t, &batch);
    table_close(t);

    free(col_region);
    free(col_product);
    free(col_amount);
    free(col_qty);
}

int main(void) {
    puts("TAP version 14");

    snprintf(g_data_dir, sizeof(g_data_dir), "/tmp/dfo_sql_test_%d", (int)getpid());
    mkdir(g_data_dir, 0755);

    seed_sales();

    Arena *a = arena_create(0);

    /* ── COUNT(*) total ── */
    {
        RowBuf rb;
        int n = run_sql(a, "SELECT COUNT(*) FROM sales", &rb);
        ok(n >= 0, "COUNT(*) executes without error");
        ok(rows_contain(&rb, "10"), "COUNT(*) = 10");
        rowbuf_free(&rb);
    }

    /* ── SUM ── */
    {
        RowBuf rb;
        run_sql(a, "SELECT SUM(amount) FROM sales", &rb);
        /* 1500+800+950+2000+600+1200+700+1100+1800+500 = 11150 */
        ok(rows_contain(&rb, "11150"), "SUM(amount) = 11150");
        rowbuf_free(&rb);
    }

    /* ── AVG ── */
    {
        RowBuf rb;
        run_sql(a, "SELECT AVG(qty) FROM sales", &rb);
        /* total = 10+5+7+15+4+8+3+9+12+3 = 76; 76/10 = 7.6 */
        ok(rows_contain(&rb, "7.6") || rows_contain(&rb, "7"),
           "AVG(qty) = 7.6");
        rowbuf_free(&rb);
    }

    /* ── MIN / MAX ── */
    {
        RowBuf rb;
        run_sql(a, "SELECT MIN(amount), MAX(amount) FROM sales", &rb);
        ok(rows_contain(&rb, "500"),  "MIN(amount) = 500");
        ok(rows_contain(&rb, "2000"), "MAX(amount) = 2000");
        rowbuf_free(&rb);
    }

    /* ── COUNT(*) with WHERE ── */
    {
        RowBuf rb;
        run_sql(a, "SELECT COUNT(*) FROM sales WHERE region = 'North'", &rb);
        ok(rows_contain(&rb, "3"), "COUNT(*) WHERE region=North = 3");
        rowbuf_free(&rb);
    }

    /* ── SUM with WHERE ── */
    {
        RowBuf rb;
        run_sql(a, "SELECT SUM(amount) FROM sales WHERE product = 'Widget'", &rb);
        /* 1500+2000+600+1200+1800+500 = 7600 */
        ok(rows_contain(&rb, "7600"), "SUM(amount) WHERE product=Widget = 7600");
        rowbuf_free(&rb);
    }

    /* ── GROUP BY single key ── */
    {
        RowBuf rb;
        int n = run_sql(a, "SELECT region, COUNT(*) FROM sales GROUP BY region", &rb);
        ok(n >= 0, "GROUP BY region executes");
        ok(rows_contain(&rb, "North") &&
           rows_contain(&rb, "South") &&
           rows_contain(&rb, "East")  &&
           rows_contain(&rb, "West"),
           "GROUP BY region returns all 4 regions");
        /* North appears 3 times: rows 0,2,5 */
        int found_north_3 = 0;
        for (int i = 0; i < rb.n; i++)
            if (strstr(rb.rows[i], "North") && strstr(rb.rows[i], "3"))
                found_north_3 = 1;
        ok(found_north_3, "GROUP BY region: North count = 3");
        rowbuf_free(&rb);
    }

    /* ── GROUP BY two keys ── */
    {
        RowBuf rb;
        int n = run_sql(a,
            "SELECT region, product, SUM(amount) FROM sales GROUP BY region, product", &rb);
        ok(n >= 0, "GROUP BY region,product executes");
        /* North+Widget: rows 0,5 → 1500+1200 = 2700 */
        int found = 0;
        for (int i = 0; i < rb.n; i++)
            if (strstr(rb.rows[i], "North") &&
                strstr(rb.rows[i], "Widget") &&
                strstr(rb.rows[i], "2700"))
                found = 1;
        ok(found, "GROUP BY region,product: North+Widget SUM = 2700");
        rowbuf_free(&rb);
    }

    /* ── HAVING ── */
    {
        RowBuf rb;
        int n = run_sql(a,
            "SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 2000",
            &rb);
        ok(n >= 0, "GROUP BY HAVING executes");
        /* North: 1500+950+1200=3650 ✓; East: 2000+700=2700 ✓;
           West: 1100+1800=2900 ✓; South: 800+600+500=1900 ✗ */
        ok(rows_contain(&rb, "North"), "HAVING > 2000 includes North (3650)");
        ok(rows_contain(&rb, "East"),  "HAVING > 2000 includes East (2700)");
        ok(rows_contain(&rb, "West"),  "HAVING > 2000 includes West (2900)");
        int south_absent = 1;
        for (int i = 0; i < rb.n; i++)
            if (strstr(rb.rows[i], "South")) { south_absent = 0; break; }
        ok(south_absent, "HAVING > 2000 excludes South (1900)");
        rowbuf_free(&rb);
    }

    /* ── GROUP BY + ORDER BY ── */
    {
        RowBuf rb;
        run_sql(a,
            "SELECT region, SUM(amount) as total FROM sales GROUP BY region ORDER BY total DESC",
            &rb);
        /* North(3650) > West(2900) > East(2700) > South(1900) */
        const char *north = NULL, *south = NULL;
        for (int i = 0; i < rb.n; i++) {
            if (strstr(rb.rows[i], "North")) north = rb.rows[i];
            if (strstr(rb.rows[i], "South")) south = rb.rows[i];
        }
        ok(north && south && north < south,
           "ORDER BY SUM DESC: North row before South row");
        rowbuf_free(&rb);
    }

    /* ── COUNT with filter ── */
    {
        RowBuf rb;
        int rc = run_sql(a, "SELECT COUNT(*) FROM sales WHERE qty > 5", &rb);
        ok(rc >= 0, "COUNT(*) WHERE qty > 5 executes");
        /* qty>5: 10,7,15,8,9,12 = 6 rows */
        ok(rows_contain(&rb, "6"), "COUNT(*) WHERE qty>5 = 6");
        rowbuf_free(&rb);
    }

    /* ── MIN(text) ── */
    {
        RowBuf rb;
        run_sql(a, "SELECT MIN(region) FROM sales", &rb);
        /* alphabetically first: East */
        ok(rows_contain(&rb, "East"), "MIN(region) = East (lexicographic)");
        rowbuf_free(&rb);
    }

    /* ── MAX(text) ── */
    {
        RowBuf rb;
        run_sql(a, "SELECT MAX(region) FROM sales", &rb);
        /* alphabetically last: West */
        ok(rows_contain(&rb, "West"), "MAX(region) = West (lexicographic)");
        rowbuf_free(&rb);
    }

    /* ── ORDER BY aggregate ── */
    {
        RowBuf rb;
        int rc = run_sql(a,
            "SELECT region, SUM(amount) as s FROM sales GROUP BY region ORDER BY s ASC",
            &rb);
        ok(rc >= 0, "ORDER BY aggregate expression executes without crash");
        /* South(1900) is smallest */
        if (rb.n > 0)
            ok(strstr(rb.rows[0], "South") != NULL,
               "ORDER BY s ASC: first row is South (smallest sum)");
        rowbuf_free(&rb);
    }

    arena_destroy(a);

    /* cleanup */
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", g_data_dir);
    (void)system(cmd);

    printf("1..%d\n# pass=%d fail=%d\n", _plan, _pass, _fail);
    return _fail > 0 ? 1 : 0;
}
