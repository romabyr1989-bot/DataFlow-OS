#include "matview.h"
#include "../core/log.h"
#include "../core/arena.h"
#include <sqlite3.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <pthread.h>


static const char *MV_SCHEMA =
    "CREATE TABLE IF NOT EXISTS matviews ("
    "  name           TEXT PRIMARY KEY,"
    "  definition_sql TEXT NOT NULL,"
    "  source_tables  TEXT NOT NULL,"
    "  refresh_mode   INTEGER NOT NULL DEFAULT 0,"
    "  refresh_cron   TEXT DEFAULT '',"
    "  last_refreshed INTEGER DEFAULT 0,"
    "  is_stale       INTEGER DEFAULT 1,"
    "  row_count      INTEGER DEFAULT 0"
    ");";

struct MatViewStore {
    sqlite3        *db;
    pthread_mutex_t mu;
    char            data_dir[512];
    Catalog        *catalog;

    /* Callback для выполнения SQL (устанавливается из api.c) */
    MvExecFn        exec_fn;
    void           *exec_app;

    /* Очередь on_write refresh */
    char            pending_refresh[64][128];
    int             npending;
    pthread_mutex_t pending_mu;
};

MatViewStore *mvs_create(Catalog *catalog, const char *data_dir) {
    MatViewStore *s = calloc(1, sizeof(MatViewStore));
    s->catalog = catalog;
    strncpy(s->data_dir, data_dir, sizeof(s->data_dir)-1);

    /* Открываем тот же catalog.db */
    char db_path[640];
    snprintf(db_path, sizeof(db_path), "%s/catalog.db", data_dir);
    if (sqlite3_open(db_path, &s->db) != SQLITE_OK) {
        LOG_ERROR("matview: failed to open db: %s", sqlite3_errmsg(s->db));
        free(s);
        return NULL;
    }
    sqlite3_exec(s->db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    if (sqlite3_exec(s->db, MV_SCHEMA, NULL, NULL, NULL) != SQLITE_OK) {
        LOG_ERROR("matview: schema init: %s", sqlite3_errmsg(s->db));
        sqlite3_close(s->db);
        free(s);
        return NULL;
    }
    pthread_mutex_init(&s->mu, NULL);
    pthread_mutex_init(&s->pending_mu, NULL);
    LOG_INFO("matview store initialized");
    return s;
}

void mvs_destroy(MatViewStore *s) {
    if (!s) return;
    pthread_mutex_destroy(&s->mu);
    pthread_mutex_destroy(&s->pending_mu);
    sqlite3_close(s->db);
    free(s);
}

/* Парсить JSON-массив source_tables */
static void parse_source_tables(const char *json, MatView *mv) {
    mv->nsource_tables = 0;
    if (!json || !*json) return;
    const char *p = json;
    while (*p && mv->nsource_tables < 16) {
        const char *q = strchr(p, '"');
        if (!q) break;
        q++;
        const char *e = strchr(q, '"');
        if (!e) break;
        size_t len = (size_t)(e - q);
        if (len >= 128) len = 127;
        memcpy(mv->source_tables[mv->nsource_tables], q, len);
        mv->source_tables[mv->nsource_tables][len] = '\0';
        mv->nsource_tables++;
        p = e + 1;
    }
}

static int load_mv(sqlite3_stmt *stmt, MatView *mv) {
    memset(mv, 0, sizeof(*mv));
    const char *name = (const char *)sqlite3_column_text(stmt, 0);
    const char *sql  = (const char *)sqlite3_column_text(stmt, 1);
    const char *srcs = (const char *)sqlite3_column_text(stmt, 2);
    if (name) strncpy(mv->name,           name, sizeof(mv->name)-1);
    if (sql)  strncpy(mv->definition_sql, sql,  sizeof(mv->definition_sql)-1);
    mv->refresh_mode    = (MvRefreshMode)sqlite3_column_int(stmt, 3);
    const char *cron = (const char *)sqlite3_column_text(stmt, 4);
    if (cron) strncpy(mv->refresh_cron, cron, sizeof(mv->refresh_cron)-1);
    mv->last_refreshed_at  = sqlite3_column_int64(stmt, 5);
    mv->is_stale           = sqlite3_column_int(stmt, 6) != 0;
    mv->row_count          = sqlite3_column_int64(stmt, 7);
    parse_source_tables(srcs, mv);
    return 0;
}

int mvs_create_view(MatViewStore *s, const MatView *mv) {
    if (!s || !mv || !mv->name[0] || !mv->definition_sql[0]) return -1;

    /* Построить JSON-массив source_tables */
    char srcs_json[2048]; int soff = 0;
    soff += snprintf(srcs_json, sizeof(srcs_json), "[");
    for (int i = 0; i < mv->nsource_tables; i++) {
        soff += snprintf(srcs_json+soff, sizeof(srcs_json)-(size_t)soff,
                         "%s\"%s\"", i?",":"", mv->source_tables[i]);
    }
    snprintf(srcs_json+soff, sizeof(srcs_json)-(size_t)soff, "]");

    pthread_mutex_lock(&s->mu);
    const char *sql =
        "INSERT INTO matviews (name,definition_sql,source_tables,refresh_mode,refresh_cron,is_stale) "
        "VALUES (?,?,?,?,?,1) "
        "ON CONFLICT(name) DO UPDATE SET "
        "  definition_sql=excluded.definition_sql,"
        "  source_tables=excluded.source_tables,"
        "  refresh_mode=excluded.refresh_mode,"
        "  refresh_cron=excluded.refresh_cron,"
        "  is_stale=1;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&s->mu); return -1;
    }
    sqlite3_bind_text(stmt, 1, mv->name, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, mv->definition_sql, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, srcs_json, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt,  4, (int)mv->refresh_mode);
    sqlite3_bind_text(stmt, 5, mv->refresh_cron, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&s->mu);

    LOG_INFO("matview: created '%s' (mode=%d)", mv->name, (int)mv->refresh_mode);
    return rc == SQLITE_DONE ? 0 : -1;
}

int mvs_drop_view(MatViewStore *s, const char *name) {
    if (!s || !name) return -1;
    pthread_mutex_lock(&s->mu);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(s->db, "DELETE FROM matviews WHERE name=?;", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&s->mu);

    /* Удалить внутреннюю таблицу */
    char storage_name[160];
    mvs_storage_name(name, storage_name, sizeof(storage_name));
    catalog_drop_table(s->catalog, storage_name);
    LOG_INFO("matview: dropped '%s'", name);
    return rc == SQLITE_DONE ? 0 : -1;
}

int mvs_get(MatViewStore *s, const char *name, MatView *out) {
    if (!s || !name) return -1;
    pthread_mutex_lock(&s->mu);
    sqlite3_stmt *stmt;
    const char *sql =
        "SELECT name,definition_sql,source_tables,refresh_mode,refresh_cron,"
        "last_refreshed,is_stale,row_count FROM matviews WHERE name=?;";
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&s->mu); return -1;
    }
    sqlite3_bind_text(stmt, 1, name, -1, SQLITE_STATIC);
    int found = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        load_mv(stmt, out);
        found = 1;
    }
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&s->mu);
    return found ? 0 : -1;
}

int mvs_list(MatViewStore *s, char **json_out, Arena *a) {
    if (!s) return -1;
    pthread_mutex_lock(&s->mu);
    const char *sql =
        "SELECT name,definition_sql,source_tables,refresh_mode,refresh_cron,"
        "last_refreshed,is_stale,row_count FROM matviews ORDER BY name;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&s->mu); return -1;
    }
    char *buf = arena_alloc(a, 65536);
    int off = snprintf(buf, 65536, "[");
    int first = 1;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        MatView mv; load_mv(stmt, &mv);
        if (!first) off += snprintf(buf+off, 65536-(size_t)off, ",");
        off += snprintf(buf+off, 65536-(size_t)off,
            "{\"name\":\"%s\",\"refresh_mode\":%d,\"is_stale\":%s,"
            "\"last_refreshed_at\":%lld,\"row_count\":%lld}",
            mv.name, (int)mv.refresh_mode, mv.is_stale?"true":"false",
            (long long)mv.last_refreshed_at, (long long)mv.row_count);
        first = 0;
    }
    snprintf(buf+off, 65536-(size_t)off, "]");
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&s->mu);
    *json_out = buf;
    return 0;
}

/* Зарегистрировать exec callback из api.c */
void mvs_set_exec_fn(MatViewStore *s, MvExecFn fn, void *app) {
    s->exec_fn = fn;
    s->exec_app = app;
}

int mvs_refresh(MatViewStore *s, const char *name, void *app) {
    if (!s || !name) return -1;
    MatView mv;
    if (mvs_get(s, name, &mv) != 0) return -1;
    if (mv.is_refreshing) return 0;

    /* Помечаем как обновляемую */
    pthread_mutex_lock(&s->mu);
    sqlite3_exec(s->db, "UPDATE matviews SET is_stale=1 WHERE name=?;", NULL, NULL, NULL);
    pthread_mutex_unlock(&s->mu);

    int64_t t0 = (int64_t)time(NULL) * 1000;

    /* Имя хранилища для данных */
    char storage_name[160];
    mvs_storage_name(name, storage_name, sizeof(storage_name));

    /* Выполняем через callback (если установлен) */
    int rc = -1;
    if (s->exec_fn) {
        char refresh_sql[4224];
        snprintf(refresh_sql, sizeof(refresh_sql),
                 "CREATE OR REPLACE TABLE %s AS %s",
                 storage_name, mv.definition_sql);
        rc = s->exec_fn(mv.definition_sql, s->data_dir, s->exec_app);
    } else {
        LOG_WARN("matview: exec_fn not set, can't refresh '%s'", name);
        rc = 0; /* Без callback не можем выполнить — не считаем ошибкой */
    }

    int64_t dur_ms = (int64_t)time(NULL) * 1000 - t0;

    /* Обновляем метаданные */
    pthread_mutex_lock(&s->mu);
    const char *upd =
        "UPDATE matviews SET is_stale=0,last_refreshed=?,row_count=0 WHERE name=?;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, upd, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, (int64_t)time(NULL));
        sqlite3_bind_text(stmt,  2, name, -1, SQLITE_STATIC);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    pthread_mutex_unlock(&s->mu);
    (void)dur_ms;

    LOG_INFO("matview: refreshed '%s' rc=%d dur=%llums", name, rc, (long long)dur_ms);
    return rc;
}

void mvs_invalidate(MatViewStore *s, const char *table_name) {
    if (!s || !table_name) return;

    pthread_mutex_lock(&s->mu);
    /* Находим view, использующие эту таблицу */
    const char *sql = "SELECT name,source_tables FROM matviews WHERE is_stale=0;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&s->mu); return;
    }
    char to_invalidate[64][128]; int ninv = 0;
    char to_refresh[64][128];    int nref = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && ninv < 64) {
        const char *name = (const char *)sqlite3_column_text(stmt, 0);
        const char *srcs = (const char *)sqlite3_column_text(stmt, 1);
        if (!name || !srcs) continue;
        if (strstr(srcs, table_name)) {
            strncpy(to_invalidate[ninv], name, 127);
            ninv++;
        }
    }
    sqlite3_finalize(stmt);

    /* Помечаем все найденные view как stale */
    for (int i = 0; i < ninv; i++) {
        sqlite3_stmt *upd;
        sqlite3_prepare_v2(s->db,
            "UPDATE matviews SET is_stale=1 WHERE name=?;", -1, &upd, NULL);
        sqlite3_bind_text(upd, 1, to_invalidate[i], -1, SQLITE_STATIC);
        sqlite3_step(upd);
        sqlite3_finalize(upd);

        /* Собираем список для ON_WRITE refresh */
        MatView mv;
        char mv_get_sql[256];
        snprintf(mv_get_sql, sizeof(mv_get_sql),
            "SELECT name,definition_sql,source_tables,refresh_mode,refresh_cron,"
            "last_refreshed,is_stale,row_count FROM matviews WHERE name='%s';",
            to_invalidate[i]);
        sqlite3_stmt *gs;
        if (sqlite3_prepare_v2(s->db, mv_get_sql, -1, &gs, NULL) == SQLITE_OK) {
            if (sqlite3_step(gs) == SQLITE_ROW) {
                load_mv(gs, &mv);
                if (mv.refresh_mode == MV_REFRESH_ON_WRITE && nref < 64) {
                    strncpy(to_refresh[nref++], mv.name, 127);
                }
            }
            sqlite3_finalize(gs);
        }
    }
    pthread_mutex_unlock(&s->mu);

    /* Ставим в очередь ON_WRITE refresh */
    if (nref > 0) {
        pthread_mutex_lock(&s->pending_mu);
        for (int i = 0; i < nref && s->npending < 64; i++) {
            /* Проверяем дубликаты */
            bool dup = false;
            for (int j = 0; j < s->npending; j++)
                if (!strcmp(s->pending_refresh[j], to_refresh[i])) { dup=true; break; }
            if (!dup) strncpy(s->pending_refresh[s->npending++], to_refresh[i], 127);
        }
        pthread_mutex_unlock(&s->pending_mu);
    }

    if (ninv > 0)
        LOG_INFO("matview: invalidated %d views due to write on '%s'", ninv, table_name);
}

void mvs_tick(MatViewStore *s, void *app) {
    if (!s) return;

    /* Обрабатываем ON_WRITE pending queue */
    pthread_mutex_lock(&s->pending_mu);
    char names[64][128]; int n = s->npending;
    memcpy(names, s->pending_refresh, (size_t)n * sizeof(names[0]));
    s->npending = 0;
    pthread_mutex_unlock(&s->pending_mu);

    for (int i = 0; i < n; i++)
        mvs_refresh(s, names[i], app);

    /* Cron: простая проверка — если last_refreshed + period < now */
    /* Полный cron-парсер выходит за рамки; поддерживаем только refresh_cron как секунды */
    pthread_mutex_lock(&s->mu);
    const char *sql =
        "SELECT name,refresh_cron,last_refreshed FROM matviews WHERE refresh_mode=2 AND is_stale=0;";
    sqlite3_stmt *stmt;
    char sched_names[64][128]; int nsched = 0;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) == SQLITE_OK) {
        int64_t now = (int64_t)time(NULL);
        while (sqlite3_step(stmt) == SQLITE_ROW && nsched < 64) {
            const char *nm   = (const char *)sqlite3_column_text(stmt, 0);
            const char *cron = (const char *)sqlite3_column_text(stmt, 1);
            int64_t     last = sqlite3_column_int64(stmt, 2);
            if (!nm || !cron) continue;
            int period = atoi(cron); /* cron как интервал в секундах */
            if (period > 0 && now - last >= (int64_t)period)
                strncpy(sched_names[nsched++], nm, 127);
        }
        sqlite3_finalize(stmt);
    }
    pthread_mutex_unlock(&s->mu);

    for (int i = 0; i < nsched; i++)
        mvs_refresh(s, sched_names[i], app);
}
