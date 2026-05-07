#pragma once
#include "../storage/storage.h"
#include "../core/arena.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>

typedef enum {
    MV_REFRESH_MANUAL   = 0,
    MV_REFRESH_ON_WRITE = 1,
    MV_REFRESH_SCHEDULE = 2,
} MvRefreshMode;

typedef struct {
    char          name[128];
    char          definition_sql[4096];
    char          source_tables[16][128];
    int           nsource_tables;
    MvRefreshMode refresh_mode;
    char          refresh_cron[64];
    int64_t       last_refreshed_at;
    int64_t       refresh_duration_ms;
    bool          is_stale;
    int64_t       row_count;
    bool          is_refreshing;
} MatView;

typedef struct MatViewStore MatViewStore;

/* App forward declaration — избегаем циклических include */
struct App;

MatViewStore *mvs_create(Catalog *catalog, const char *data_dir);
void          mvs_destroy(MatViewStore *s);

int  mvs_create_view(MatViewStore *s, const MatView *mv);
int  mvs_drop_view(MatViewStore *s, const char *name);
int  mvs_get(MatViewStore *s, const char *name, MatView *out);
int  mvs_list(MatViewStore *s, char **json_out, Arena *a);
int  mvs_refresh(MatViewStore *s, const char *name, void *app);

/* Отметить все view, использующие table_name, как устаревшие */
void mvs_invalidate(MatViewStore *s, const char *table_name);

/* Вызывается планировщиком: обрабатывает schedule + on_write refresh */
void mvs_tick(MatViewStore *s, void *app);

/* Зарегистрировать exec callback (вызывается из main.c после инициализации) */
typedef int (*MvExecFn)(const char *sql, const char *data_dir, void *app);
void mvs_set_exec_fn(MatViewStore *s, MvExecFn fn, void *app);

/* Имя внутренней таблицы для хранения данных матвью */
static inline void mvs_storage_name(const char *name, char *out, size_t len) {
    snprintf(out, len, "_mv_%s", name);
}
