#pragma once
#include "auth.h"
#include "../core/arena.h"
#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>

typedef enum {
    ACTION_TABLE_READ    = 1 << 0,
    ACTION_TABLE_WRITE   = 1 << 1,
    ACTION_TABLE_CREATE  = 1 << 2,
    ACTION_TABLE_DROP    = 1 << 3,
    ACTION_PIPELINE_RUN  = 1 << 4,
    ACTION_PIPELINE_EDIT = 1 << 5,
    ACTION_ADMIN         = 1 << 6,
    ACTION_METRICS_READ  = 1 << 7,
} RbacAction;

typedef struct {
    int      id;
    AuthRole role;
    char     table_pattern[128];
    uint32_t allowed_actions;
    char     row_filter[512];
} RbacPolicy;

typedef struct RbacStore {
    sqlite3         *db;
    pthread_mutex_t  mu;
    bool             enabled;
} RbacStore;

RbacStore *rbac_store_create(const char *db_path, bool enabled);
void       rbac_store_destroy(RbacStore *s);

/* 0 = разрешено, -1 = запрещено */
int rbac_check(RbacStore *s, const AuthClaims *claims,
               RbacAction action, const char *table_name);

/* NULL если нет RLS; иначе WHERE-выражение с подставленным user_id */
const char *rbac_row_filter(RbacStore *s, const AuthClaims *claims,
                             const char *table_name, Arena *a);

int rbac_policy_set(RbacStore *s, AuthRole role, const char *table_pattern,
                    uint32_t actions, const char *row_filter);
int rbac_policy_del(RbacStore *s, int policy_id);
int rbac_policy_list(RbacStore *s, AuthRole role, char **json_out, Arena *a);

/* Устанавливает дефолтные политики:
   admin:   ACTION_ADMIN|все права на "*"
   analyst: TABLE_READ|TABLE_WRITE|PIPELINE_RUN|PIPELINE_EDIT|METRICS_READ на "*"
   viewer:  TABLE_READ|METRICS_READ на "*" */
void rbac_init_defaults(RbacStore *s);
