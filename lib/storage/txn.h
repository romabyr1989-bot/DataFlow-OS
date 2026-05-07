#pragma once
#include "storage.h"
#include "../core/arena.h"
#include <stdint.h>
#include <pthread.h>
#include <stdbool.h>

#define TXN_MAX_ACTIVE  64
#define TXN_MAX_ENTRIES 4096
#define TXN_ID_NONE     0

typedef uint64_t TxnId;

typedef enum {
    TXN_ACTIVE    = 0,
    TXN_COMMITTED = 1,
    TXN_ABORTED   = 2,
} TxnStatus;

typedef enum {
    TXN_OP_INSERT = 0,
    TXN_OP_DELETE = 1,
    TXN_OP_UPDATE = 2,
} TxnOpType;

/* One buffered operation */
typedef struct {
    char       table[128];
    TxnOpType  op;
    ColBatch  *batch;         /* TXN_OP_INSERT: deep-copied batch */
    int64_t    orig_offset;   /* TXN_OP_DELETE / UPDATE: WAL offset */
    char      *new_csv;       /* TXN_OP_UPDATE: new row CSV */
    size_t     csv_len;
} TxnEntry;

typedef struct {
    TxnId      id;
    TxnStatus  status;
    int64_t    begin_ts;      /* unix timestamp at BEGIN */
    TxnEntry   entries[TXN_MAX_ENTRIES];
    int        nentries;
    Arena     *arena;         /* lives from BEGIN to COMMIT/ROLLBACK */
} Txn;

typedef struct {
    Txn             txns[TXN_MAX_ACTIVE];
    pthread_mutex_t mu;
    TxnId           next_id;   /* monotonically increasing */
} TxnManager;

/*
 * Callback invoked once per buffered entry during txn_commit.
 * Returns 0 on success, -1 on failure (causes commit to abort).
 */
typedef int (*TxnApplyFn)(const char *table, TxnOpType op,
                           ColBatch *batch,
                           int64_t orig_offset,
                           const char *new_csv, size_t csv_len,
                           void *userdata);

TxnManager *txn_manager_create(void);
void        txn_manager_destroy(TxnManager *tm);

/* Roll back all active transactions older than timeout_sec seconds. */
void        txn_manager_timeout_check(TxnManager *tm, int timeout_sec);

/* Lifecycle */
TxnId  txn_begin(TxnManager *tm);
int    txn_commit(TxnManager *tm, TxnId id, TxnApplyFn fn, void *userdata);
int    txn_rollback(TxnManager *tm, TxnId id);
Txn   *txn_find(TxnManager *tm, TxnId id); /* returns NULL if not found/active */

/* Buffer write operations (deep-copy data into txn->arena) */
int txn_buffer_insert(TxnManager *tm, TxnId id,
                      const char *table, const ColBatch *batch);
int txn_buffer_delete(TxnManager *tm, TxnId id,
                      const char *table, int64_t orig_offset);
int txn_buffer_update(TxnManager *tm, TxnId id,
                      const char *table, int64_t orig_offset,
                      const char *new_csv, size_t csv_len);
