#pragma once
#include "arena.h"
#include <stdint.h>
#include <stdbool.h>

/* Open-addressing hashmap, Robin Hood probing, string keys */
typedef struct HMEntry {
    const char *key;
    void       *val;
    uint32_t    hash;
    uint32_t    psl;   /* probe sequence length */
} HMEntry;

typedef struct {
    HMEntry *slots;
    uint32_t cap;
    uint32_t count;
    Arena   *arena; /* for key copies */
} HashMap;

void  hm_init(HashMap *m, Arena *a, uint32_t initial_cap);
void  hm_set(HashMap *m, const char *key, void *val);
void *hm_get(HashMap *m, const char *key);
bool  hm_del(HashMap *m, const char *key);
void  hm_clear(HashMap *m);

/* iterate: idx=0 start; returns next idx or -1 */
int   hm_next(const HashMap *m, int idx, const char **key_out, void **val_out);
