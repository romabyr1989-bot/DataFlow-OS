#include "hashmap.h"
#include <stdlib.h>
#include <string.h>

static uint32_t fnv1a(const char *s) {
    uint32_t h = 2166136261u;
    while (*s) { h ^= (uint8_t)*s++; h *= 16777619u; }
    return h ? h : 1;
}

static void hm_grow(HashMap *m);

void hm_init(HashMap *m, Arena *a, uint32_t cap) {
    if (cap < 8) cap = 8;
    /* round up to power of 2 */
    uint32_t c = 1; while (c < cap) c <<= 1;
    m->slots = calloc(c, sizeof(HMEntry));
    m->cap   = c; m->count = 0; m->arena = a;
}

static void hm_insert_slot(HMEntry *slots, uint32_t cap, HMEntry e) {
    uint32_t idx = e.hash & (cap - 1);
    for (;;) {
        HMEntry *s = &slots[idx];
        if (!s->key) { *s = e; return; }
        if (s->psl < e.psl) { HMEntry tmp = *s; *s = e; e = tmp; }
        e.psl++; idx = (idx + 1) & (cap - 1);
    }
}

static void hm_grow(HashMap *m) {
    uint32_t newcap = m->cap * 2;
    HMEntry *newslots = calloc(newcap, sizeof(HMEntry));
    for (uint32_t i = 0; i < m->cap; i++) {
        if (m->slots[i].key) {
            HMEntry e = m->slots[i]; e.psl = 0;
            hm_insert_slot(newslots, newcap, e);
        }
    }
    free(m->slots);
    m->slots = newslots; m->cap = newcap;
}

void hm_set(HashMap *m, const char *key, void *val) {
    if (m->count * 4 >= m->cap * 3) hm_grow(m);
    uint32_t h = fnv1a(key);
    uint32_t idx = h & (m->cap - 1);
    uint32_t psl = 0;
    /* check existing */
    for (uint32_t i = 0; i < m->cap; i++) {
        HMEntry *s = &m->slots[(idx + i) & (m->cap - 1)];
        if (!s->key) break;
        if (s->hash == h && strcmp(s->key, key) == 0) { s->val = val; return; }
    }
    const char *k = m->arena ? arena_strdup(m->arena, key) : strdup(key);
    HMEntry e = {k, val, h, psl};
    hm_insert_slot(m->slots, m->cap, e);
    m->count++;
}

void *hm_get(HashMap *m, const char *key) {
    uint32_t h = fnv1a(key), idx = h & (m->cap - 1);
    for (uint32_t i = 0; i < m->cap; i++) {
        HMEntry *s = &m->slots[(idx + i) & (m->cap - 1)];
        if (!s->key) return NULL;
        if (s->hash == h && strcmp(s->key, key) == 0) return s->val;
        if (s->psl < i) return NULL;
    }
    return NULL;
}

bool hm_del(HashMap *m, const char *key) {
    uint32_t h = fnv1a(key), idx = h & (m->cap - 1);
    for (uint32_t i = 0; i < m->cap; i++) {
        uint32_t pos = (idx + i) & (m->cap - 1);
        HMEntry *s = &m->slots[pos];
        if (!s->key) return false;
        if (s->hash == h && strcmp(s->key, key) == 0) {
            s->key = NULL; m->count--;
            /* backward shift deletion */
            for (;;) {
                uint32_t npos = (pos + 1) & (m->cap - 1);
                HMEntry *n = &m->slots[npos];
                if (!n->key || n->psl == 0) { m->slots[pos].key = NULL; break; }
                m->slots[pos] = *n; m->slots[pos].psl--;
                pos = npos;
            }
            return true;
        }
        if (s->psl < i) return false;
    }
    return false;
}

void hm_clear(HashMap *m) { memset(m->slots, 0, m->cap * sizeof(HMEntry)); m->count = 0; }

int hm_next(const HashMap *m, int idx, const char **k, void **v) {
    for (int i = idx; i < (int)m->cap; i++) {
        if (m->slots[i].key) { if (k) *k = m->slots[i].key; if (v) *v = m->slots[i].val; return i + 1; }
    }
    return -1;
}
