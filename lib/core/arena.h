#pragma once
#include <stddef.h>
#include <stdint.h>

#define ARENA_DEFAULT_BLOCK (65536)
#define ARENA_ALIGN         16

typedef struct ArenaBlock {
    struct ArenaBlock *prev;
    size_t cap, used;
    uint8_t data[];
} ArenaBlock;

typedef struct {
    ArenaBlock *top;
    size_t total_allocated, total_wasted;
} Arena;

Arena    *arena_create(size_t initial_size);
void      arena_destroy(Arena *a);
void     *arena_alloc(Arena *a, size_t sz);
void     *arena_calloc(Arena *a, size_t sz);
char     *arena_strdup(Arena *a, const char *s);
char     *arena_strndup(Arena *a, const char *s, size_t n);
char     *arena_sprintf(Arena *a, const char *fmt, ...) __attribute__((format(printf,2,3)));
void      arena_reset(Arena *a);

typedef struct { ArenaBlock *block; size_t used; } ArenaMark;
ArenaMark arena_mark(Arena *a);
void      arena_restore(Arena *a, ArenaMark m);
