/* TAP tests for arena allocator */
#include "../../lib/core/arena.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>

static int plan = 0, pass = 0, fail = 0;

#define ok(cond, ...) do { \
    plan++; \
    if(cond){pass++;printf("ok %d — " __VA_ARGS__);puts("");} \
    else    {fail++;printf("not ok %d — " __VA_ARGS__);puts("");} \
} while(0)

int main(void) {
    puts("TAP version 14");

    /* basic alloc */
    Arena *a = arena_create(0);
    ok(a != NULL, "arena_create succeeds");

    void *p1 = arena_alloc(a, 64);
    ok(p1 != NULL, "alloc 64 bytes");

    /* alignment */
    void *p2 = arena_alloc(a, 1);
    ok(((uintptr_t)p2 % ARENA_ALIGN) == 0, "allocation is aligned");

    /* strdup */
    char *s = arena_strdup(a, "hello world");
    ok(s && strcmp(s, "hello world") == 0, "arena_strdup works");

    /* sprintf */
    char *fmt = arena_sprintf(a, "value=%d", 42);
    ok(fmt && strcmp(fmt, "value=42") == 0, "arena_sprintf works");

    /* mark/restore */
    ArenaMark m = arena_mark(a);
    char *tmp = arena_alloc(a, 4096);
    ok(tmp != NULL, "alloc after mark");
    arena_restore(a, m);
    /* after restore, next alloc should reclaim space */
    char *tmp2 = arena_alloc(a, 4096);
    ok(tmp2 != NULL, "alloc after restore succeeds");

    /* grow beyond block */
    Arena *b = arena_create(64);
    void *big = arena_alloc(b, 65536);
    ok(big != NULL, "alloc larger than initial block");

    /* reset */
    arena_reset(a);
    void *after_reset = arena_alloc(a, 16);
    ok(after_reset != NULL, "alloc after reset");

    arena_destroy(a);
    arena_destroy(b);
    ok(1, "destroy succeeds (no crash)");

    printf("1..%d\n", plan);
    printf("# pass=%d fail=%d\n", pass, fail);
    return fail > 0 ? 1 : 0;
}
