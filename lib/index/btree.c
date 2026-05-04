#include "btree.h"
#include "../core/log.h"
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>

/* ── Constants ── */
#define MAGIC    UINT64_C(0x44464F42545245FF)
#define VERSION  1

#define PT_LEAF  1
#define PT_INT   2

/* Capacities:
 *   Leaf  page payload = 4096 - 8 = 4088 bytes → 4088/16 = 255 entries
 *   Internal page payload = 4096 - 8 = 4088 bytes
 *     Layout: child0(4) + N*(key(8)+child(4)) → 4 + N*12 ≤ 4088
 *             N ≤ (4088-4)/12 = 340 */
#define LEAF_MAX  255
#define INT_MAX   340

typedef uint8_t Page[BT_PAGE_SIZE];

struct BTree {
    int     fd;
    int32_t root;    /* page index of current root */
    int32_t npages;  /* total allocated pages      */
    int64_t nkeys;   /* total inserted key count   */
};

/* ── File header — stored verbatim in page 0 ── */
typedef struct __attribute__((packed)) {
    uint64_t magic;
    uint32_t version;
    int32_t  root;
    int64_t  nkeys;
    int32_t  npages;
} FHdr;

/* ── Page header — first 8 bytes of every B-tree page ── */
typedef struct __attribute__((packed)) {
    uint8_t  type;   /* PT_LEAF or PT_INT                        */
    uint8_t  _pad;
    uint16_t n;      /* number of keys stored                    */
    int32_t  next;   /* leaf: next sibling page; internal: -1   */
} PHdr;

/* ── Leaf entry ── 16 bytes ── */
typedef struct __attribute__((packed)) { int64_t key; int64_t off; } LE;

/* ─────────────────────────────────────────────────────────────
   Internal page data layout (after 8-byte PHdr):
     [int32_t child[0]]
     [int64_t key[0]]  [int32_t child[1]]
     [int64_t key[1]]  [int32_t child[2]]
     ...
     [int64_t key[n-1]] [int32_t child[n]]
   Offsets from data start (p+8):
     child[0]   → byte 0
     key[i]     → byte 4 + i*12
     child[i+1] → byte 4 + i*12 + 8
   ───────────────────────────────────────────────────────────── */

/* ── Raw page I/O ── */
static int rd(int fd, int32_t pg, Page p) {
    ssize_t r = pread(fd, p, BT_PAGE_SIZE, (off_t)pg * BT_PAGE_SIZE);
    return r == BT_PAGE_SIZE ? 0 : -1;
}
static int wr(int fd, int32_t pg, const Page p) {
    ssize_t w = pwrite(fd, p, BT_PAGE_SIZE, (off_t)pg * BT_PAGE_SIZE);
    return w == BT_PAGE_SIZE ? 0 : -1;
}
static int32_t alloc_pg(BTree *t) {
    int32_t no = t->npages++;
    Page blank; memset(blank, 0, BT_PAGE_SIZE);
    wr(t->fd, no, blank);
    return no;
}
static void flush_hdr(BTree *t) {
    Page p; memset(p, 0, BT_PAGE_SIZE);
    FHdr *h = (FHdr *)p;
    h->magic = MAGIC; h->version = VERSION;
    h->root = t->root; h->nkeys = t->nkeys; h->npages = t->npages;
    wr(t->fd, 0, p);
}

/* ── Leaf accessors ── */
static PHdr *L_hdr(Page p)       { return (PHdr *)p; }
static LE   *L_ent(Page p, int i){ return (LE *)(p + 8) + i; }
static void  L_init(Page p)      { memset(p, 0, BT_PAGE_SIZE); L_hdr(p)->type=PT_LEAF; L_hdr(p)->next=-1; }

/* ── Internal accessors ── */
static PHdr*   I_hdr(Page p)          { return (PHdr *)p; }
static int32_t I_ch (Page p, int i)   {
    const uint8_t *d = p + 8;
    return i == 0 ? *(const int32_t *)d
                  : *(const int32_t *)(d + 4 + (size_t)(i-1)*12 + 8);
}
static int64_t I_key(Page p, int i)   { return *(int64_t *)(p + 8 + 4 + (size_t)i*12); }
static void I_set_ch(Page p, int i, int32_t c) {
    uint8_t *d = p + 8;
    if (i == 0) *(int32_t *)d = c;
    else *(int32_t *)(d + 4 + (size_t)(i-1)*12 + 8) = c;
}
static void I_set_key(Page p, int i, int64_t k) { *(int64_t *)(p + 8 + 4 + (size_t)i*12) = k; }
static void I_init(Page p) { memset(p, 0, BT_PAGE_SIZE); I_hdr(p)->type=PT_INT; I_hdr(p)->next=-1; }

/* ── Leaf insert ──
 * Returns 0  = inserted, no split.
 * Returns 1  = split; left half stays at pgno, new right page at *rnew,
 *              first key of right half is *mk (push up to parent). */
static int leaf_ins(BTree *t, int32_t pgno, int64_t key, int64_t off,
                    int64_t *mk, int32_t *rnew) {
    Page p; rd(t->fd, pgno, p);
    PHdr *h = L_hdr(p);
    int n = h->n;
    int pos = 0;
    while (pos < n && L_ent(p, pos)->key <= key) pos++;

    if (n < LEAF_MAX) {
        memmove(L_ent(p, pos+1), L_ent(p, pos), (size_t)(n-pos) * sizeof(LE));
        L_ent(p, pos)->key = key; L_ent(p, pos)->off = off;
        h->n++;
        wr(t->fd, pgno, p);
        return 0;
    }

    /* gather all LEAF_MAX+1 entries into tmp */
    LE tmp[LEAF_MAX + 1];
    memcpy(tmp, L_ent(p, 0), (size_t)pos * sizeof(LE));
    tmp[pos].key = key; tmp[pos].off = off;
    memcpy(&tmp[pos+1], L_ent(p, pos), (size_t)(n-pos) * sizeof(LE));

    int mid = (LEAF_MAX + 1) / 2;   /* = 128 */

    /* left stays in pgno */
    memcpy(L_ent(p, 0), tmp, (size_t)mid * sizeof(LE));
    h->n = mid;

    /* right goes to new page */
    int32_t rno = alloc_pg(t);
    Page rp; L_init(rp);
    int rn = LEAF_MAX + 1 - mid;
    memcpy(L_ent(rp, 0), &tmp[mid], (size_t)rn * sizeof(LE));
    L_hdr(rp)->n = rn;
    L_hdr(rp)->next = h->next;
    h->next = rno;

    wr(t->fd, pgno, p);
    wr(t->fd, rno,  rp);
    *mk = tmp[mid].key; *rnew = rno;
    return 1;
}

/* ── Internal insert ──
 * Inserts (key, right_child) into the in-memory page p.
 * Returns 0  = inserted (p updated in-place).
 * Returns 1  = split; left stays in p, right content in rp_out,
 *              pushed-up key is *mk. */
static int int_ins(Page p, int64_t key, int32_t rc, int64_t *mk, Page rp_out) {
    PHdr *h = I_hdr(p);
    int n = h->n;
    int pos = 0;
    while (pos < n && key >= I_key(p, pos)) pos++;

    if (n < INT_MAX) {
        for (int i = n-1; i >= pos; i--) {
            I_set_key(p, i+1, I_key(p, i));
            I_set_ch(p, i+2, I_ch(p, i+1));
        }
        I_set_key(p, pos, key);
        I_set_ch(p, pos+1, rc);
        h->n++;
        return 0;
    }

    /* build tmp arrays and insert */
    int64_t tkeys[INT_MAX + 1];
    int32_t tchs [INT_MAX + 2];
    for (int i = 0; i <= n; i++) tchs[i]  = I_ch(p, i);
    for (int i = 0; i < n;  i++) tkeys[i] = I_key(p, i);

    memmove(&tkeys[pos+1], &tkeys[pos], (size_t)(n-pos) * sizeof(int64_t));
    memmove(&tchs[pos+2],  &tchs[pos+1], (size_t)(n-pos) * sizeof(int32_t));
    tkeys[pos] = key; tchs[pos+1] = rc;
    /* now n+1 keys, n+2 children */

    int mid = (n + 1) / 2;  /* key at mid is pushed up, not stored */

    /* rebuild left half */
    I_init(p); h = I_hdr(p); h->n = mid;
    I_set_ch(p, 0, tchs[0]);
    for (int i = 0; i < mid; i++) { I_set_key(p, i, tkeys[i]); I_set_ch(p, i+1, tchs[i+1]); }

    /* build right half */
    I_init(rp_out);
    PHdr *rh = I_hdr(rp_out);
    int rn = n - mid;
    rh->n = rn;
    I_set_ch(rp_out, 0, tchs[mid+1]);
    for (int i = 0; i < rn; i++) {
        I_set_key(rp_out, i, tkeys[mid+1+i]);
        I_set_ch(rp_out, i+1, tchs[mid+2+i]);
    }
    *mk = tkeys[mid];
    return 1;
}

/* ── Recursive descent insert ── */
static int do_insert(BTree *t, int32_t pgno, int64_t key, int64_t off,
                     int64_t *mk, int32_t *rnew) {
    Page p; rd(t->fd, pgno, p);
    PHdr *h = (PHdr *)p;

    if (h->type == PT_LEAF)
        return leaf_ins(t, pgno, key, off, mk, rnew);

    /* internal: pick child */
    int n = h->n, ci = 0;
    while (ci < n && key >= I_key(p, ci)) ci++;
    int32_t child = I_ch(p, ci);

    int64_t cmk; int32_t crnew;
    int split = do_insert(t, child, key, off, &cmk, &crnew);
    if (!split) return 0;

    /* child split — insert (cmk, crnew) into this node */
    rd(t->fd, pgno, p);          /* re-read (safe: child writes don't touch us) */
    Page rp;
    int me_split = int_ins(p, cmk, crnew, mk, rp);
    wr(t->fd, pgno, p);
    if (!me_split) return 0;

    *rnew = alloc_pg(t);
    wr(t->fd, *rnew, rp);
    return 1;
}

/* ── Public: insert ── */
int btree_insert(BTree *t, int64_t key, int64_t off) {
    if (!t) return -1;
    int64_t mk; int32_t rnew;
    int split = do_insert(t, t->root, key, off, &mk, &rnew);

    if (split) {
        /* grow a new root */
        int32_t new_root = alloc_pg(t);
        Page p; I_init(p);
        PHdr *h = I_hdr(p); h->n = 1;
        I_set_ch(p, 0, t->root);
        I_set_key(p, 0, mk);
        I_set_ch(p, 1, rnew);
        wr(t->fd, new_root, p);
        t->root = new_root;
    }
    t->nkeys++;
    flush_hdr(t);
    return 0;
}

/* ── Find leftmost leaf where max_key >= target ── */
static int32_t find_leaf_ge(BTree *t, int64_t target) {
    int32_t cur = t->root;
    Page p;
    for (;;) {
        rd(t->fd, cur, p);
        PHdr *h = (PHdr *)p;
        if (h->type == PT_LEAF) return cur;
        int n = h->n, ci = 0;
        while (ci < n && target > I_key(p, ci)) ci++;
        cur = I_ch(p, ci);
    }
}

/* ── Public: range scan [lo, hi] ── */
int btree_range(BTree *t, int64_t lo, int64_t hi,
                int64_t *offs, int *n_out) {
    *n_out = 0;
    if (!t || t->nkeys == 0) return 0;

    int32_t lpg = find_leaf_ge(t, lo);
    Page p;

    while (lpg >= 0 && lpg < t->npages && *n_out < BT_MAX_OFFSETS) {
        rd(t->fd, lpg, p);
        PHdr *h = L_hdr(p);
        int n = h->n;
        for (int i = 0; i < n && *n_out < BT_MAX_OFFSETS; i++) {
            LE *e = L_ent(p, i);
            if (e->key > hi) { lpg = -1; goto done; }
            if (e->key >= lo) offs[(*n_out)++] = e->off;
        }
        lpg = h->next;
    }
done:
    return 0;
}

/* ── Public: point lookup ── */
int btree_lookup(BTree *t, int64_t key, int64_t *offs, int *n_out) {
    return btree_range(t, key, key, offs, n_out);
}

/* ── Public: create (new file) ── */
BTree *btree_create(const char *path) {
    int fd = open(path, O_RDWR|O_CREAT|O_TRUNC|O_CLOEXEC, 0644);
    if (fd < 0) { LOG_ERROR("btree_create %s: %s", path, strerror(errno)); return NULL; }

    BTree *t = calloc(1, sizeof(BTree));
    t->fd = fd; t->root = 1; t->npages = 1; t->nkeys = 0;

    /* page 0 = header */
    Page blank; memset(blank, 0, BT_PAGE_SIZE);
    wr(fd, 0, blank);

    /* page 1 = initial root leaf */
    Page root; L_init(root);
    wr(fd, 1, root);
    t->npages = 2;

    flush_hdr(t);
    return t;
}

/* ── Public: open (existing file) ── */
BTree *btree_open(const char *path) {
    int fd = open(path, O_RDWR|O_CLOEXEC);
    if (fd < 0) return NULL;

    Page p;
    if (pread(fd, p, BT_PAGE_SIZE, 0) != BT_PAGE_SIZE) { close(fd); return NULL; }
    FHdr *h = (FHdr *)p;
    if (h->magic != MAGIC) { close(fd); return NULL; }

    BTree *t = calloc(1, sizeof(BTree));
    t->fd = fd; t->root = h->root; t->npages = h->npages; t->nkeys = h->nkeys;
    return t;
}

/* ── Public: close ── */
void btree_close(BTree *t) {
    if (!t) return;
    flush_hdr(t);
    close(t->fd);
    free(t);
}

int64_t btree_count(BTree *t) { return t ? t->nkeys : 0; }
