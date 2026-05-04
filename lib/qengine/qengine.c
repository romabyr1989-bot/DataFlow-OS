#include "qengine.h"
#include "../core/log.h"
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <math.h>

/* ── Expression evaluator ── */
static bool bit_is_null(uint8_t *bm, int i) {
    return bm && !!(bm[i/8] & (1u << (i%8)));
}

Scalar eval_expr(Expr *e, EvalCtx *ctx, Arena *a) {
    Scalar s = {SV_NULL};
    if (!e) return s;
    switch (e->type) {
    case EXPR_LITERAL_INT:   s.type=SV_INT;    s.val.ival=e->ival;  return s;
    case EXPR_LITERAL_FLOAT: s.type=SV_DOUBLE; s.val.fval=e->fval;  return s;
    case EXPR_LITERAL_STR:   s.type=SV_TEXT;   s.val.sval=e->sval;  return s;
    case EXPR_LITERAL_BOOL:  s.type=SV_BOOL;   s.val.bval=e->bval;  return s;
    case EXPR_LITERAL_NULL:  return s;
    case EXPR_ALIAS:         return eval_expr(e->expr, ctx, a);
    case EXPR_COL: {
        if (!ctx || !ctx->schema || !ctx->batch) return s;
        for (int c = 0; c < ctx->schema->ncols; c++) {
            if (strcmp(ctx->schema->cols[c].name, e->name) != 0) continue;
            if (bit_is_null(ctx->batch->null_bitmap[c], ctx->row)) return s;
            switch (ctx->schema->cols[c].type) {
            case COL_INT64:  s.type=SV_INT;    s.val.ival=((int64_t*)ctx->batch->values[c])[ctx->row]; break;
            case COL_DOUBLE: s.type=SV_DOUBLE; s.val.fval=((double*)ctx->batch->values[c])[ctx->row]; break;
            case COL_TEXT:   s.type=SV_TEXT;   s.val.sval=((char**)ctx->batch->values[c])[ctx->row]; break;
            case COL_BOOL:   s.type=SV_BOOL;   s.val.bval=((int*)ctx->batch->values[c])[ctx->row]!=0; break;
            default: break;
            }
            return s;
        }
        return s;
    }
    case EXPR_FUNC: {
        /* aggregate functions return 0/null in scalar context */
        const char *fn = e->func_name;
        if (!strcasecmp(fn,"count")) { s.type=SV_INT; s.val.ival=0; return s; }
        if (!strcasecmp(fn,"now"))   { s.type=SV_INT; s.val.ival=(int64_t)time(NULL); return s; }
        if (e->nargs > 0) {
            Scalar arg0 = eval_expr(e->args[0], ctx, a);
            if (!strcasecmp(fn,"abs")) {
                if (arg0.type==SV_INT)    { s=arg0; if(s.val.ival<0) s.val.ival=-s.val.ival; return s; }
                if (arg0.type==SV_DOUBLE) { s=arg0; s.val.fval=fabs(s.val.fval); return s; }
            }
            if (!strcasecmp(fn,"upper") && arg0.type==SV_TEXT) {
                char *out=arena_strdup(a,arg0.val.sval);
                for(char*p=out;*p;p++) if(*p>='a'&&*p<='z') *p-=32;
                s.type=SV_TEXT; s.val.sval=out; return s;
            }
            if (!strcasecmp(fn,"lower") && arg0.type==SV_TEXT) {
                char *out=arena_strdup(a,arg0.val.sval);
                for(char*p=out;*p;p++) if(*p>='A'&&*p<='Z') *p+=32;
                s.type=SV_TEXT; s.val.sval=out; return s;
            }
            if (!strcasecmp(fn,"length") && arg0.type==SV_TEXT) {
                s.type=SV_INT; s.val.ival=(int64_t)strlen(arg0.val.sval); return s;
            }
        }
        return s;
    }
    case EXPR_BINOP: {
        Scalar L = eval_expr(e->left, ctx, a);
        /* short-circuit for AND/OR */
        if (e->op == OP_AND) {
            bool lb = (L.type==SV_BOOL)?L.val.bval:(L.type!=SV_NULL);
            if (!lb) { s.type=SV_BOOL; s.val.bval=false; return s; }
            Scalar R = eval_expr(e->right, ctx, a);
            s.type=SV_BOOL; s.val.bval=(R.type==SV_BOOL)?R.val.bval:(R.type!=SV_NULL); return s;
        }
        if (e->op == OP_OR) {
            bool lb = (L.type==SV_BOOL)?L.val.bval:(L.type!=SV_NULL);
            if (lb) { s.type=SV_BOOL; s.val.bval=true; return s; }
            Scalar R = eval_expr(e->right, ctx, a);
            s.type=SV_BOOL; s.val.bval=(R.type==SV_BOOL)?R.val.bval:(R.type!=SV_NULL); return s;
        }
        Scalar R = eval_expr(e->right, ctx, a);
        /* numeric coercion */
        double lv=0, rv=0; bool numeric=false;
        if ((L.type==SV_INT||L.type==SV_DOUBLE)&&(R.type==SV_INT||R.type==SV_DOUBLE)) {
            lv = L.type==SV_INT ? (double)L.val.ival : L.val.fval;
            rv = R.type==SV_INT ? (double)R.val.ival : R.val.fval;
            numeric = true;
        }
        switch (e->op) {
        case OP_EQ:
            if (L.type==SV_TEXT&&R.type==SV_TEXT) { s.type=SV_BOOL; s.val.bval=!strcmp(L.val.sval,R.val.sval); return s; }
            s.type=SV_BOOL; s.val.bval=numeric&&lv==rv; return s;
        case OP_NE:
            if (L.type==SV_TEXT&&R.type==SV_TEXT) { s.type=SV_BOOL; s.val.bval=!!strcmp(L.val.sval,R.val.sval); return s; }
            s.type=SV_BOOL; s.val.bval=numeric&&lv!=rv; return s;
        case OP_LT: s.type=SV_BOOL; s.val.bval=numeric&&lv< rv; return s;
        case OP_LE: s.type=SV_BOOL; s.val.bval=numeric&&lv<=rv; return s;
        case OP_GT: s.type=SV_BOOL; s.val.bval=numeric&&lv> rv; return s;
        case OP_GE: s.type=SV_BOOL; s.val.bval=numeric&&lv>=rv; return s;
        case OP_ADD:
            if (L.type==SV_INT&&R.type==SV_INT) { s.type=SV_INT; s.val.ival=L.val.ival+R.val.ival; return s; }
            s.type=SV_DOUBLE; s.val.fval=lv+rv; return s;
        case OP_SUB:
            if (L.type==SV_INT&&R.type==SV_INT) { s.type=SV_INT; s.val.ival=L.val.ival-R.val.ival; return s; }
            s.type=SV_DOUBLE; s.val.fval=lv-rv; return s;
        case OP_MUL:
            if (L.type==SV_INT&&R.type==SV_INT) { s.type=SV_INT; s.val.ival=L.val.ival*R.val.ival; return s; }
            s.type=SV_DOUBLE; s.val.fval=lv*rv; return s;
        case OP_DIV:
            s.type=SV_DOUBLE; s.val.fval=rv!=0?lv/rv:0; return s;
        case OP_BETWEEN: {
            /* right is packed as AND(low,high) */
            Scalar lo=eval_expr(e->right->left,ctx,a);
            Scalar hi=eval_expr(e->right->right,ctx,a);
            double loval=lo.type==SV_INT?(double)lo.val.ival:lo.val.fval;
            double hival=hi.type==SV_INT?(double)hi.val.ival:hi.val.fval;
            s.type=SV_BOOL; s.val.bval=numeric&&lv>=loval&&lv<=hival; return s;
        }
        case OP_LIKE: {
            if (L.type!=SV_TEXT||R.type!=SV_TEXT) { s.type=SV_BOOL; s.val.bval=false; return s; }
            /* simple % wildcard matching */
            const char *str=L.val.sval, *pat=R.val.sval;
            /* naive but correct for basic patterns */
            const char *star=strchr(pat,'%');
            bool match=false;
            if (!star) match=!strcmp(str,pat);
            else if (star==pat) match=!!strstr(str,pat+1);
            else {
                size_t pre=(size_t)(star-pat);
                match=!strncmp(str,pat,pre)&&!!strstr(str+pre,star+1);
            }
            s.type=SV_BOOL; s.val.bval=match; return s;
        }
        default: break;
        }
        return s;
    }
    case EXPR_UNOP: {
        if (e->op==OP_IS_NULL) {
            Scalar inner=eval_expr(e->left,ctx,a);
            s.type=SV_BOOL; s.val.bval=(inner.type==SV_NULL); return s;
        }
        if (e->op==OP_IS_NOT_NULL) {
            Scalar inner=eval_expr(e->left,ctx,a);
            s.type=SV_BOOL; s.val.bval=(inner.type!=SV_NULL); return s;
        }
        if (e->op==OP_NOT) {
            Scalar inner=eval_expr(e->left,ctx,a);
            s.type=SV_BOOL; s.val.bval=!(inner.type==SV_BOOL?inner.val.bval:(inner.type!=SV_NULL)); return s;
        }
        if (e->op==OP_SUB) {
            Scalar inner=eval_expr(e->left,ctx,a);
            if(inner.type==SV_INT){s=inner;s.val.ival=-s.val.ival;return s;}
            if(inner.type==SV_DOUBLE){s=inner;s.val.fval=-s.val.fval;return s;}
        }
        return s;
    }
    default: return s;
    }
}

bool eval_bool(Expr *e, EvalCtx *ctx, Arena *a) {
    Scalar s = eval_expr(e, ctx, a);
    if (s.type == SV_BOOL)   return s.val.bval;
    if (s.type == SV_NULL)   return false;
    if (s.type == SV_INT)    return s.val.ival != 0;
    if (s.type == SV_DOUBLE) return s.val.fval != 0.0;
    if (s.type == SV_TEXT)   return s.val.sval && *s.val.sval;
    return false;
}

/* ── Scan operator ── */
typedef struct {
    char   data_dir[512];
    char   table_name[128];
    FILE  *wal_file;
    int    done;
    char   delim;
    char **headers;
    int    ncols;
} ScanState;

static int scan_open(Operator *op) {
    ScanState *st = op->state;
    char path[700];
    snprintf(path, sizeof(path), "%s/%s/wal.bin", st->data_dir, st->table_name);
    st->wal_file = fopen(path, "r");
    st->done = (st->wal_file == NULL);
    return 0;
}

static int scan_next(Operator *op, ColBatch **out) {
    ScanState *st = op->state;
    if (st->done || !st->wal_file) return 1; /* EOF */
    Schema *schema = op->output_schema;
    if (!schema) return 1;
    int ncols = schema->ncols;
    ColBatch *batch = arena_calloc(op->arena, sizeof(ColBatch));
    batch->schema = schema; batch->ncols = ncols;
    int64_t **iv = arena_calloc(op->arena, ncols * sizeof(void*));
    double  **dv = arena_calloc(op->arena, ncols * sizeof(void*));
    char   ***sv = arena_calloc(op->arena, ncols * sizeof(void*));
    for (int c = 0; c < ncols; c++) {
        switch (schema->cols[c].type) {
        case COL_INT64:  iv[c]=arena_alloc(op->arena,BATCH_SIZE*sizeof(int64_t)); batch->values[c]=iv[c]; break;
        case COL_DOUBLE: dv[c]=arena_alloc(op->arena,BATCH_SIZE*sizeof(double));  batch->values[c]=dv[c]; break;
        default:         sv[c]=arena_alloc(op->arena,BATCH_SIZE*sizeof(char*));   batch->values[c]=sv[c]; break;
        }
        batch->null_bitmap[c]=arena_calloc(op->arena,(BATCH_SIZE+7)/8);
    }
    /* Read WAL TLV records */
    int row = 0;
    uint32_t rec_len;
    char line_buf[65536];
    while (row < BATCH_SIZE) {
        if (fread(&rec_len, 4, 1, st->wal_file) != 1) { st->done=1; break; }
        if (rec_len >= sizeof(line_buf)) { fseek(st->wal_file, rec_len, SEEK_CUR); continue; }
        if (fread(line_buf, 1, rec_len, st->wal_file) != rec_len) { st->done=1; break; }
        line_buf[rec_len] = '\0';
        /* parse comma-separated line */
        char *p = line_buf; int col = 0;
        while (col < ncols) {
            char *comma = strchr(p, ',');
            size_t vlen = comma ? (size_t)(comma - p) : strlen(p);
            /* strip \n */
            while (vlen > 0 && (p[vlen-1]=='\n'||p[vlen-1]=='\r')) vlen--;
            char tmp[4096]; if(vlen>=sizeof(tmp)) vlen=sizeof(tmp)-1;
            memcpy(tmp,p,vlen); tmp[vlen]='\0';
            if (strcmp(tmp,"NULL")==0) {
                batch->null_bitmap[col][row/8] |= (1u<<(row%8));
            } else {
                switch (schema->cols[col].type) {
                case COL_INT64:  iv[col][row]=strtoll(tmp,NULL,10); break;
                case COL_DOUBLE: dv[col][row]=strtod(tmp,NULL); break;
                case COL_BOOL:   ((int*)sv[col])[row]=(strcasecmp(tmp,"true")==0||strcmp(tmp,"1")==0); break;
                default:         sv[col][row]=arena_strdup(op->arena,tmp); break;
                }
            }
            p = comma ? comma+1 : p+strlen(p);
            col++;
        }
        row++;
    }
    batch->nrows = row;
    *out = batch;
    return row > 0 ? 0 : 1;
}

static void scan_close(Operator *op) {
    ScanState *st = op->state;
    if (st->wal_file) { fclose(st->wal_file); st->wal_file=NULL; }
}

static const OpVtable SCAN_VT = {scan_open, scan_next, scan_close};

Operator *op_scan(Arena *a, const char *table, Schema *schema, const char *data_dir) {
    Operator *op = arena_calloc(a, sizeof(Operator));
    op->vt = &SCAN_VT; op->arena = a; op->output_schema = schema;
    ScanState *st = arena_calloc(a, sizeof(ScanState));
    strncpy(st->table_name, table, sizeof(st->table_name)-1);
    strncpy(st->data_dir, data_dir, sizeof(st->data_dir)-1);
    op->state = st;
    return op;
}

/* ── Filter operator ── */
typedef struct { Expr *predicate; } FilterState;

static int filter_open(Operator *op) { return op->left->vt->open(op->left); }

static int filter_next(Operator *op, ColBatch **out) {
    FilterState *st = op->state;
    for (;;) {
        ColBatch *src = NULL;
        int rc = op->left->vt->next(op->left, &src);
        if (rc != 0) return rc;
        /* filter rows */
        ColBatch *dst = arena_calloc(op->arena, sizeof(ColBatch));
        dst->schema = src->schema; dst->ncols = src->ncols;
        /* allocate same layout */
        for (int c = 0; c < src->ncols; c++) {
            size_t esz = (src->schema->cols[c].type==COL_INT64)?sizeof(int64_t):(src->schema->cols[c].type==COL_DOUBLE)?sizeof(double):sizeof(char*);
            dst->values[c]      = arena_alloc(op->arena, (size_t)src->nrows * esz);
            dst->null_bitmap[c] = arena_calloc(op->arena, ((size_t)src->nrows+7)/8);
        }
        int out_row = 0;
        EvalCtx ctx = {src->schema, src, 0};
        for (int r = 0; r < src->nrows; r++) {
            ctx.row = r;
            if (!eval_bool(st->predicate, &ctx, op->arena)) continue;
            /* copy row */
            for (int c = 0; c < src->ncols; c++) {
                bool is_null = src->null_bitmap[c] && !!(src->null_bitmap[c][r/8]&(1u<<(r%8)));
                if (is_null) { dst->null_bitmap[c][out_row/8] |= 1u<<(out_row%8); continue; }
                switch (src->schema->cols[c].type) {
                case COL_INT64:  ((int64_t*)dst->values[c])[out_row]=((int64_t*)src->values[c])[r]; break;
                case COL_DOUBLE: ((double*)dst->values[c])[out_row]=((double*)src->values[c])[r]; break;
                default:         ((char**)dst->values[c])[out_row]=((char**)src->values[c])[r]; break;
                }
            }
            out_row++;
        }
        dst->nrows = out_row;
        *out = dst;
        if (out_row > 0) return 0;
        /* empty batch — try next batch from child */
    }
}

static void filter_close(Operator *op) { op->left->vt->close(op->left); }
static const OpVtable FILTER_VT = {filter_open, filter_next, filter_close};

Operator *op_filter(Arena *a, Operator *src, Expr *predicate) {
    Operator *op = arena_calloc(a, sizeof(Operator));
    op->vt=&FILTER_VT; op->left=src; op->arena=a;
    op->output_schema=src->output_schema;
    FilterState *st=arena_calloc(a,sizeof(FilterState)); st->predicate=predicate;
    op->state=st;
    return op;
}

/* ── Limit operator ── */
typedef struct { int64_t limit, offset, emitted, skipped; } LimitState;

static int limit_open(Operator *op) { return op->left->vt->open(op->left); }

static int limit_next(Operator *op, ColBatch **out) {
    LimitState *st = op->state;
    if (st->limit >= 0 && st->emitted >= st->limit) return 1;
    ColBatch *src=NULL;
    int rc = op->left->vt->next(op->left, &src);
    if (rc != 0) return rc;
    /* apply offset/limit */
    int start=0;
    if (st->skipped < st->offset) {
        int64_t skip = st->offset - st->skipped;
        if (skip >= src->nrows) { st->skipped+=src->nrows; *out=src; src->nrows=0; return 0; }
        start=(int)skip; st->skipped=st->offset;
    }
    int avail=src->nrows-start;
    if (st->limit>=0&&st->emitted+avail>st->limit) avail=(int)(st->limit-st->emitted);
    /* create sub-batch */
    ColBatch *dst=arena_calloc(op->arena,sizeof(ColBatch));
    dst->schema=src->schema; dst->ncols=src->ncols;
    for(int c=0;c<src->ncols;c++){
        size_t esz=(src->schema->cols[c].type==COL_INT64)?sizeof(int64_t):(src->schema->cols[c].type==COL_DOUBLE)?sizeof(double):sizeof(char*);
        dst->values[c]=arena_alloc(op->arena,(size_t)avail*esz+(size_t)1);
        dst->null_bitmap[c]=arena_calloc(op->arena,((size_t)avail+7)/8);
        for(int r=0;r<avail;r++){
            int sr=start+r;
            bool is_null=src->null_bitmap[c]&&!!(src->null_bitmap[c][sr/8]&(1u<<(sr%8)));
            if(is_null){dst->null_bitmap[c][r/8]|=1u<<(r%8);continue;}
            switch(src->schema->cols[c].type){
            case COL_INT64:  ((int64_t*)dst->values[c])[r]=((int64_t*)src->values[c])[sr]; break;
            case COL_DOUBLE: ((double*)dst->values[c])[r]=((double*)src->values[c])[sr]; break;
            default:         ((char**)dst->values[c])[r]=((char**)src->values[c])[sr]; break;
            }
        }
    }
    dst->nrows=avail; st->emitted+=avail;
    *out=dst; return 0;
}

static void limit_close(Operator *op) { op->left->vt->close(op->left); }
static const OpVtable LIMIT_VT={limit_open,limit_next,limit_close};

Operator *op_limit(Arena *a, Operator *src, int64_t limit, int64_t offset) {
    Operator *op=arena_calloc(a,sizeof(Operator));
    op->vt=&LIMIT_VT; op->left=src; op->arena=a; op->output_schema=src->output_schema;
    LimitState *st=arena_calloc(a,sizeof(LimitState));
    st->limit=limit; st->offset=offset;
    op->state=st; return op;
}

/* ── Build from logical plan ── */
Operator *qengine_build(Arena *a, PlanNode *plan, const char *data_dir) {
    if (!plan) return NULL;
    switch (plan->type) {
    case PLAN_SCAN: {
        /* schema would be fetched from catalog in full impl */
        return op_scan(a, plan->table, NULL, data_dir);
    }
    case PLAN_FILTER: {
        Operator *src=qengine_build(a,plan->left,data_dir);
        return op_filter(a,src,plan->predicate);
    }
    case PLAN_LIMIT: {
        Operator *src=qengine_build(a,plan->left,data_dir);
        return op_limit(a,src,plan->limit,plan->offset);
    }
    case PLAN_PROJECT:
    case PLAN_SORT:
    case PLAN_AGG:
    case PLAN_JOIN:
        /* delegate to left for now */
        return qengine_build(a, plan->left, data_dir);
    default:
        return NULL;
    }
}

/* ── Execute to JSON rows ── */
static void scalar_to_json(Scalar *s, JBuf *jb) {
    switch (s->type) {
    case SV_NULL:   jb_null(jb); break;
    case SV_INT:    jb_int(jb, s->val.ival); break;
    case SV_DOUBLE: jb_double(jb, s->val.fval); break;
    case SV_TEXT:   jb_str(jb, s->val.sval); break;
    case SV_BOOL:   jb_bool(jb, s->val.bval); break;
    }
}

int qengine_exec_json(Operator *root, Arena *a, RowJsonCb cb, void *ud, int *rows_out) {
    if (!root) { if(rows_out)*rows_out=0; return 0; }
    root->vt->open(root);
    int total=0;
    ColBatch *batch=NULL;
    while (root->vt->next(root,&batch)==0 && batch && batch->nrows>0) {
        Schema *schema=batch->schema;
        for (int r=0;r<batch->nrows;r++){
            JBuf jb; jb_init(&jb,a,256);
            jb_obj_begin(&jb);
            if (schema) {
                for (int c=0;c<batch->ncols;c++){
                    jb_key(&jb,schema->cols[c].name);
                    bool is_null=batch->null_bitmap[c]&&!!(batch->null_bitmap[c][r/8]&(1u<<(r%8)));
                    if(is_null){jb_null(&jb);continue;}
                    Scalar s={SV_NULL};
                    switch(schema->cols[c].type){
                    case COL_INT64:  s.type=SV_INT; s.val.ival=((int64_t*)batch->values[c])[r]; break;
                    case COL_DOUBLE: s.type=SV_DOUBLE; s.val.fval=((double*)batch->values[c])[r]; break;
                    case COL_TEXT:   s.type=SV_TEXT; s.val.sval=((char**)batch->values[c])[r]; break;
                    case COL_BOOL:   s.type=SV_BOOL; s.val.bval=((int*)batch->values[c])[r]!=0; break;
                    default: break;
                    }
                    scalar_to_json(&s,&jb);
                }
            }
            jb_obj_end(&jb);
            if(cb) cb(jb_done(&jb),ud);
            total++;
        }
        batch=NULL;
    }
    root->vt->close(root);
    if(rows_out)*rows_out=total;
    return 0;
}
