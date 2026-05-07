#include "app.h"
#include "../../lib/core/json.h"
#include "../../lib/core/log.h"
#include "../../lib/sql_parser/sql.h"
#include "../../lib/storage/storage.h"
#include "../../lib/connector/connector.h"
#include "../../lib/auth/auth.h"
#include <sqlite3.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <math.h>

/* ── Table name validation ── */
static bool valid_table_name(const char *s) {
    if (!s || !*s) return false;
    for (const char *p = s; *p; p++) {
        char c = *p;
        if (!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='_')) return false;
    }
    return strlen(s) < 128;
}

/* ── Static file serving ── */
static void h_static_file(HttpReq *req, HttpResp *resp,
                          const char *path, const char *ct) {
    (void)req;
    FILE *f = fopen(path, "rb");
    if (!f) { http_resp_error(resp, 404, "file not found"); return; }
    fseek(f, 0, SEEK_END); long sz = ftell(f); rewind(f);
    char *buf = arena_alloc(req->arena, (size_t)sz + 1);
    if (!buf || fread(buf, 1, (size_t)sz, f) != (size_t)sz) {
        fclose(f); http_resp_error(resp, 500, "read error"); return;
    }
    fclose(f);
    buf[sz] = '\0';
    resp->status = 200; resp->content_type = ct;
    resp->body = buf; resp->body_len = (size_t)sz;
}
static void h_ui_html(HttpReq *r, HttpResp *resp) { h_static_file(r, resp, "ui/index.html", "text/html"); }
static void h_ui_css (HttpReq *r, HttpResp *resp) { h_static_file(r, resp, "ui/style.css",  "text/css"); }
static void h_ui_js  (HttpReq *r, HttpResp *resp) { h_static_file(r, resp, "ui/app.js",     "application/javascript"); }

/* ── CSV helpers ── */
static char detect_delim(const char *line, size_t n) {
    int commas = 0, semis = 0;
    for (size_t i = 0; i < n; i++) {
        if (line[i] == ',') commas++;
        else if (line[i] == ';') semis++;
    }
    return (semis > commas) ? ';' : ',';
}

static void split_line_simple(char *line, char delim, char **out, int max_cols, int *nout) {
    int n = 0; char *p = line; out[n++] = p;
    while (*p && n < max_cols) {
        if (*p == delim) { *p = '\0'; out[n++] = p + 1; }
        p++;
    }
    *nout = n;
}

/* ═══════════════════════════════════════════════════════
   QUERY ENGINE — full SQL executor
   ═══════════════════════════════════════════════════════ */

#define MAX_JOIN_TABLES 32
#define MAX_RS_ROWS     100000

/* ── Scalar value ── */
typedef struct {
    bool        is_null;
    bool        is_bool;
    bool        is_num;
    bool        b;
    double      num;
    const char *str;
} Val;

static Val vnull(void)               { Val v={0}; v.is_null=true; return v; }
static Val vbool(bool b)             { Val v={0}; v.is_bool=true; v.b=b; return v; }
static Val vnum(double n)            { Val v={0}; v.is_num=true; v.num=n; return v; }
static Val vstr_s(const char *s)     { Val v={0}; v.str=s; return v; }  /* no alloc */

static bool vt(Val v) {  /* truthy */
    if (v.is_null) return false;
    if (v.is_bool) return v.b;
    if (v.is_num)  return v.num != 0.0;
    return v.str && *v.str;
}

static bool pnum(const char *s, double *o) {
    if (!s || !*s) return false;
    const char *p = s;
    while (*p == ' ' || *p == '\t') p++;
    if (!*p) return false;
    char *end = NULL;
    double val = strtod(p, &end);
    if (!end || end == p) return false;
    while (*end == ' ' || *end == '\t') end++;
    if (*end) return false;
    if (o) *o = val;
    return true;
}

static const char *val_str(Val v, Arena *a) {
    if (v.is_null)  return "";
    if (v.str)      return v.str;
    if (v.is_bool)  return v.b ? "true" : "false";
    if (v.is_num) {
        double n = v.num;
        if (n == (int64_t)n && fabs(n) < 1e15)
            return arena_sprintf(a, "%lld", (long long)(int64_t)n);
        return arena_sprintf(a, "%.10g", n);
    }
    return "";
}

static int vcmp(Val a, Val b) {
    if (a.is_null && b.is_null) return 0;
    if (a.is_null) return -1;
    if (b.is_null) return  1;
    if (a.is_num && b.is_num) {
        if (a.num < b.num) return -1;
        if (a.num > b.num) return  1;
        return 0;
    }
    /* try numeric comparison if both look like numbers */
    double na, nb;
    if (pnum(a.str, &na) && pnum(b.str, &nb)) {
        if (na < nb) return -1;
        if (na > nb) return  1;
        return 0;
    }
    const char *sa = a.str ? a.str : (a.is_bool ? (a.b?"true":"false") : "");
    const char *sb = b.str ? b.str : (b.is_bool ? (b.b?"true":"false") : "");
    return strcmp(sa, sb);
}

static bool veq(Val a, Val b) {
    if (a.is_null || b.is_null) return false;
    if (a.is_num && b.is_num)   return a.num == b.num;
    double na, nb;
    if (a.str && b.str) {
        if (!strcmp(a.str, b.str)) return true;
        if (pnum(a.str,&na) && pnum(b.str,&nb)) return na == nb;
        return false;
    }
    if (a.is_bool && b.is_bool) return a.b == b.b;
    return false;
}

/* ── LIKE matching ── */
static bool like_r(const char *s, const char *p) {
    if (!*p) return !*s;
    if (*p == '%') {
        while (*p == '%') p++;
        if (!*p) return true;
        for (; *s; s++) if (like_r(s, p)) return true;
        return like_r(s, p);
    }
    if (!*s) return false;
    if (*p == '_' || *s == *p) return like_r(s+1, p+1);
    return false;
}

static bool like_match(const char *str, const char *pat, bool icase, Arena *a) {
    if (!str || !pat) return false;
    if (!icase) return like_r(str, pat);
    /* case-insensitive: lower both */
    char *ls = arena_strdup(a, str);
    char *lp = arena_strdup(a, pat);
    for (char *p=ls; *p; p++) if(*p>='A'&&*p<='Z') *p+=32;
    for (char *p=lp; *p; p++) if(*p>='A'&&*p<='Z') *p+=32;
    return like_r(ls, lp);
}

/* ── Join context — N rows from N tables ── */
typedef struct {
    int         n;
    Schema     *schemas[MAX_JOIN_TABLES];
    char      **rows[MAX_JOIN_TABLES];   /* NULL = outer-join null row */
    const char *tnames[MAX_JOIN_TABLES];
    const char *aliases[MAX_JOIN_TABLES];
} JoinCtx;

static const char *jctx_col(JoinCtx *ctx, const char *tbl, const char *col) {
    if (!ctx || !col) return NULL;
    for (int t = 0; t < ctx->n; t++) {
        if (!ctx->rows[t] || !ctx->schemas[t]) continue;
        if (tbl && *tbl) {
            bool match = (ctx->tnames[t]  && !strcasecmp(tbl, ctx->tnames[t])) ||
                         (ctx->aliases[t] && !strcasecmp(tbl, ctx->aliases[t]));
            if (!match) continue;
        }
        Schema *sc = ctx->schemas[t];
        for (int c = 0; c < sc->ncols; c++) {
            if (!strcasecmp(sc->cols[c].name, col))
                return ctx->rows[t][c];
        }
    }
    return NULL;
}

/* ── ResultSet ── */
typedef struct {
    char  **cells;   /* ncols strings */
    char  **skeys;   /* nskeys sort-key strings (pre-evaluated) */
} OutRow;

typedef struct {
    OutRow     *rows;
    int         nrows, cap;
    char      **col_names;
    int         ncols;
    int         nskeys;
} RS;

static RS *rs_new(Arena *a, int ncols, char **col_names, int nskeys) {
    RS *rs = arena_calloc(a, sizeof(RS));
    rs->ncols = ncols;
    rs->nskeys = nskeys;
    rs->col_names = col_names;
    rs->cap = 64;
    rs->rows = arena_alloc(a, (size_t)rs->cap * sizeof(OutRow));
    return rs;
}

static void rs_add(RS *rs, Arena *a, char **cells, char **skeys) {
    if (rs->nrows >= MAX_RS_ROWS) return;
    if (rs->nrows == rs->cap) {
        int nc = rs->cap * 2;
        OutRow *nb = arena_alloc(a, (size_t)nc * sizeof(OutRow));
        memcpy(nb, rs->rows, (size_t)rs->nrows * sizeof(OutRow));
        rs->rows = nb; rs->cap = nc;
    }
    rs->rows[rs->nrows].cells = cells;
    rs->rows[rs->nrows].skeys = skeys;
    rs->nrows++;
}

/* ── Table data ── */
typedef struct {
    Schema     *schema;
    char     ***rows;   /* rows[i] is char*[ncols] */
    int         nrows;
    const char *tname;
    const char *alias;
} TblData;

/* ── Virtual table registry (CTEs + derived tables) ── */
typedef struct { const char *name; RS *rs; Schema *schema; } VTEntry;
typedef struct { VTEntry e[64]; int n; } VTReg;

static void vt_add(VTReg *vt, const char *name, RS *rs, Schema *schema) {
    if (vt->n < 64) { vt->e[vt->n].name=name; vt->e[vt->n].rs=rs; vt->e[vt->n].schema=schema; vt->n++; }
}

static bool vt_get(VTReg *vt, const char *name, RS **rs_out, Schema **sc_out) {
    if (!vt) return false;
    for (int i=0;i<vt->n;i++) {
        if (!strcasecmp(vt->e[i].name, name)) {
            if (rs_out) *rs_out = vt->e[i].rs;
            if (sc_out) *sc_out = vt->e[i].schema;
            return true;
        }
    }
    return false;
}

/* forward decls */
static Val  eval_val(Expr *e, JoinCtx *ctx, Arena *a);
static RS  *exec_stmt(Arena *a, const Stmt *s, VTReg *vt);
static bool expr_has_agg(Expr *e);
static bool expr_has_window(Expr *e);
static Expr *find_first_agg(Expr *e);
static bool expr_find_agg_args(Expr *e, const char *fn, Expr **call_args, int nargs);
static void apply_windows(Arena *a, RS *rs, const SelectStmt *sel);

/* ── GrpAcc forward declaration for thread-local group context ── */
typedef struct GrpAcc_s {
    char   *key;
    double *sums, *mins, *maxs;
    int    *cnts;
    int     total_cnt;
    bool   *min_set, *max_set;
    char  **first_str;
} GrpAcc;

/* Thread-local group context: lets eval_val resolve agg funcs during group emission */
static __thread GrpAcc           *tl_grp = NULL;
static __thread const SelectStmt *tl_sel = NULL;
/* Thread-local outer join context: for correlated subqueries */
static __thread JoinCtx          *tl_outer_ctx = NULL;

/* Thread-local window function pre-computed values (set during apply_windows re-eval) */
typedef struct { Expr *win_expr; char **values; } WinValEntry;
static __thread WinValEntry *tl_win_vals    = NULL;
static __thread int          tl_nwin_vals   = 0;
static __thread int          tl_win_cur_row = -1;

/* ── Expression evaluator ── */

static bool is_agg_name(const char *fn) {
    return !strcasecmp(fn,"COUNT") || !strcasecmp(fn,"SUM") ||
           !strcasecmp(fn,"AVG")   || !strcasecmp(fn,"MIN") || !strcasecmp(fn,"MAX");
}

static Val eval_func(const char *fn, Expr **args, int nargs, JoinCtx *ctx, Arena *a) {
    /* scalar functions */
    if (!strcasecmp(fn,"coalesce") || !strcasecmp(fn,"ifnull") || !strcasecmp(fn,"nvl")) {
        for (int i=0;i<nargs;i++) { Val v=eval_val(args[i],ctx,a); if(!v.is_null) return v; }
        return vnull();
    }
    if (!strcasecmp(fn,"nullif") && nargs==2) {
        Val a0=eval_val(args[0],ctx,a), a1=eval_val(args[1],ctx,a);
        if (veq(a0,a1)) return vnull();
        return a0;
    }
    if (!strcasecmp(fn,"if") || !strcasecmp(fn,"iif")) {
        if (nargs < 2) return vnull();
        Val cond = eval_val(args[0],ctx,a);
        return eval_val(vt(cond)?args[1]:args[2<nargs?2:1], ctx, a);
    }
    if (!strcasecmp(fn,"greatest")) {
        Val best=vnull();
        for (int i=0;i<nargs;i++) { Val v=eval_val(args[i],ctx,a); if(best.is_null||vcmp(v,best)>0) best=v; }
        return best;
    }
    if (!strcasecmp(fn,"least")) {
        Val best=vnull();
        for (int i=0;i<nargs;i++) { Val v=eval_val(args[i],ctx,a); if(best.is_null||vcmp(v,best)<0) best=v; }
        return best;
    }
    if (nargs == 0 && !strcasecmp(fn,"now"))  return vnum((double)time(NULL));
    if (nargs == 0 && !strcasecmp(fn,"random")) return vnum((double)rand()/(double)RAND_MAX);
    if (nargs == 0 && !strcasecmp(fn,"pi"))   return vnum(3.14159265358979323846);
    if (nargs == 0 && !strcasecmp(fn,"current_timestamp")) return vnum((double)time(NULL));

    if (nargs >= 1) {
        Val a0 = eval_val(args[0],ctx,a);
        const char *s0 = val_str(a0, a);
        double n0 = a0.num;
        if (!a0.is_num) pnum(s0, &n0);

        if (!strcasecmp(fn,"abs")) {
            if (a0.is_null) return vnull();
            if (a0.is_num) return vnum(fabs(a0.num));
            double n; if (pnum(s0,&n)) return vnum(fabs(n)); return vnull();
        }
        if (!strcasecmp(fn,"ceil") || !strcasecmp(fn,"ceiling")) {
            if (a0.is_null) return vnull(); return vnum(ceil(n0));
        }
        if (!strcasecmp(fn,"floor")) { if (a0.is_null) return vnull(); return vnum(floor(n0)); }
        if (!strcasecmp(fn,"sqrt"))  { if (a0.is_null) return vnull(); return vnum(sqrt(n0 > 0 ? n0 : 0)); }
        if (!strcasecmp(fn,"exp"))   { if (a0.is_null) return vnull(); return vnum(exp(n0)); }
        if (!strcasecmp(fn,"ln")   || !strcasecmp(fn,"log"))  {
            if (a0.is_null) return vnull(); return vnum(n0 > 0 ? log(n0) : 0);
        }
        if (!strcasecmp(fn,"log10")) { if (a0.is_null) return vnull(); return vnum(n0 > 0 ? log10(n0) : 0); }
        if (!strcasecmp(fn,"sign"))  { if (a0.is_null) return vnull(); return vnum(n0>0?1:n0<0?-1:0); }
        if (!strcasecmp(fn,"upper") || !strcasecmp(fn,"ucase")) {
            if (a0.is_null) return vnull();
            char *out=arena_strdup(a,s0);
            for(char*p=out;*p;p++) if(*p>='a'&&*p<='z') *p-=32;
            return vstr_s(out);
        }
        if (!strcasecmp(fn,"lower") || !strcasecmp(fn,"lcase")) {
            if (a0.is_null) return vnull();
            char *out=arena_strdup(a,s0);
            for(char*p=out;*p;p++) if(*p>='A'&&*p<='Z') *p+=32;
            return vstr_s(out);
        }
        if (!strcasecmp(fn,"length") || !strcasecmp(fn,"len") || !strcasecmp(fn,"char_length")) {
            if (a0.is_null) return vnull(); return vnum((double)strlen(s0));
        }
        if (!strcasecmp(fn,"ltrim")) {
            if (a0.is_null) return vnull();
            while (*s0 == ' ' || *s0 == '\t') s0++;
            return vstr_s(arena_strdup(a, s0));
        }
        if (!strcasecmp(fn,"rtrim")) {
            if (a0.is_null) return vnull();
            char *out = arena_strdup(a,s0);
            size_t l = strlen(out);
            while (l>0 && (out[l-1]==' '||out[l-1]=='\t')) out[--l]='\0';
            return vstr_s(out);
        }
        if (!strcasecmp(fn,"trim")) {
            if (a0.is_null) return vnull();
            while (*s0==' '||*s0=='\t') s0++;
            char *out=arena_strdup(a,s0);
            size_t l=strlen(out);
            while(l>0&&(out[l-1]==' '||out[l-1]=='\t')) out[--l]='\0';
            return vstr_s(out);
        }
        if (!strcasecmp(fn,"reverse")) {
            if (a0.is_null) return vnull();
            size_t l=strlen(s0); char *out=arena_strdup(a,s0);
            for(size_t i=0;i<l/2;i++){char t=out[i];out[i]=out[l-1-i];out[l-1-i]=t;}
            return vstr_s(out);
        }
        if (!strcasecmp(fn,"md5") || !strcasecmp(fn,"sha1") || !strcasecmp(fn,"hash")) {
            /* stub: return the string */
            return a0;
        }
        if (!strcasecmp(fn,"to_char") || !strcasecmp(fn,"str") || !strcasecmp(fn,"cast_text")) {
            return vstr_s(val_str(a0, a));
        }
        if (!strcasecmp(fn,"to_number") || !strcasecmp(fn,"to_float")) {
            double n; if (pnum(s0,&n)) return vnum(n); return vnull();
        }
        if (!strcasecmp(fn,"to_int") || !strcasecmp(fn,"int") || !strcasecmp(fn,"integer")) {
            double n; if (pnum(s0,&n)) return vnum(floor(n)); return vnull();
        }
        if (!strcasecmp(fn,"bool") || !strcasecmp(fn,"to_bool")) {
            if (a0.is_null) return vnull();
            return vbool(!strcasecmp(s0,"true")||!strcmp(s0,"1"));
        }
        if (!strcasecmp(fn,"not")) { return vbool(!vt(a0)); }
        if (!strcasecmp(fn,"is_null") || !strcasecmp(fn,"isnull")) { return vbool(a0.is_null); }
        if (!strcasecmp(fn,"is_not_null") || !strcasecmp(fn,"notnull")) { return vbool(!a0.is_null); }

        if (nargs >= 2) {
            Val a1 = eval_val(args[1],ctx,a);
            const char *s1 = val_str(a1, a);
            double n1 = a1.num; if (!a1.is_num) pnum(s1, &n1);

            if (!strcasecmp(fn,"round")) {
                if (a0.is_null) return vnull();
                int dec = (int)n1;
                double factor = pow(10.0, (double)dec);
                return vnum(round(n0 * factor) / factor);
            }
            if (!strcasecmp(fn,"power") || !strcasecmp(fn,"pow")) {
                if (a0.is_null) return vnull(); return vnum(pow(n0,n1));
            }
            if (!strcasecmp(fn,"mod"))  { if (a0.is_null) return vnull(); return vnum(n1!=0?fmod(n0,n1):0); }
            if (!strcasecmp(fn,"left")) {
                if (a0.is_null) return vnull();
                int k=(int)n1; if(k<0) k=0;
                size_t sl=strlen(s0); if((size_t)k>sl) k=(int)sl;
                return vstr_s(arena_strndup(a,s0,(size_t)k));
            }
            if (!strcasecmp(fn,"right")) {
                if (a0.is_null) return vnull();
                int k=(int)n1; if(k<0) k=0;
                size_t sl=strlen(s0); if((size_t)k>sl) k=(int)sl;
                return vstr_s(arena_strdup(a, s0+sl-k));
            }
            if (!strcasecmp(fn,"repeat")) {
                if (a0.is_null) return vnull();
                int k=(int)n1; if(k<0) k=0; if(k>1000) k=1000;
                size_t sl=strlen(s0);
                char *out=arena_alloc(a, sl*(size_t)k+1);
                out[0]='\0';
                for(int i=0;i<k;i++) memcpy(out+sl*(size_t)i, s0, sl);
                out[sl*(size_t)k]='\0';
                return vstr_s(out);
            }
            if (!strcasecmp(fn,"lpad") || !strcasecmp(fn,"rpad")) {
                if (a0.is_null) return vnull();
                int k=(int)n1; if(k<0)k=0; if(k>10000)k=10000;
                const char *pad = (nargs>=3) ? val_str(eval_val(args[2],ctx,a),a) : " ";
                if(!pad||!*pad) pad=" ";
                size_t sl=strlen(s0), pl=strlen(pad);
                if((int)sl>=(int)k) return vstr_s(arena_strndup(a,s0,(size_t)k));
                int need=k-(int)sl;
                char *out=arena_alloc(a,(size_t)k+1);
                if(!strcasecmp(fn,"lpad")){
                    int pos=0;
                    while(pos<need){int take=(int)pl<need-pos?(int)pl:need-pos;memcpy(out+pos,pad,(size_t)take);pos+=take;}
                    memcpy(out+need,s0,sl); out[k]='\0';
                } else {
                    memcpy(out,s0,sl);
                    int pos=(int)sl;
                    while(pos<k){int take=(int)pl<k-pos?(int)pl:k-pos;memcpy(out+pos,pad,(size_t)take);pos+=take;}
                    out[k]='\0';
                }
                return vstr_s(out);
            }
            if (!strcasecmp(fn,"instr") || !strcasecmp(fn,"strpos") || !strcasecmp(fn,"locate")) {
                if (a0.is_null || a1.is_null) return vnull();
                const char *pos = strstr(s0, s1);
                return vnum(pos ? (double)(pos-s0+1) : 0.0);
            }
            if (!strcasecmp(fn,"split_part")) {
                if (a0.is_null||a1.is_null) return vnull();
                int idx = (nargs>=3)?(int)eval_val(args[2],ctx,a).num:1;
                if(idx<1) return vnull();
                char *tmp=arena_strdup(a,s0);
                char *tok=strtok(tmp,s1); int i=1;
                while(tok&&i<idx){tok=strtok(NULL,s1);i++;}
                return tok?vstr_s(arena_strdup(a,tok)):vnull();
            }

            if (nargs >= 3) {
                Val a2 = eval_val(args[2],ctx,a);
                const char *s2 = val_str(a2, a);
                if (!strcasecmp(fn,"substr") || !strcasecmp(fn,"substring") || !strcasecmp(fn,"mid")) {
                    if (a0.is_null) return vnull();
                    int start=(int)n1-1; int len=(int)(a2.is_null?0:a2.num); /* 1-based */
                    if(start<0)start=0;
                    size_t sl=strlen(s0);
                    if((size_t)start>=sl) return vstr_s("");
                    if(len<0||(size_t)(start+len)>sl) len=(int)(sl-start);
                    return vstr_s(arena_strndup(a,s0+start,(size_t)len));
                }
                if (!strcasecmp(fn,"replace")) {
                    if (a0.is_null) return vnull();
                    if (!s1||!*s1) return a0;
                    size_t fl=strlen(s1), rl=strlen(s2);
                    /* count occurrences */
                    int cnt=0; const char *p=s0;
                    while((p=strstr(p,s1))){cnt++;p+=fl;}
                    size_t outsz=strlen(s0)+(size_t)cnt*(rl>fl?rl-fl:0)+1;
                    char *out=arena_alloc(a,outsz); char *op=out;
                    const char *src=s0;
                    while(*src){
                        const char *found=strstr(src,s1);
                        if(!found){size_t rem=strlen(src);memcpy(op,src,rem);op+=rem;break;}
                        memcpy(op,src,(size_t)(found-src)); op+=found-src;
                        memcpy(op,s2,rl); op+=rl; src=found+fl;
                    }
                    *op='\0';
                    return vstr_s(out);
                }
            }
        }

        /* cast */
        if (!strcasecmp(fn,"cast") && nargs==2) {
            Val a1=eval_val(args[1],ctx,a);
            const char *tname=a1.str?a1.str:"";
            if (!strncasecmp(tname,"int",3)||!strncasecmp(tname,"big",3)||!strncasecmp(tname,"sma",3)) {
                double n; if(pnum(s0,&n)) return vnum(floor(n)); return vnull();
            }
            if (!strncasecmp(tname,"float",5)||!strncasecmp(tname,"double",6)||!strncasecmp(tname,"num",3)||!strncasecmp(tname,"dec",3)||!strncasecmp(tname,"real",4)) {
                double n; if(pnum(s0,&n)) return vnum(n); return vnull();
            }
            if (!strncasecmp(tname,"bool",4)) {
                return vbool(!strcasecmp(s0,"true")||!strcmp(s0,"1"));
            }
            /* text/varchar/char: return as string */
            return vstr_s(s0);
        }
    }

    /* concat (variadic) */
    if (!strcasecmp(fn,"concat") || !strcasecmp(fn,"concat_ws")) {
        const char *sep="";
        int start=0;
        if (!strcasecmp(fn,"concat_ws") && nargs>0) {
            Val sv=eval_val(args[0],ctx,a);
            sep=val_str(sv,a); start=1;
        }
        size_t total=0;
        for(int i=start;i<nargs;i++){ Val v=eval_val(args[i],ctx,a); if(!v.is_null) total+=strlen(val_str(v,a)); }
        total += (nargs>start+1) ? (size_t)(nargs-start-1)*strlen(sep) : 0;
        char *out=arena_alloc(a,total+1); out[0]='\0';
        for(int i=start;i<nargs;i++){
            Val v=eval_val(args[i],ctx,a);
            if(v.is_null) continue;
            if(i>start && sep[0]) strcat(out,sep);
            strcat(out,val_str(v,a));
        }
        return vstr_s(out);
    }

    /* aggregate functions: return accumulated group value if inside group emission */
    if (is_agg_name(fn)) {
        if (tl_grp && tl_sel) {
            GrpAcc *g = tl_grp;
            const SelectStmt *sel = tl_sel;
            /* find matching select slot by expr pointer identity (handles ROUND(AVG(x)) etc.) */
            for (int i=0; i<sel->nselect; i++) {
                Expr *se=sel->select_list[i];
                Expr *sb=(se->type==EXPR_ALIAS)?se->expr:se;
                if (!expr_find_agg_args(sb, fn, args, nargs)) continue;
                if (!strcasecmp(fn,"COUNT")) return vnum((double)g->cnts[i]);
                if (!strcasecmp(fn,"SUM"))   return vnum(g->sums[i]);
                if (!strcasecmp(fn,"AVG"))   return vnum(g->cnts[i] ? g->sums[i]/g->cnts[i] : 0.0);
                if (!strcasecmp(fn,"MIN"))   return g->min_set[i] ? vnum(g->mins[i]) : vnull();
                if (!strcasecmp(fn,"MAX"))   return g->max_set[i] ? vnum(g->maxs[i]) : vnull();
            }
            /* fallback: total count for COUNT(*) */
            if (!strcasecmp(fn,"COUNT")) return vnum((double)g->total_cnt);
        }
        /* scalar context (no group) */
        if (!strcasecmp(fn,"COUNT")) return vnum(0);
        if (!strcasecmp(fn,"SUM") || !strcasecmp(fn,"AVG")) return vnum(0);
        if (!strcasecmp(fn,"MIN") || !strcasecmp(fn,"MAX")) return vnull();
    }

    /* exists: check if subquery returned rows; expose outer ctx for correlated queries */
    if (!strcasecmp(fn,"exists") && nargs==1) {
        Expr *sub_e = args[0];
        if (sub_e && sub_e->type == EXPR_SUBQUERY && sub_e->subq) {
            JoinCtx *prev_outer = tl_outer_ctx;
            if (ctx) tl_outer_ctx = ctx;
            RS *sub_rs = exec_stmt(a, sub_e->subq, NULL);
            tl_outer_ctx = prev_outer;
            return vbool(sub_rs && sub_rs->nrows > 0);
        }
    }

    return vnull();
}

static Val eval_val(Expr *e, JoinCtx *ctx, Arena *a) {
    if (!e) return vnull();
    switch (e->type) {
    case EXPR_LITERAL_INT:   return vnum((double)e->ival);
    case EXPR_LITERAL_FLOAT: return vnum(e->fval);
    case EXPR_LITERAL_STR:   return vstr_s(e->sval);
    case EXPR_LITERAL_BOOL:  return vbool(e->bval);
    case EXPR_LITERAL_NULL:  return vnull();
    case EXPR_ALIAS:         return eval_val(e->expr, ctx, a);
    case EXPR_STAR:          return vstr_s("*");
    case EXPR_COL: {
        if (!ctx) return vnull();
        const char *s = jctx_col(ctx, e->table, e->name);
        /* correlated subquery: fall back to outer context if not found */
        if (!s && tl_outer_ctx) s = jctx_col(tl_outer_ctx, e->table, e->name);
        if (!s) return vnull();
        Val v = vstr_s(s);
        double n;
        if (pnum(s, &n)) { v.is_num=true; v.num=n; }
        if (!strcasecmp(s,"true"))  { v.is_bool=true; v.b=true; }
        if (!strcasecmp(s,"false")) { v.is_bool=true; v.b=false; }
        return v;
    }
    case EXPR_FUNC:
        return eval_func(e->func_name, e->args, e->nargs, ctx, a);
    case EXPR_CASE: {
        if (e->case_op) {
            /* simple CASE x WHEN v THEN ... */
            Val x = eval_val(e->case_op, ctx, a);
            for (int i=0;i<e->nwhens;i++) {
                Val w = eval_val(e->whens[i], ctx, a);
                if (veq(x, w)) return eval_val(e->thens[i], ctx, a);
            }
        } else {
            /* searched CASE WHEN cond THEN ... */
            for (int i=0;i<e->nwhens;i++) {
                Val w = eval_val(e->whens[i], ctx, a);
                if (vt(w)) return eval_val(e->thens[i], ctx, a);
            }
        }
        if (e->else_expr) return eval_val(e->else_expr, ctx, a);
        return vnull();
    }
    case EXPR_SUBQUERY: {
        /* scalar correlated subquery */
        if (!e->subq) return vnull();
        JoinCtx *prev_outer = tl_outer_ctx;
        if (ctx) tl_outer_ctx = ctx;
        RS *sub = exec_stmt(a, e->subq, NULL);
        tl_outer_ctx = prev_outer;
        if (!sub || sub->nrows==0) return vnull();
        const char *v = sub->rows[0].cells ? sub->rows[0].cells[0] : NULL;
        if (!v) return vnull();
        Val rv = vstr_s(v);
        double n; if (pnum(v,&n)) { rv.is_num=true; rv.num=n; }
        return rv;
    }
    case EXPR_LIST: return vnull(); /* lists not evaluated directly */
    case EXPR_WINDOW:
        /* placeholder: actual value injected by apply_windows re-evaluation pass */
        if (tl_win_vals && tl_win_cur_row >= 0) {
            for (int _i = 0; _i < tl_nwin_vals; _i++) {
                if (tl_win_vals[_i].win_expr == e) {
                    const char *_v = tl_win_vals[_i].values[tl_win_cur_row];
                    if (!_v) return vnull();
                    Val _rv = vstr_s(_v);
                    double _n; if (pnum(_v, &_n)) { _rv.is_num=true; _rv.num=_n; }
                    return _rv;
                }
            }
        }
        return vnull();
    case EXPR_UNOP: {
        if (e->op == OP_IS_NULL) {
            Val inner = eval_val(e->left, ctx, a);
            return vbool(inner.is_null);
        }
        if (e->op == OP_IS_NOT_NULL) {
            Val inner = eval_val(e->left, ctx, a);
            return vbool(!inner.is_null);
        }
        if (e->op == OP_NOT) {
            Val inner = eval_val(e->left, ctx, a);
            return vbool(!vt(inner));
        }
        if (e->op == OP_SUB) {
            Val inner = eval_val(e->left, ctx, a);
            if (inner.is_num) return vnum(-inner.num);
            double n; if (pnum(inner.str,&n)) return vnum(-n);
        }
        return vnull();
    }
    case EXPR_BINOP: {
        /* short-circuit for AND/OR */
        if (e->op == OP_AND) {
            Val L = eval_val(e->left, ctx, a);
            if (!vt(L)) return vbool(false);
            Val R = eval_val(e->right, ctx, a);
            return vbool(vt(R));
        }
        if (e->op == OP_OR) {
            Val L = eval_val(e->left, ctx, a);
            if (vt(L)) return vbool(true);
            Val R = eval_val(e->right, ctx, a);
            return vbool(vt(R));
        }

        Val L = eval_val(e->left, ctx, a);

        /* IN / NOT IN */
        if (e->op == OP_IN || e->op == OP_NOT_IN) {
            bool found = false;
            Expr *rhs = e->right;
            if (rhs && rhs->type == EXPR_LIST) {
                for (int i=0;i<rhs->nitems;i++) {
                    Val iv = eval_val(rhs->items[i], ctx, a);
                    if (veq(L, iv)) { found=true; break; }
                }
            } else if (rhs && rhs->type == EXPR_SUBQUERY && rhs->subq) {
                JoinCtx *prev_outer = tl_outer_ctx;
                if (ctx) tl_outer_ctx = ctx;
                RS *sub = exec_stmt(a, rhs->subq, NULL);
                tl_outer_ctx = prev_outer;
                if (sub) {
                    for (int i=0;i<sub->nrows;i++) {
                        if (!sub->rows[i].cells) continue;
                        Val sv = vstr_s(sub->rows[i].cells[0]);
                        double n; if (pnum(sv.str,&n)) { sv.is_num=true; sv.num=n; }
                        if (veq(L, sv)) { found=true; break; }
                    }
                }
            }
            return vbool(e->op==OP_IN ? found : !found);
        }

        Val R = eval_val(e->right, ctx, a);
        double lv=0, rv=0;
        bool ln = L.is_num || pnum(L.str, &lv);
        bool rn = R.is_num || pnum(R.str, &rv);
        if (L.is_num) lv = L.num;
        if (R.is_num) rv = R.num;
        bool num_ok = ln && rn;

        switch (e->op) {
        case OP_EQ:  return vbool(veq(L,R));
        case OP_NE:  return vbool(!veq(L,R));
        case OP_LT:  return vbool(vcmp(L,R) <  0);
        case OP_LE:  return vbool(vcmp(L,R) <= 0);
        case OP_GT:  return vbool(vcmp(L,R) >  0);
        case OP_GE:  return vbool(vcmp(L,R) >= 0);
        case OP_ADD:
            if (!num_ok) {
                /* string concat fallback */
                const char *sl=val_str(L,a), *sr=val_str(R,a);
                size_t tl=strlen(sl)+strlen(sr)+1;
                char *out=arena_alloc(a,tl); strcpy(out,sl); strcat(out,sr);
                return vstr_s(out);
            }
            if (L.is_num&&R.is_num&&L.num==(int64_t)L.num&&R.num==(int64_t)R.num)
                return vnum(L.num+R.num);
            return vnum(lv+rv);
        case OP_SUB: return num_ok ? vnum(lv-rv) : vnull();
        case OP_MUL: return num_ok ? vnum(lv*rv) : vnull();
        case OP_DIV: return num_ok ? (rv!=0?vnum(lv/rv):vnull()) : vnull();
        case OP_MOD: return num_ok ? (rv!=0?vnum(fmod(lv,rv)):vnull()) : vnull();
        case OP_CONCAT: {
            const char *sl=val_str(L,a), *sr=val_str(R,a);
            size_t tl=strlen(sl)+strlen(sr)+1;
            char *out=arena_alloc(a,tl); strcpy(out,sl); strcat(out,sr);
            return vstr_s(out);
        }
        case OP_LIKE:     { if(L.is_null||R.is_null) return vbool(false); return vbool(like_match(val_str(L,a),val_str(R,a),false,a)); }
        case OP_ILIKE:    { if(L.is_null||R.is_null) return vbool(false); return vbool(like_match(val_str(L,a),val_str(R,a),true,a)); }
        case OP_NOT_LIKE: { if(L.is_null||R.is_null) return vbool(false); return vbool(!like_match(val_str(L,a),val_str(R,a),false,a)); }
        case OP_NOT_ILIKE:{ if(L.is_null||R.is_null) return vbool(false); return vbool(!like_match(val_str(L,a),val_str(R,a),true,a)); }
        case OP_BETWEEN: {
            if (L.is_null) return vbool(false);
            Val lo=eval_val(e->right->left,ctx,a);
            Val hi=eval_val(e->right->right,ctx,a);
            return vbool(vcmp(L,lo)>=0 && vcmp(L,hi)<=0);
        }
        case OP_NOT_BETWEEN: {
            if (L.is_null) return vbool(false);
            Val lo=eval_val(e->right->left,ctx,a);
            Val hi=eval_val(e->right->right,ctx,a);
            return vbool(vcmp(L,lo)<0 || vcmp(L,hi)>0);
        }
        default: return vnull();
        }
    }
    default: return vnull();
    }
}

/* ── Column name inference ── */
static const char *expr_name(Expr *e, int pos, Arena *a) {
    if (!e) return arena_sprintf(a,"col%d",pos+1);
    if (e->type == EXPR_ALIAS)   return e->alias;
    if (e->type == EXPR_COL)     return e->name;
    if (e->type == EXPR_FUNC)    return e->func_name;
    if (e->type == EXPR_WINDOW)  return e->func_name ? e->func_name : arena_sprintf(a,"col%d",pos+1);
    if (e->type == EXPR_STAR)  return "*";
    if (e->type == EXPR_LITERAL_INT)   return arena_sprintf(a,"%lld",(long long)e->ival);
    if (e->type == EXPR_LITERAL_FLOAT) return arena_sprintf(a,"%.10g",e->fval);
    if (e->type == EXPR_LITERAL_STR)   return e->sval;
    return arena_sprintf(a,"col%d",pos+1);
}

/* ── Index predicate hint ── */
typedef struct {
    const char *col;   /* column name to look up         */
    int64_t     lo;    /* range low  (inclusive)          */
    int64_t     hi;    /* range high (inclusive)          */
    bool        valid; /* true if this hint is usable     */
} IdxHint;

/* Walk WHERE AST for simple "col op int_literal" patterns */
static void extract_idx_hint_r(Expr *e, IdxHint *out) {
    if (!e || out->valid) return;
    if (e->type == EXPR_BINOP) {
        if (e->op == OP_AND) {
            extract_idx_hint_r(e->left, out);
            extract_idx_hint_r(e->right, out);
            return;
        }
        /* col = int */
        if (e->op == OP_EQ &&
            e->left  && e->left->type  == EXPR_COL &&
            e->right && e->right->type == EXPR_LITERAL_INT) {
            out->col = e->left->name;
            out->lo  = out->hi = e->right->ival;
            out->valid = true;
            return;
        }
        /* int = col (commuted) */
        if (e->op == OP_EQ &&
            e->left  && e->left->type  == EXPR_LITERAL_INT &&
            e->right && e->right->type == EXPR_COL) {
            out->col = e->right->name;
            out->lo  = out->hi = e->left->ival;
            out->valid = true;
            return;
        }
        /* col BETWEEN lo AND hi  (parser packs lo/hi as AND subtree) */
        if (e->op == OP_BETWEEN &&
            e->left  && e->left->type  == EXPR_COL  &&
            e->right && e->right->type == EXPR_BINOP &&
            e->right->left  && e->right->left->type  == EXPR_LITERAL_INT &&
            e->right->right && e->right->right->type == EXPR_LITERAL_INT) {
            out->col = e->left->name;
            out->lo  = e->right->left->ival;
            out->hi  = e->right->right->ival;
            out->valid = true;
        }
    }
}
static IdxHint extract_idx_hint(Expr *where) {
    IdxHint h = {0};
    extract_idx_hint_r(where, &h);
    return h;
}

/* Parse one WAL record from *wf at current position into a row array */
static char **parse_wal_row(Arena *a, FILE *wf, int ncols) {
    uint32_t l = 0;
    if (fread(&l, 4, 1, wf) != 1) return NULL;
    if (l == 0 || l > 262144) return NULL;
    char *line = arena_alloc(a, (size_t)l + 1);
    if (fread(line, 1, l, wf) != l) return NULL;
    line[l] = '\0';
    size_t rl = strlen(line);
    while (rl > 0 && (line[rl-1]=='\n'||line[rl-1]=='\r')) line[--rl]='\0';
    char *vals[MAX_COLS]={0}; int nv=0;
    split_line_simple(line, ',', vals, MAX_COLS, &nv);
    char **row = arena_alloc(a, (size_t)ncols * sizeof(char*));
    for (int i = 0; i < ncols; i++) row[i] = (i<nv&&vals[i]) ? vals[i] : "";
    return row;
}

/* ── Load table rows ── */
static int load_tbl(Arena *a, const char *tname, const char *alias,
                    VTReg *vt_reg, TblData *out, const IdxHint *ih) {
    memset(out, 0, sizeof(*out));
    out->tname = tname; out->alias = alias;

    /* Check virtual registry first (CTEs / derived tables) */
    RS *vrs = NULL; Schema *vsc = NULL;
    if (vt_reg && vt_get(vt_reg, tname, &vrs, &vsc) && vrs) {
        out->schema = vsc;
        out->nrows  = vrs->nrows;
        out->rows   = arena_alloc(a, (size_t)vrs->nrows * sizeof(char**));
        for (int i=0; i<vrs->nrows; i++) out->rows[i] = vrs->rows[i].cells;
        return 0;
    }

    if (catalog_get_schema(g_app.catalog, tname, &out->schema, a) != 0 || !out->schema)
        return -1;

    int ncols = out->schema->ncols;
    char wal_path[1024];
    snprintf(wal_path, sizeof(wal_path), "%s/%s/wal.bin", g_app.data_dir, tname);

    /* ── Index-accelerated path ── */
    if (ih && ih->valid && ih->col) {
        /* find col_idx in schema */
        int ci = -1;
        for (int i = 0; i < ncols; i++)
            if (out->schema->cols[i].name &&
                strcasecmp(out->schema->cols[i].name, ih->col) == 0 &&
                out->schema->cols[i].type == COL_INT64) { ci = i; break; }

        if (ci >= 0) {
            /* look for open B-tree handle in g_app.tables */
            pthread_mutex_lock(&g_app.tables_mu);
            Table *t = hm_get(&g_app.tables, tname);
            BTree *bt = t ? table_get_index(t, ci) : NULL;
            pthread_mutex_unlock(&g_app.tables_mu);

            if (bt) {
                /* allocate offsets on heap (can be large) */
                int64_t *offs = malloc((size_t)BT_MAX_OFFSETS * sizeof(int64_t));
                int noffs = 0;
                btree_range(bt, ih->lo, ih->hi, offs, &noffs);

                int cap = noffs > 0 ? noffs : 1;
                char ***rows = arena_alloc(a, (size_t)cap * sizeof(char**));
                int n = 0;

                FILE *wf = fopen(wal_path, "rb");
                if (wf) {
                    for (int oi = 0; oi < noffs; oi++) {
                        if (fseeko(wf, (off_t)offs[oi], SEEK_SET) != 0) continue;
                        char **row = parse_wal_row(a, wf, ncols);
                        if (row) rows[n++] = row;
                    }
                    fclose(wf);
                }
                free(offs);
                out->rows = rows; out->nrows = n;
                return 0;
            }
        }
    }

    /* ── Full scan with tombstone support ── */
    FILE *wf = fopen(wal_path, "rb");
    if (!wf) { out->nrows=0; return 0; }

    /* Pass 1: collect tombstones */
    typedef struct { int64_t orig_off; uint8_t op; char *new_csv; size_t new_len; } Tombstone;
    int tb_cap=16, ntb=0;
    Tombstone *tbs = arena_alloc(a, (size_t)tb_cap * sizeof(Tombstone));
    {
        int64_t file_off=0;
        char tbuf[262144];
        while (1) {
            uint32_t l=0;
            if (fread(&l,1,4,wf)!=4) break;
            if (l==0||l>sizeof(tbuf)-1) { fseek(wf,(long)l,SEEK_CUR); file_off+=4+(int64_t)l; continue; }
            if (fread(tbuf,1,l,wf)!=l) break;
            file_off += 4+(int64_t)l;
            uint8_t op=(uint8_t)tbuf[0];
            if ((op==WAL_OP_DELETE||op==WAL_OP_UPDATE) && l>=9) {
                int64_t orig=0;
                for(int b=0;b<8;b++) orig=(orig<<8)|((uint8_t)tbuf[1+b]);
                if(ntb==tb_cap){tb_cap*=2;Tombstone*nb=arena_alloc(a,(size_t)tb_cap*sizeof(Tombstone));memcpy(nb,tbs,(size_t)ntb*sizeof(Tombstone));tbs=nb;}
                tbs[ntb].orig_off=orig; tbs[ntb].op=op;
                tbs[ntb].new_csv=NULL; tbs[ntb].new_len=0;
                if (op==WAL_OP_UPDATE && l>9) {
                    size_t csv_len=l-9;
                    char *nc=arena_alloc(a,csv_len+1);
                    memcpy(nc,tbuf+9,csv_len); nc[csv_len]='\0';
                    tbs[ntb].new_csv=nc; tbs[ntb].new_len=csv_len;
                }
                ntb++;
            }
        }
        rewind(wf);
    }

    /* Pass 2: yield rows with tombstones applied */
    int cap=128, n=0;
    char ***rows = arena_alloc(a, (size_t)cap * sizeof(char**));
    int64_t file_off=0;
    while (1) {
        uint32_t l=0;
        if (fread(&l,1,4,wf)!=4) break;
        int64_t rec_off=file_off; file_off+=4+(int64_t)l;
        if (l==0||l>262144) { fseek(wf,(long)l,SEEK_CUR); continue; }
        char *line = arena_alloc(a,(size_t)l+1);
        if (fread(line,1,l,wf)!=l) break;
        line[l]='\0';

        uint8_t op=(uint8_t)(unsigned char)line[0];
        if (op==WAL_OP_DELETE||op==WAL_OP_UPDATE) continue; /* skip tombstone records */

        size_t rl=strlen(line);
        while(rl>0&&(line[rl-1]=='\n'||line[rl-1]=='\r')) line[--rl]='\0';

        int printable=0;
        for (size_t ci=0;ci<rl;ci++) if ((unsigned char)line[ci]>=0x20) printable++;
        if (printable < 2) continue;

        /* Check tombstones */
        bool dead=false; char *upd_csv=NULL; size_t upd_len=0;
        for(int ti=0;ti<ntb;ti++) {
            if(tbs[ti].orig_off==rec_off) {
                dead=true;
                if(tbs[ti].op==WAL_OP_UPDATE) { upd_csv=tbs[ti].new_csv; upd_len=tbs[ti].new_len; }
                break;
            }
        }
        if(dead && !upd_csv) continue; /* deleted */
        if(upd_csv) {
            /* apply update: parse new CSV */
            char *uline=arena_alloc(a,upd_len+1);
            memcpy(uline,upd_csv,upd_len); uline[upd_len]='\0';
            size_t ul=strlen(uline);
            while(ul>0&&(uline[ul-1]=='\n'||uline[ul-1]=='\r')) uline[--ul]='\0';
            char *vals[MAX_COLS]={0}; int nv=0;
            split_line_simple(uline,',',vals,MAX_COLS,&nv);
            char **row=arena_alloc(a,(size_t)ncols*sizeof(char*));
            for(int i=0;i<ncols;i++) row[i]=(i<nv&&vals[i])?vals[i]:"";
            if(n==cap){cap*=2;char***nb=arena_alloc(a,(size_t)cap*sizeof(char**));memcpy(nb,rows,(size_t)n*sizeof(char**));rows=nb;}
            rows[n++]=row;
            continue;
        }

        char *vals[MAX_COLS]={0}; int nv=0;
        split_line_simple(line,',',vals,MAX_COLS,&nv);
        char **row=arena_alloc(a,(size_t)ncols*sizeof(char*));
        for(int i=0;i<ncols;i++) row[i]=(i<nv&&vals[i])?vals[i]:"";

        if(n==cap){cap*=2;char***nb=arena_alloc(a,(size_t)cap*sizeof(char**));memcpy(nb,rows,(size_t)n*sizeof(char**));rows=nb;}
        rows[n++]=row;
    }
    fclose(wf);
    out->rows=rows; out->nrows=n;
    return 0;
}

/* ── Execution state passed through the recursive join ── */
typedef struct {
    Arena            *a;
    const SelectStmt *sel;
    VTReg            *vt;
    RS               *rs;
    bool              do_agg;
    bool              implicit_agg;  /* agg with no GROUP BY */
    GrpAcc           *grps;
    int               ngrps, cap_grps;
} ExecState;


static GrpAcc *find_or_create_grp(ExecState *st, const char *key) {
    for (int i=0;i<st->ngrps;i++) if(!strcmp(st->grps[i].key,key)) return &st->grps[i];
    if (st->ngrps==st->cap_grps) {
        int nc=st->cap_grps*2;
        GrpAcc *nb=arena_alloc(st->a,(size_t)nc*sizeof(GrpAcc));
        memcpy(nb,st->grps,(size_t)st->ngrps*sizeof(GrpAcc));
        st->grps=nb; st->cap_grps=nc;
    }
    GrpAcc *g=&st->grps[st->ngrps++];
    memset(g,0,sizeof(*g));
    g->key=arena_strdup(st->a,key);
    int nc=st->sel->nselect;
    g->sums     =arena_calloc(st->a,(size_t)nc*sizeof(double));
    g->mins     =arena_alloc (st->a,(size_t)nc*sizeof(double)); memset(g->mins,0,(size_t)nc*sizeof(double));
    g->maxs     =arena_alloc (st->a,(size_t)nc*sizeof(double)); memset(g->maxs,0,(size_t)nc*sizeof(double));
    g->cnts     =arena_calloc(st->a,(size_t)nc*sizeof(int));
    g->min_set  =arena_calloc(st->a,(size_t)nc*sizeof(bool));
    g->max_set  =arena_calloc(st->a,(size_t)nc*sizeof(bool));
    g->first_str=arena_calloc(st->a,(size_t)nc*sizeof(char*));
    return g;
}

static void agg_row(ExecState *st, GrpAcc *g, JoinCtx *ctx) {
    g->total_cnt++;
    for (int s=0;s<st->sel->nselect;s++) {
        Expr *se=st->sel->select_list[s];
        Expr *base=(se->type==EXPR_ALIAS)?se->expr:se;
        Expr *agg_e = (base->type==EXPR_FUNC && base->func_name && is_agg_name(base->func_name))
                      ? base : find_first_agg(base);
        if (agg_e) {
            const char *fn=agg_e->func_name;
            bool is_star=(agg_e->nargs>0&&agg_e->args[0]->type==EXPR_STAR);
            if (!strcasecmp(fn,"COUNT")) {
                if (is_star) g->cnts[s]++;
                else {
                    Val v=eval_val(agg_e->nargs>0?agg_e->args[0]:NULL,ctx,st->a);
                    if (!v.is_null) g->cnts[s]++;
                }
            } else {
                Val v=eval_val(agg_e->nargs>0?agg_e->args[0]:NULL,ctx,st->a);
                if (!v.is_null) {
                    double n=v.num; if (!v.is_num) pnum(val_str(v,st->a),&n);
                    g->sums[s]+=n; g->cnts[s]++;
                    if (!g->min_set[s]||n<g->mins[s]){g->mins[s]=n;g->min_set[s]=true;}
                    if (!g->max_set[s]||n>g->maxs[s]){g->maxs[s]=n;g->max_set[s]=true;}
                }
            }
        } else {
            /* non-agg: keep first value */
            if (!g->first_str[s]) {
                Val v=eval_val(base,ctx,st->a);
                g->first_str[s]=arena_strdup(st->a,val_str(v,st->a));
            }
        }
    }
}

static void emit_groups(ExecState *st) {
    /* Build a synthetic schema for HAVING evaluation */
    int nc=st->sel->nselect;
    Schema *out_sc=arena_calloc(st->a,sizeof(Schema));
    out_sc->ncols=nc; out_sc->cols=arena_alloc(st->a,(size_t)nc*sizeof(ColDef));
    for(int i=0;i<nc;i++){
        out_sc->cols[i].name=expr_name(st->sel->select_list[i],i,st->a);
        out_sc->cols[i].type=COL_TEXT; out_sc->cols[i].nullable=true;
    }

    for (int gi=0;gi<st->ngrps;gi++) {
        GrpAcc *g=&st->grps[gi];

        /* expose group to eval_val so ROUND(AVG(x)) etc. work */
        tl_grp = g;
        tl_sel = st->sel;

        char **cells=arena_alloc(st->a,(size_t)nc*sizeof(char*));
        JoinCtx empty_ctx={0};
        for (int s=0;s<nc;s++) {
            Expr *se=st->sel->select_list[s];
            Expr *base=(se->type==EXPR_ALIAS)?se->expr:se;
            if (base->type==EXPR_FUNC && base->func_name && is_agg_name(base->func_name)) {
                /* pure aggregate at top level — read directly from accumulator */
                const char *fn=base->func_name;
                if (!strcasecmp(fn,"COUNT")) cells[s]=arena_sprintf(st->a,"%d",g->cnts[s]);
                else if (!strcasecmp(fn,"SUM")) cells[s]=arena_sprintf(st->a,"%.10g",g->sums[s]);
                else if (!strcasecmp(fn,"AVG")) cells[s]=g->cnts[s]?arena_sprintf(st->a,"%.10g",g->sums[s]/g->cnts[s]):"";
                else if (!strcasecmp(fn,"MIN")) cells[s]=g->min_set[s]?arena_sprintf(st->a,"%.10g",g->mins[s]):"";
                else if (!strcasecmp(fn,"MAX")) cells[s]=g->max_set[s]?arena_sprintf(st->a,"%.10g",g->maxs[s]):"";
                else cells[s]="";
            } else if (expr_has_agg(base)) {
                /* expression wrapping aggregate (e.g. ROUND(AVG(x))) — eval with group ctx */
                Val v=eval_val(se,&empty_ctx,st->a);
                cells[s]=arena_strdup(st->a,val_str(v,st->a));
            } else {
                cells[s]=g->first_str[s]?g->first_str[s]:"";
            }
        }

        /* HAVING — evaluated with group context active */
        if (st->sel->having) {
            JoinCtx hctx={0}; hctx.n=1;
            hctx.schemas[0]=out_sc; hctx.rows[0]=cells;
            hctx.tnames[0]=""; hctx.aliases[0]="";
            Val hv=eval_val(st->sel->having,&hctx,st->a);
            tl_grp=NULL; tl_sel=NULL;
            if (!vt(hv)) continue;
            tl_grp=g; tl_sel=st->sel;
        }

        /* sort keys */
        char **skeys=NULL;
        if (st->sel->norder>0) {
            JoinCtx sctx={0}; sctx.n=1;
            sctx.schemas[0]=out_sc; sctx.rows[0]=cells;
            skeys=arena_alloc(st->a,(size_t)st->sel->norder*sizeof(char*));
            for(int o=0;o<st->sel->norder;o++) {
                Val sv=eval_val(st->sel->order_by[o].expr,&sctx,st->a);
                skeys[o]=arena_strdup(st->a,val_str(sv,st->a));
            }
        }

        tl_grp=NULL; tl_sel=NULL;
        rs_add(st->rs, st->a, cells, skeys);
    }
}

static void collect_row(ExecState *st, JoinCtx *ctx) {
    /* WHERE filter */
    if (st->sel->where) {
        Val w=eval_val(st->sel->where,ctx,st->a);
        if (!vt(w)) return;
    }

    if (st->do_agg) {
        /* Build group key */
        char kbuf[4096]; kbuf[0]='\0';
        if (!st->implicit_agg) {
            for (int g=0;g<st->sel->ngroup;g++) {
                Val gv=eval_val(st->sel->group_by[g],ctx,st->a);
                if (g) strncat(kbuf,"\x1F",sizeof(kbuf)-strlen(kbuf)-1);
                strncat(kbuf,val_str(gv,st->a),sizeof(kbuf)-strlen(kbuf)-1);
            }
        }
        GrpAcc *grp=find_or_create_grp(st,kbuf);
        agg_row(st,grp,ctx);
        return;
    }

    /* Direct projection */
    int nc=st->sel->nselect;
    bool star=(nc==1 && st->sel->select_list[0]->type==EXPR_STAR);
    int out_cols=star?0:nc;
    if (star) {
        /* expand star: count all columns */
        for (int t=0;t<ctx->n;t++) {
            if (ctx->schemas[t]) out_cols+=ctx->schemas[t]->ncols;
        }
    }

    char **cells=arena_alloc(st->a,(size_t)out_cols*sizeof(char*));
    if (star) {
        int ci=0;
        for (int t=0;t<ctx->n;t++) {
            if (!ctx->schemas[t]||!ctx->rows[t]) {
                if (ctx->schemas[t]) for (int c=0;c<ctx->schemas[t]->ncols;c++) cells[ci++]="";
                continue;
            }
            for (int c=0;c<ctx->schemas[t]->ncols;c++) cells[ci++]=ctx->rows[t][c]?ctx->rows[t][c]:"";
        }
    } else {
        for (int s=0;s<nc;s++) {
            Expr *se=st->sel->select_list[s];
            Expr *base=(se->type==EXPR_ALIAS)?se->expr:se;
            Val v=eval_val(base,ctx,st->a);
            cells[s]=arena_strdup(st->a,val_str(v,st->a));
        }
    }

    /* sort keys — resolve aliases against SELECT list before evaluating */
    char **skeys=NULL;
    if (st->sel->norder>0) {
        skeys=arena_alloc(st->a,(size_t)st->sel->norder*sizeof(char*));
        for(int o=0;o<st->sel->norder;o++) {
            Expr *oe=st->sel->order_by[o].expr;
            /* resolve SELECT alias: ORDER BY alias_name → eval the aliased expr */
            if (oe && oe->type==EXPR_COL && oe->name) {
                for(int s=0;s<st->sel->nselect;s++){
                    Expr *se=st->sel->select_list[s];
                    if (se->type==EXPR_ALIAS && se->alias && !strcasecmp(se->alias,oe->name)) {
                        oe=se->expr; break;
                    }
                }
            }
            /* positional ORDER BY: integer literal → use that SELECT column value */
            if (oe && oe->type==EXPR_LITERAL_INT && oe->ival>=1 && oe->ival<=st->sel->nselect) {
                Expr *se=st->sel->select_list[oe->ival-1];
                if (se->type==EXPR_ALIAS) se=se->expr;
                oe=se;
            }
            Val sv=eval_val(oe,ctx,st->a);
            skeys[o]=arena_strdup(st->a,val_str(sv,st->a));
        }
    }

    rs_add(st->rs, st->a, cells, skeys);
}

static void join_recurse(ExecState *st, int depth, JoinCtx *ctx,
                         TblData *tables, int ntables, bool force_nulls) {
    if (force_nulls) {
        /* propagate NULLs for remaining tables */
        for (int i=depth;i<ntables;i++) {
            ctx->rows[i]=NULL; ctx->schemas[i]=tables[i].schema;
            ctx->tnames[i]=tables[i].tname; ctx->aliases[i]=tables[i].alias;
        }
        ctx->n=ntables;
        collect_row(st, ctx);
        return;
    }
    if (depth == ntables) {
        ctx->n = ntables;
        collect_row(st, ctx);
        return;
    }

    TblData *td=&tables[depth];
    ctx->schemas[depth]=td->schema;
    ctx->tnames[depth]=td->tname;
    ctx->aliases[depth]=td->alias;

    JoinType jtype=(depth==0)?JOIN_INNER:st->sel->from[depth].join_type;
    Expr *join_on=(depth==0)?NULL:st->sel->from[depth].on;

    if (jtype==JOIN_CROSS || (td->nrows==0 && jtype!=JOIN_LEFT && jtype!=JOIN_FULL)) {
        if (td->nrows==0 && jtype==JOIN_CROSS) return; /* CROSS JOIN with empty table = empty */
        if (td->nrows==0 && jtype==JOIN_LEFT && depth>0) {
            ctx->rows[depth]=NULL;
            join_recurse(st, depth+1, ctx, tables, ntables, false);
            return;
        }
    }

    bool any_match=false;
    for (int i=0;i<td->nrows;i++) {
        ctx->rows[depth]=td->rows[i];
        if (join_on) {
            ctx->n=depth+1;
            Val cv=eval_val(join_on,ctx,st->a);
            if (!vt(cv)) continue;
        }
        any_match=true;
        join_recurse(st, depth+1, ctx, tables, ntables, false);
    }

    if (!any_match && (jtype==JOIN_LEFT || (jtype==JOIN_FULL && depth==0))) {
        ctx->rows[depth]=NULL;
        join_recurse(st, depth+1, ctx, tables, ntables, jtype==JOIN_LEFT);
    }
}

/* ── Detect if any SELECT expression contains an aggregate ── */
static bool expr_has_agg(Expr *e) {
    if (!e) return false;
    Expr *b=(e->type==EXPR_ALIAS)?e->expr:e;
    if (b->type==EXPR_FUNC && b->func_name && is_agg_name(b->func_name)) return true;
    if (b->type==EXPR_FUNC) { for(int i=0;i<b->nargs;i++) if(expr_has_agg(b->args[i])) return true; return false; }
    if (b->type==EXPR_BINOP) return expr_has_agg(b->left)||expr_has_agg(b->right);
    if (b->type==EXPR_UNOP) return expr_has_agg(b->left);
    if (b->type==EXPR_CASE) {
        for(int i=0;i<b->nwhens;i++) if(expr_has_agg(b->whens[i])||expr_has_agg(b->thens[i])) return true;
        return expr_has_agg(b->else_expr);
    }
    return false;
}

/* Return first aggregate sub-expression found by DFS (NULL if none) */
static Expr *find_first_agg(Expr *e) {
    if (!e) return NULL;
    if (e->type==EXPR_ALIAS) return find_first_agg(e->expr);
    if (e->type==EXPR_FUNC && e->func_name) {
        if (is_agg_name(e->func_name)) return e;
        for (int i=0;i<e->nargs;i++) { Expr *r=find_first_agg(e->args[i]); if(r) return r; }
        return NULL;
    }
    if (e->type==EXPR_BINOP||e->type==EXPR_UNOP) {
        Expr *r=find_first_agg(e->left); if(r) return r;
        return find_first_agg(e->right);
    }
    if (e->type==EXPR_CASE) {
        for(int i=0;i<e->nwhens;i++) {
            Expr *r=find_first_agg(e->whens[i]); if(r) return r;
            r=find_first_agg(e->thens[i]); if(r) return r;
        }
        return find_first_agg(e->else_expr);
    }
    return NULL;
}

/* Return true if expr tree contains an agg function named fn with identical args (by ptr) */
static bool expr_find_agg_args(Expr *e, const char *fn, Expr **call_args, int nargs) {
    if (!e) return false;
    if (e->type==EXPR_ALIAS) return expr_find_agg_args(e->expr, fn, call_args, nargs);
    if (e->type==EXPR_FUNC && e->func_name) {
        if (!strcasecmp(e->func_name, fn) && is_agg_name(fn)) {
            /* match by argument pointer identity */
            if (e->nargs == nargs) {
                bool ok=true;
                for(int i=0;i<nargs;i++) if(e->args[i]!=call_args[i]){ok=false;break;}
                if (ok) return true;
            }
            /* COUNT(*) special: accept if both have EXPR_STAR first arg */
            if (nargs==1 && e->nargs==1 &&
                call_args[0]->type==EXPR_STAR && e->args[0]->type==EXPR_STAR) return true;
        }
        for (int i=0;i<e->nargs;i++) if(expr_find_agg_args(e->args[i],fn,call_args,nargs)) return true;
        return false;
    }
    if (e->type==EXPR_BINOP||e->type==EXPR_UNOP)
        return expr_find_agg_args(e->left,fn,call_args,nargs)||expr_find_agg_args(e->right,fn,call_args,nargs);
    if (e->type==EXPR_CASE) {
        for(int i=0;i<e->nwhens;i++)
            if(expr_find_agg_args(e->whens[i],fn,call_args,nargs)||
               expr_find_agg_args(e->thens[i],fn,call_args,nargs)) return true;
        return expr_find_agg_args(e->else_expr,fn,call_args,nargs);
    }
    return false;
}

/* ── Build column names for a star expansion ── */
static void build_star_col_names(Arena *a, const SelectStmt *sel, TblData *tables, int ntables,
                                  char ***names_out, int *ncols_out) {
    int nc=0;
    for (int t=0;t<ntables;t++) if(tables[t].schema) nc+=tables[t].schema->ncols;
    char **names=arena_alloc(a,(size_t)nc*sizeof(char*));
    int ci=0;
    for (int t=0;t<ntables;t++) {
        if (!tables[t].schema) continue;
        for (int c=0;c<tables[t].schema->ncols;c++) {
            /* prefix with table alias if multiple tables */
            if (ntables>1 && (tables[t].alias||tables[t].tname)) {
                const char *pfx=tables[t].alias?tables[t].alias:tables[t].tname;
                names[ci++]=arena_sprintf(a,"%s.%s",pfx,tables[t].schema->cols[c].name);
            } else {
                names[ci++]=arena_strdup(a,tables[t].schema->cols[c].name);
            }
        }
    }
    *names_out=names; *ncols_out=nc;
    (void)sel;
}

/* ── Window function support ── */

static bool expr_has_window(Expr *e) {
    if (!e) return false;
    if (e->type == EXPR_WINDOW) return true;
    if (e->type == EXPR_ALIAS)  return expr_has_window(e->expr);
    if (e->type == EXPR_BINOP)  return expr_has_window(e->left) || expr_has_window(e->right);
    if (e->type == EXPR_UNOP)   return expr_has_window(e->left);
    if (e->type == EXPR_FUNC) {
        for (int i = 0; i < e->nargs; i++) if (expr_has_window(e->args[i])) return true;
    }
    if (e->type == EXPR_CASE) {
        if (expr_has_window(e->case_op)) return true;
        for (int i = 0; i < e->nwhens; i++)
            if (expr_has_window(e->whens[i]) || expr_has_window(e->thens[i])) return true;
        return expr_has_window(e->else_expr);
    }
    return false;
}

/* Collect all unique EXPR_WINDOW nodes from expression tree into out[]. */
static int collect_win_exprs(Expr *e, Expr **out, int cap, int n) {
    if (!e || n >= cap) return n;
    if (e->type == EXPR_WINDOW) {
        for (int i = 0; i < n; i++) if (out[i] == e) return n;
        out[n++] = e; return n;
    }
    if (e->type == EXPR_ALIAS)  return collect_win_exprs(e->expr, out, cap, n);
    if (e->type == EXPR_BINOP) { n = collect_win_exprs(e->left,out,cap,n); return collect_win_exprs(e->right,out,cap,n); }
    if (e->type == EXPR_UNOP)   return collect_win_exprs(e->left, out, cap, n);
    if (e->type == EXPR_FUNC) {
        for (int i = 0; i < e->nargs; i++) n = collect_win_exprs(e->args[i], out, cap, n);
    }
    if (e->type == EXPR_CASE) {
        n = collect_win_exprs(e->case_op, out, cap, n);
        for (int i = 0; i < e->nwhens; i++) {
            n = collect_win_exprs(e->whens[i], out, cap, n);
            n = collect_win_exprs(e->thens[i], out, cap, n);
        }
        n = collect_win_exprs(e->else_expr, out, cap, n);
    }
    return n;
}

/* Resolve frame start index (0-based position within sorted partition). */
static int win_frame_start(WindowSpec *ws, int pi, int plen) {
    (void)plen;
    switch (ws->frame_start.kind) {
    case WBOUND_UNBOUNDED_PREC: return 0;
    case WBOUND_N_PREC: { int v = pi - (int)ws->frame_start.n; return v < 0 ? 0 : v; }
    case WBOUND_N_FOLL: { int v = pi + (int)ws->frame_start.n; return v >= plen ? plen : v; }
    default:            return pi; /* CURRENT_ROW */
    }
}

/* Resolve frame end index. */
static int win_frame_end(WindowSpec *ws, int pi, int plen) {
    switch (ws->frame_end.kind) {
    case WBOUND_UNBOUNDED_FOLL: return plen - 1;
    case WBOUND_N_FOLL: { int v = pi + (int)ws->frame_end.n; return v >= plen ? plen-1 : v; }
    case WBOUND_N_PREC: { int v = pi - (int)ws->frame_end.n; return v < 0 ? 0 : v; }
    case WBOUND_UNBOUNDED_PREC: return 0;
    default:            return pi; /* CURRENT_ROW */
    }
}

/* Compare two rows by their per-key order-key arrays. Returns <0 / 0 / >0. */
static int win_ord_cmp(char **ak, char **bk, OrderItem *order_by, int norder) {
    for (int k = 0; k < norder; k++) {
        double na, nb;
        int c;
        if (pnum(ak[k], &na) && pnum(bk[k], &nb)) c = (na<nb)?-1:(na>nb?1:0);
        else c = strcmp(ak[k], bk[k]);
        if (order_by[k].desc) c = -c;
        if (c != 0) return c;
    }
    return 0;
}

/*
 * apply_windows — post-processing step that computes all window function values
 * for every row in rs, then re-evaluates expressions containing EXPR_WINDOW nodes
 * so that combined expressions like "revenue - LAG(...)" work correctly.
 */
static void apply_windows(Arena *a, RS *rs, const SelectStmt *sel) {
    if (rs->nrows == 0) return;

    /* Collect all unique EXPR_WINDOW nodes across the select list */
#define MAX_WIN_EXPRS 32
    Expr *win_exprs[MAX_WIN_EXPRS];
    int nwin = 0;
    for (int si = 0; si < sel->nselect; si++)
        nwin = collect_win_exprs(sel->select_list[si], win_exprs, MAX_WIN_EXPRS, nwin);
    if (nwin == 0) return;

    /* Build output schema: maps col_names → cells positions */
    Schema *out_sc = arena_calloc(a, sizeof(Schema));
    out_sc->ncols = rs->ncols;
    out_sc->cols  = arena_alloc(a, (size_t)rs->ncols * sizeof(ColDef));
    for (int i = 0; i < rs->ncols; i++) {
        out_sc->cols[i].name = rs->col_names[i];
        out_sc->cols[i].type = COL_TEXT;
    }

    /* Allocate per-window, per-row value storage */
    WinValEntry *wve = arena_alloc(a, (size_t)nwin * sizeof(WinValEntry));
    for (int wi = 0; wi < nwin; wi++) {
        wve[wi].win_expr = win_exprs[wi];
        wve[wi].values   = arena_alloc(a, (size_t)rs->nrows * sizeof(char*));
        for (int r = 0; r < rs->nrows; r++) wve[wi].values[r] = "";
    }

    /* Compute window function values for each EXPR_WINDOW node */
    for (int wi = 0; wi < nwin; wi++) {
        Expr      *e  = win_exprs[wi];
        WindowSpec *ws = e->win_spec;
        const char *fn = e->func_name ? e->func_name : "";

        /* Compute partition key string and per-order-key strings for every row */
        char  **part_keys    = arena_alloc(a, (size_t)rs->nrows * sizeof(char*));
        char ***row_ord_keys = NULL;
        if (ws->norder > 0)
            row_ord_keys = arena_alloc(a, (size_t)rs->nrows * sizeof(char**));

        for (int r = 0; r < rs->nrows; r++) {
            JoinCtx ctx = {0};
            ctx.n = 1; ctx.schemas[0] = out_sc;
            ctx.rows[0] = rs->rows[r].cells;
            ctx.tnames[0] = ""; ctx.aliases[0] = "";

            char kbuf[4096]; kbuf[0] = '\0';
            for (int k = 0; k < ws->npartition; k++) {
                Val v = eval_val(ws->partition_by[k], &ctx, a);
                if (k) strncat(kbuf, "\x01", sizeof(kbuf)-strlen(kbuf)-1);
                strncat(kbuf, val_str(v, a), sizeof(kbuf)-strlen(kbuf)-1);
            }
            part_keys[r] = arena_strdup(a, kbuf);

            if (ws->norder > 0) {
                row_ord_keys[r] = arena_alloc(a, (size_t)ws->norder * sizeof(char*));
                for (int k = 0; k < ws->norder; k++) {
                    Val v = eval_val(ws->order_by[k].expr, &ctx, a);
                    row_ord_keys[r][k] = arena_strdup(a, val_str(v, a));
                }
            }
        }

        /* Sort row indices by (partition_key, order_keys) — stable insertion sort */
        int *idx = arena_alloc(a, (size_t)rs->nrows * sizeof(int));
        for (int r = 0; r < rs->nrows; r++) idx[r] = r;
        for (int i = 1; i < rs->nrows; i++) {
            int ki = idx[i], j = i - 1;
            while (j >= 0) {
                int ai = idx[j];
                int c = strcmp(part_keys[ai], part_keys[ki]);
                if (c == 0 && ws->norder > 0)
                    c = win_ord_cmp(row_ord_keys[ai], row_ord_keys[ki], ws->order_by, ws->norder);
                if (c <= 0) break;
                idx[j+1] = idx[j]; j--;
            }
            idx[j+1] = ki;
        }

        /* Walk partitions and compute window values per function type */
        const char *prev_part = NULL;
        int part_start = 0;

        for (int i = 0; i <= rs->nrows; i++) {
            bool ep = (i == rs->nrows) || (!prev_part) ||
                      (strcmp(part_keys[idx[i]], prev_part) != 0);

            if (i > 0 && ep) {
                int plen = i - part_start;

                if (!strcasecmp(fn, "row_number")) {
                    for (int pi = 0; pi < plen; pi++)
                        wve[wi].values[idx[part_start+pi]] = arena_sprintf(a, "%d", pi+1);

                } else if (!strcasecmp(fn, "rank")) {
                    char **prev_ord = NULL;
                    int rank = 1;
                    for (int pi = 0; pi < plen; pi++) {
                        int ri = idx[part_start+pi];
                        if (pi == 0) {
                            prev_ord = ws->norder > 0 ? row_ord_keys[ri] : NULL;
                        } else {
                            bool same = true;
                            if (ws->norder > 0 && prev_ord)
                                for (int k=0; k<ws->norder && same; k++)
                                    if (strcmp(prev_ord[k], row_ord_keys[ri][k])) same=false;
                            if (!same) { rank = pi+1; prev_ord = ws->norder > 0 ? row_ord_keys[ri] : NULL; }
                        }
                        wve[wi].values[ri] = arena_sprintf(a, "%d", rank);
                    }

                } else if (!strcasecmp(fn, "dense_rank")) {
                    char **prev_ord = NULL;
                    int dr = 1;
                    for (int pi = 0; pi < plen; pi++) {
                        int ri = idx[part_start+pi];
                        if (pi == 0) {
                            prev_ord = ws->norder > 0 ? row_ord_keys[ri] : NULL;
                        } else {
                            bool same = true;
                            if (ws->norder > 0 && prev_ord)
                                for (int k=0; k<ws->norder && same; k++)
                                    if (strcmp(prev_ord[k], row_ord_keys[ri][k])) same=false;
                            if (!same) { dr++; prev_ord = ws->norder > 0 ? row_ord_keys[ri] : NULL; }
                        }
                        wve[wi].values[ri] = arena_sprintf(a, "%d", dr);
                    }

                } else if (!strcasecmp(fn, "lag") || !strcasecmp(fn, "lead")) {
                    int offset = 1;
                    const char *def_val = "0";
                    if (e->nargs >= 2) {
                        JoinCtx c0 = {0}; Val ov = eval_val(e->args[1], &c0, a);
                        if (ov.is_num) offset = (int)ov.num;
                    }
                    if (e->nargs >= 3) {
                        JoinCtx c0 = {0}; Val dv = eval_val(e->args[2], &c0, a);
                        def_val = arena_strdup(a, val_str(dv, a));
                    }
                    bool is_lead = !strcasecmp(fn, "lead");
                    for (int pi = 0; pi < plen; pi++) {
                        int ri  = idx[part_start+pi];
                        int spi = is_lead ? (pi + offset) : (pi - offset);
                        if (spi < 0 || spi >= plen) {
                            wve[wi].values[ri] = arena_strdup(a, def_val);
                        } else {
                            int sri = idx[part_start+spi];
                            JoinCtx ctx = {0}; ctx.n=1; ctx.schemas[0]=out_sc;
                            ctx.rows[0]=rs->rows[sri].cells; ctx.tnames[0]=""; ctx.aliases[0]="";
                            Val v = e->nargs > 0 ? eval_val(e->args[0], &ctx, a) : vnull();
                            wve[wi].values[ri] = arena_strdup(a, val_str(v, a));
                        }
                    }

                } else if (!strcasecmp(fn, "first_value") || !strcasecmp(fn, "last_value")) {
                    bool is_last = !strcasecmp(fn, "last_value");
                    for (int pi = 0; pi < plen; pi++) {
                        int ri  = idx[part_start+pi];
                        int f_s = ws->has_frame ? win_frame_start(ws,pi,plen) : 0;
                        int f_e = ws->has_frame ? win_frame_end(ws,pi,plen)   : plen-1;
                        int spi = is_last ? f_e : f_s;
                        int sri = idx[part_start+spi];
                        JoinCtx ctx = {0}; ctx.n=1; ctx.schemas[0]=out_sc;
                        ctx.rows[0]=rs->rows[sri].cells; ctx.tnames[0]=""; ctx.aliases[0]="";
                        Val v = e->nargs > 0 ? eval_val(e->args[0], &ctx, a) : vnull();
                        wve[wi].values[ri] = arena_strdup(a, val_str(v, a));
                    }

                } else {
                    /* Aggregate window: AVG / SUM / MIN / MAX / COUNT with frame */
                    for (int pi = 0; pi < plen; pi++) {
                        int ri  = idx[part_start+pi];
                        int f_s, f_e;
                        if (ws->has_frame) {
                            f_s = win_frame_start(ws, pi, plen);
                            f_e = win_frame_end(ws, pi, plen);
                        } else if (ws->norder > 0) {
                            f_s = 0; f_e = pi; /* default: UNBOUNDED PRECEDING to CURRENT ROW */
                        } else {
                            f_s = 0; f_e = plen - 1; /* no ORDER: entire partition */
                        }
                        double sum=0, cnt=0, minv=0, maxv=0;
                        bool min_set=false, max_set=false;
                        for (int fi = f_s; fi <= f_e; fi++) {
                            int fri = idx[part_start+fi];
                            JoinCtx ctx = {0}; ctx.n=1; ctx.schemas[0]=out_sc;
                            ctx.rows[0]=rs->rows[fri].cells; ctx.tnames[0]=""; ctx.aliases[0]="";
                            Val v = e->nargs > 0 ? eval_val(e->args[0], &ctx, a) : vnull();
                            if (!v.is_null) {
                                double nv = v.is_num ? v.num : 0;
                                if (!v.is_num) pnum(val_str(v, a), &nv);
                                sum += nv; cnt++;
                                if (!min_set || nv < minv) { minv=nv; min_set=true; }
                                if (!max_set || nv > maxv) { maxv=nv; max_set=true; }
                            }
                        }
                        const char *res;
                        if      (!strcasecmp(fn,"count")) res = arena_sprintf(a,"%.0f",cnt);
                        else if (!strcasecmp(fn,"sum"))   res = arena_sprintf(a,"%.10g",sum);
                        else if (!strcasecmp(fn,"avg"))   res = cnt>0 ? arena_sprintf(a,"%.10g",sum/cnt) : "";
                        else if (!strcasecmp(fn,"min"))   res = min_set ? arena_sprintf(a,"%.10g",minv) : "";
                        else if (!strcasecmp(fn,"max"))   res = max_set ? arena_sprintf(a,"%.10g",maxv) : "";
                        else res = "";
                        wve[wi].values[ri] = arena_strdup(a, res);
                    }
                }
            }

            if (i < rs->nrows) {
                if (!prev_part || strcmp(part_keys[idx[i]], prev_part) != 0) {
                    prev_part = part_keys[idx[i]];
                    part_start = i;
                }
            }
        }
    }

    /* Re-evaluate all select expressions that contain a window node,
       with tl_win_cur_row set so EXPR_WINDOW returns the pre-computed value. */
    tl_win_vals    = wve;
    tl_nwin_vals   = nwin;

    for (int si = 0; si < sel->nselect; si++) {
        if (!expr_has_window(sel->select_list[si])) continue;
        Expr *se   = sel->select_list[si];
        Expr *base = (se->type == EXPR_ALIAS) ? se->expr : se;
        for (int r = 0; r < rs->nrows; r++) {
            tl_win_cur_row = r;
            JoinCtx ctx = {0}; ctx.n=1; ctx.schemas[0]=out_sc;
            ctx.rows[0]=rs->rows[r].cells; ctx.tnames[0]=""; ctx.aliases[0]="";
            Val v = eval_val(base, &ctx, a);
            rs->rows[r].cells[si] = arena_strdup(a, val_str(v, a));
        }
    }

    tl_win_vals    = NULL;
    tl_nwin_vals   = 0;
    tl_win_cur_row = -1;
}

/* ── Execute a SELECT statement ── */
static RS *exec_select(Arena *a, const SelectStmt *sel, VTReg *parent_vt) {
    /* Setup VT registry — inherit parent + add CTEs */
    VTReg local_vt={0};
    if (parent_vt) { local_vt=*parent_vt; }
    VTReg *vt=&local_vt;

    /* Execute CTEs */
    for (int i=0;i<sel->nctes;i++) {
        CTE *cte=&sel->ctes[i];
        if (!cte->body) continue;
        Stmt tmp={.type=STMT_SELECT,.select=*cte->body};
        RS *cte_rs=exec_select(a,&tmp.select,vt);
        if (!cte_rs) continue;
        /* build schema for CTE */
        Schema *cte_sc=arena_calloc(a,sizeof(Schema));
        cte_sc->ncols=cte_rs->ncols; cte_sc->cols=arena_alloc(a,(size_t)cte_rs->ncols*sizeof(ColDef));
        for(int c=0;c<cte_rs->ncols;c++){cte_sc->cols[c].name=cte_rs->col_names[c];cte_sc->cols[c].type=COL_TEXT;}
        vt_add(vt,cte->name,cte_rs,cte_sc);
    }

    /* Load tables */
    int ntables=sel->nfrom;
    if (ntables==0) {
        /* SELECT without FROM */
        int nc=sel->nselect;
        char **names=arena_alloc(a,(size_t)nc*sizeof(char*));
        for(int i=0;i<nc;i++) names[i]=arena_strdup(a,expr_name(sel->select_list[i],i,a));
        RS *rs=rs_new(a,nc,names,sel->norder);
        char **cells=arena_alloc(a,(size_t)nc*sizeof(char*));
        JoinCtx ctx={0};
        for(int i=0;i<nc;i++){
            Val v=eval_val(sel->select_list[i],&ctx,a);
            cells[i]=arena_strdup(a,val_str(v,a));
        }
        rs_add(rs,a,cells,NULL);
        return rs;
    }

    IdxHint ih = extract_idx_hint(sel->where);
    TblData *tables=arena_calloc(a,(size_t)ntables*sizeof(TblData));
    for (int i=0;i<ntables;i++) {
        const char *tname=sel->from[i].table;
        const char *alias=sel->from[i].alias;
        if (sel->from[i].subquery) {
            /* derived table: execute subquery */
            RS *sub_rs=exec_stmt(a,sel->from[i].subquery,vt);
            if (!sub_rs) { tables[i].schema=arena_calloc(a,sizeof(Schema)); continue; }
            Schema *sub_sc=arena_calloc(a,sizeof(Schema));
            sub_sc->ncols=sub_rs->ncols;
            sub_sc->cols=arena_alloc(a,(size_t)sub_rs->ncols*sizeof(ColDef));
            for(int c=0;c<sub_rs->ncols;c++){
                sub_sc->cols[c].name=sub_rs->col_names[c];
                sub_sc->cols[c].type=COL_TEXT;
            }
            tables[i].schema=sub_sc;
            tables[i].nrows=sub_rs->nrows;
            tables[i].rows=arena_alloc(a,(size_t)sub_rs->nrows*sizeof(char**));
            for(int r=0;r<sub_rs->nrows;r++) tables[i].rows[r]=sub_rs->rows[r].cells;
            tables[i].tname=alias?alias:"_sub";
            tables[i].alias=alias;
        } else if (tname) {
            if (load_tbl(a,tname,alias,vt,&tables[i],&ih)!=0) {
                /* table not found: empty schema */
                tables[i].schema=arena_calloc(a,sizeof(Schema));
                tables[i].tname=tname; tables[i].alias=alias;
            }
        }
    }

    /* Detect aggregation need */
    bool do_agg=sel->ngroup>0 || sel->having;
    if (!do_agg) {
        for(int i=0;i<sel->nselect;i++) if(expr_has_agg(sel->select_list[i])){do_agg=true;break;}
    }
    bool implicit_agg=(do_agg && sel->ngroup==0 && !sel->having);

    /* Determine output column names */
    bool is_star=(sel->nselect==1 && sel->select_list[0]->type==EXPR_STAR);
    int ncols;
    char **col_names;
    if (is_star) {
        build_star_col_names(a, sel, tables, ntables, &col_names, &ncols);
    } else {
        ncols=sel->nselect;
        col_names=arena_alloc(a,(size_t)ncols*sizeof(char*));
        for(int i=0;i<ncols;i++) col_names[i]=arena_strdup(a,expr_name(sel->select_list[i],i,a));
    }

    RS *rs=rs_new(a,ncols,col_names,sel->norder);

    ExecState st={0};
    st.a=a; st.sel=sel; st.vt=vt; st.rs=rs;
    st.do_agg=do_agg; st.implicit_agg=implicit_agg;
    st.cap_grps=64;
    st.grps=arena_alloc(a,(size_t)st.cap_grps*sizeof(GrpAcc));

    JoinCtx ctx={0};
    join_recurse(&st, 0, &ctx, tables, ntables, false);

    if (do_agg) emit_groups(&st);

    /* ── DISTINCT ── */
    if (sel->distinct && rs->nrows > 0) {
        /* simple O(n²) dedup */
        bool *keep=arena_calloc(a,(size_t)rs->nrows*sizeof(bool));
        for(int i=0;i<rs->nrows;i++){
            keep[i]=true;
            for(int j=0;j<i;j++){
                if(!keep[j]) continue;
                bool eq=true;
                for(int c=0;c<rs->ncols&&eq;c++){
                    const char *ca=rs->rows[i].cells?rs->rows[i].cells[c]:"";
                    const char *cb=rs->rows[j].cells?rs->rows[j].cells[c]:"";
                    if(strcmp(ca,cb)) eq=false;
                }
                if(eq){keep[i]=false;break;}
            }
        }
        int new_n=0;
        for(int i=0;i<rs->nrows;i++) if(keep[i]) rs->rows[new_n++]=rs->rows[i];
        rs->nrows=new_n;
    }

    /* ── Window functions ── */
    {
        bool has_win = false;
        for (int i = 0; i < sel->nselect && !has_win; i++)
            has_win = expr_has_window(sel->select_list[i]);
        if (has_win) apply_windows(a, rs, sel);
    }

    /* ── ORDER BY ── */
    if (sel->norder > 0 && rs->nrows > 1) {
        /* insertion sort for stability on small sets, otherwise bubble */
        for(int i=1;i<rs->nrows;i++){
            OutRow tmp_row=rs->rows[i];
            int j=i-1;
            while(j>=0){
                int cmp=0;
                for(int o=0;o<sel->norder&&cmp==0;o++){
                    const char *ka=rs->rows[j].skeys?rs->rows[j].skeys[o]:"";
                    const char *kb=tmp_row.skeys?tmp_row.skeys[o]:"";
                    double na,nb;
                    bool an=pnum(ka,&na),bn=pnum(kb,&nb);
                    if(an&&bn) cmp=(na<nb)?-1:(na>nb?1:0);
                    else cmp=strcmp(ka,kb);
                    if(sel->order_by[o].desc) cmp=-cmp;
                }
                if(cmp<=0) break;
                rs->rows[j+1]=rs->rows[j]; j--;
            }
            rs->rows[j+1]=tmp_row;
        }
    }

    /* ── LIMIT / OFFSET ── */
    int64_t off=sel->offset>0?sel->offset:0;
    int64_t lim=sel->limit;
    if (off > 0 || lim >= 0) {
        int start=(int)off; if(start>rs->nrows) start=rs->nrows;
        int end_idx=rs->nrows;
        if(lim>=0 && start+lim<end_idx) end_idx=(int)(start+lim);
        int new_n=0;
        for(int i=start;i<end_idx;i++) rs->rows[new_n++]=rs->rows[i];
        rs->nrows=new_n;
    }

    return rs;
}

/* ── Execute a statement (handles SET_OP) ── */
static RS *exec_stmt(Arena *a, const Stmt *s, VTReg *vt) {
    if (!s) return NULL;
    if (s->type == STMT_SELECT) return exec_select(a, &s->select, vt);
    if (s->type == STMT_SET_OP) {
        RS *L=exec_stmt(a,s->set_left,vt);
        RS *R=exec_stmt(a,s->set_right,vt);
        if (!L) return R;
        if (!R) return L;
        int nc=L->ncols;
        RS *out=rs_new(a,nc,L->col_names,0);

        if (s->set_op==SET_UNION_ALL) {
            for(int i=0;i<L->nrows;i++) rs_add(out,a,L->rows[i].cells,NULL);
            for(int i=0;i<R->nrows;i++) rs_add(out,a,R->rows[i].cells,NULL);
        } else if (s->set_op==SET_UNION) {
            /* deduplicate combined L ∪ R result */
            for(int i=0;i<L->nrows;i++){
                bool dup=false;
                for(int j=0;j<out->nrows&&!dup;j++){
                    bool eq=true;
                    for(int c=0;c<nc&&eq;c++){
                        const char *ca=out->rows[j].cells?out->rows[j].cells[c]:"";
                        const char *cb=L->rows[i].cells?L->rows[i].cells[c]:"";
                        if(strcmp(ca,cb)) eq=false;
                    }
                    if(eq) dup=true;
                }
                if(!dup) rs_add(out,a,L->rows[i].cells,NULL);
            }
            for(int i=0;i<R->nrows;i++){
                bool dup=false;
                for(int j=0;j<out->nrows&&!dup;j++){
                    bool eq=true;
                    for(int c=0;c<nc&&eq;c++){
                        const char *ca=out->rows[j].cells?out->rows[j].cells[c]:"";
                        const char *cb=R->rows[i].cells?R->rows[i].cells[c]:"";
                        if(strcmp(ca,cb)) eq=false;
                    }
                    if(eq) dup=true;
                }
                if(!dup) rs_add(out,a,R->rows[i].cells,NULL);
            }
        } else if (s->set_op==SET_INTERSECT) {
            for(int i=0;i<L->nrows;i++){
                for(int j=0;j<R->nrows;j++){
                    bool eq=true;
                    for(int c=0;c<nc&&eq;c++){
                        const char *ca=L->rows[i].cells?L->rows[i].cells[c]:"";
                        const char *cb=R->rows[j].cells?R->rows[j].cells[c]:"";
                        if(strcmp(ca,cb)) eq=false;
                    }
                    if(eq){rs_add(out,a,L->rows[i].cells,NULL);break;}
                }
            }
        } else { /* EXCEPT */
            for(int i=0;i<L->nrows;i++){
                bool found=false;
                for(int j=0;j<R->nrows&&!found;j++){
                    bool eq=true;
                    for(int c=0;c<nc&&eq;c++){
                        const char *ca=L->rows[i].cells?L->rows[i].cells[c]:"";
                        const char *cb=R->rows[j].cells?R->rows[j].cells[c]:"";
                        if(strcmp(ca,cb)) eq=false;
                    }
                    if(eq) found=true;
                }
                if(!found) rs_add(out,a,L->rows[i].cells,NULL);
            }
        }
        return out;
    }

    /* ── DML: DELETE / UPDATE ── */
    if (s->type == STMT_DELETE || s->type == STMT_UPDATE) {
        const char *tname = s->dml.table;
        if (!tname || !*tname) return NULL;

        Schema *sc = NULL;
        if (catalog_get_schema(g_app.catalog, tname, &sc, a) != 0 || !sc) return NULL;
        int ncols = sc->ncols;

        char wal_path[1024], dir_path[1024];
        snprintf(dir_path, sizeof(dir_path), "%s/%s", g_app.data_dir, tname);
        snprintf(wal_path, sizeof(wal_path), "%s/wal.bin", dir_path);

        /* Get the open Table handle for writing tombstones */
        pthread_mutex_lock(&g_app.tables_mu);
        Table *tbl = hm_get(&g_app.tables, tname);
        pthread_mutex_unlock(&g_app.tables_mu);
        if (!tbl) return NULL;

        /* Scan WAL for matching rows */
        FILE *rf = fopen(wal_path, "rb");
        if (!rf) return NULL;

        int affected = 0;
        int64_t file_off = 0;
        char line[262144];

        while (1) {
            uint32_t l = 0;
            if (fread(&l, 1, 4, rf) != 4) break;
            int64_t rec_off = file_off;
            file_off += 4 + (int64_t)l;
            if (l == 0 || l >= sizeof(line)) { fseek(rf, (long)l, SEEK_CUR); continue; }
            if (fread(line, 1, l, rf) != l) break;
            line[l] = '\0';

            /* Skip tombstone records */
            uint8_t op = (uint8_t)(unsigned char)line[0];
            if (op == WAL_OP_DELETE || op == WAL_OP_UPDATE) continue;

            /* Strip trailing newline */
            size_t rl = strlen(line);
            while (rl > 0 && (line[rl-1]=='\n'||line[rl-1]=='\r')) line[--rl] = '\0';
            int printable = 0;
            for (size_t ci = 0; ci < rl; ci++) if ((unsigned char)line[ci] >= 0x20) printable++;
            if (printable < 2) continue;

            /* Parse CSV row */
            char *vals[MAX_COLS] = {0}; int nv = 0;
            char row_copy[262144];
            strncpy(row_copy, line, sizeof(row_copy)-1);
            row_copy[sizeof(row_copy)-1] = '\0';
            split_line_simple(row_copy, ',', vals, MAX_COLS, &nv);
            char **cells = arena_alloc(a, (size_t)ncols * sizeof(char*));
            for (int i = 0; i < ncols; i++) cells[i] = (i < nv && vals[i]) ? vals[i] : "";

            /* Evaluate WHERE */
            if (s->dml.where) {
                JoinCtx ctx = {0};
                ctx.n = 1;
                ctx.schemas[0] = sc;
                ctx.rows[0] = cells;
                ctx.tnames[0] = tname;
                ctx.aliases[0] = tname;
                Val cond = eval_val(s->dml.where, &ctx, a);
                bool pass = cond.is_null ? false : cond.is_bool ? cond.b : cond.is_num ? (cond.num != 0.0) : (cond.str && *cond.str);
                if (!pass) continue;
            }

            /* Row matches */
            if (s->type == STMT_DELETE) {
                pthread_mutex_lock(&g_app.tables_mu);
                table_delete(tbl, rec_off);
                pthread_mutex_unlock(&g_app.tables_mu);
                affected++;
            } else {
                /* UPDATE: apply SET assignments to build new CSV row */
                char **new_cells = arena_alloc(a, (size_t)ncols * sizeof(char*));
                for (int i = 0; i < ncols; i++) new_cells[i] = cells[i];
                for (int si = 0; si < s->dml.nset; si++) {
                    for (int ci = 0; ci < ncols; ci++) {
                        if (strcasecmp(sc->cols[ci].name, s->dml.set_cols[si]) == 0) {
                            new_cells[ci] = s->dml.set_vals[si];
                            break;
                        }
                    }
                }
                /* Serialize new row to CSV */
                char new_csv[262144]; int off = 0;
                for (int ci = 0; ci < ncols; ci++) {
                    if (ci) new_csv[off++] = ',';
                    const char *v = new_cells[ci] ? new_cells[ci] : "";
                    int n = snprintf(new_csv+off, sizeof(new_csv)-off-2, "%s", v);
                    if (n > 0) off += n;
                }
                new_csv[off++] = '\n';
                pthread_mutex_lock(&g_app.tables_mu);
                table_update(tbl, rec_off, new_csv, (size_t)off);
                pthread_mutex_unlock(&g_app.tables_mu);
                affected++;
            }
        }
        fclose(rf);

        /* Return result */
        const char *col_key = (s->type == STMT_DELETE) ? "deleted" : "updated";
        char **names = arena_alloc(a, sizeof(char*));
        names[0] = arena_strdup(a, col_key);
        RS *rs = rs_new(a, 1, names, 0);
        char **cells = arena_alloc(a, sizeof(char*));
        cells[0] = arena_sprintf(a, "%d", affected);
        rs_add(rs, a, cells, NULL);
        return rs;
    }

    return NULL;
}

/* ── GET /health ── */
static void h_health(HttpReq *req, HttpResp *resp) {
    (void)req;
    Arena *a = arena_create(4096);
    char *metrics_json = metrics_to_json(g_app.metrics, a);
    JBuf jb; jb_init(&jb, a, 512);
    jb_obj_begin(&jb);
    jb_key(&jb,"status"); jb_str(&jb,"ok");
    jb_key(&jb,"version"); jb_str(&jb,"1.0.0");
    jb_key(&jb,"metrics"); jb_raw(&jb, metrics_json);
    jb_obj_end(&jb);
    const char *body = jb_done(&jb);
    resp->status = 200; resp->content_type = "application/json";
    resp->body = body; resp->body_len = strlen(body);
}

/* ── GET /api/tables ── */
static void h_tables_list(HttpReq *req, HttpResp *resp) {
    (void)req;
    Arena *a = arena_create(16384);
    char *json = NULL;
    catalog_list_tables_full(g_app.catalog, &json, a);
    http_resp_json(resp, 200, json ? json : "[]");
}

/* ── POST /api/tables/query ── */
static void h_query(HttpReq *req, HttpResp *resp) {
    if (!req->body || req->body_len == 0) { http_resp_error(resp,400,"empty body"); return; }
    Arena *a = arena_create(1048576); /* 1 MiB arena */
    JVal *root = json_parse(a, req->body, req->body_len);
    const char *sql = json_str(json_get(root,"sql"),"");
    if (!*sql) { http_resp_error(resp,400,"missing sql"); arena_destroy(a); return; }

    /* optional limit override from request */
    int64_t req_limit = json_int(json_get(root,"limit"), -1);

    int64_t t0 = (int64_t)clock();
    Stmt *stmt = sql_parse(a, sql, strlen(sql));
    if (stmt->error) { http_resp_error(resp,400,stmt->error); arena_destroy(a); return; }

    /* DML requires at least ROLE_ANALYST */
    if (stmt->type == STMT_DELETE || stmt->type == STMT_UPDATE) {
        if (g_app.auth_enabled && req->auth.role == ROLE_VIEWER) {
            http_resp_error(resp, 403, "forbidden: DML requires analyst or admin role");
            arena_destroy(a); return;
        }
    }

    /* Apply request-level limit cap */
    if (stmt->type == STMT_SELECT) {
        int64_t cap = (req_limit > 0 && req_limit < 10000) ? req_limit : 10000;
        if (stmt->select.limit < 0 || stmt->select.limit > cap) stmt->select.limit = cap;
    }

    RS *rs = exec_stmt(a, stmt, NULL);

    JBuf jb; jb_init(&jb, a, 65536);
    jb_obj_begin(&jb);

    /* columns array */
    jb_key(&jb,"columns"); jb_arr_begin(&jb);
    if (rs) for(int i=0;i<rs->ncols;i++) jb_str(&jb, rs->col_names[i]?rs->col_names[i]:"");
    jb_arr_end(&jb);

    /* rows array */
    jb_key(&jb,"rows"); jb_arr_begin(&jb);
    if (rs) {
        for(int r=0;r<rs->nrows;r++){
            jb_arr_begin(&jb);
            for(int c=0;c<rs->ncols;c++){
                const char *v=rs->rows[r].cells?rs->rows[r].cells[c]:"";
                jb_str(&jb, v?v:"");
            }
            jb_arr_end(&jb);
        }
    }
    jb_arr_end(&jb);

    int64_t t1=(int64_t)clock();
    double ms=(double)(t1-t0)*1000.0/CLOCKS_PER_SEC;
    jb_key(&jb,"elapsed_ms"); jb_double(&jb,ms);
    jb_key(&jb,"row_count");  jb_int(&jb, rs?rs->nrows:0);
    jb_obj_end(&jb);

    metrics_push(&g_app.metrics->query_latency_ms, ms);
    g_app.metrics->total_queries++;

    const char *body=jb_done(&jb);
    http_resp_json(resp,200,body);
    /* arena 'a' kept alive: body lives in it until response is sent */
}

/* ── GET /api/tables/:name/schema ── */
static void h_table_schema(HttpReq *req, HttpResp *resp) {
    const char *name=hm_get(&req->params,"name");
    if(!name){http_resp_error(resp,400,"missing name");return;}
    Arena *a=arena_create(4096);
    Schema *schema=NULL;
    if(catalog_get_schema(g_app.catalog,name,&schema,a)<0){
        http_resp_error(resp,404,"table not found");arena_destroy(a);return;
    }
    JBuf jb; jb_init(&jb,a,1024);
    jb_obj_begin(&jb);
    jb_key(&jb,"table"); jb_str(&jb,name);
    jb_key(&jb,"columns"); jb_arr_begin(&jb);
    for(int i=0;i<schema->ncols;i++){
        jb_obj_begin(&jb);
        jb_key(&jb,"name"); jb_str(&jb,schema->cols[i].name);
        const char *tp="text";
        if(schema->cols[i].type==COL_INT64)  tp="int64";
        if(schema->cols[i].type==COL_DOUBLE) tp="double";
        if(schema->cols[i].type==COL_BOOL)   tp="bool";
        jb_key(&jb,"type"); jb_str(&jb,tp);
        jb_key(&jb,"nullable"); jb_bool(&jb,schema->cols[i].nullable);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);
    jb_obj_end(&jb);
    http_resp_json(resp,200,jb_done(&jb));
}

/* ── DELETE /api/tables/:name ── */
static void h_table_delete(HttpReq *req, HttpResp *resp) {
    const char *name = hm_get(&req->params, "name");
    if (!name || !*name) { http_resp_error(resp, 400, "missing name"); return; }
    if (!valid_table_name(name)) { http_resp_error(resp, 400, "invalid table name"); return; }

    pthread_mutex_lock(&g_app.tables_mu);
    Table *t = hm_get(&g_app.tables, name);
    if (t) { table_close(t); hm_del(&g_app.tables, name); }
    pthread_mutex_unlock(&g_app.tables_mu);

    catalog_drop_table(g_app.catalog, name);
    char wal_path[1024], dir_path[1024];
    snprintf(dir_path, sizeof(dir_path), "%s/%s", g_app.data_dir, name);
    snprintf(wal_path, sizeof(wal_path), "%s/wal.bin", dir_path);
    unlink(wal_path); rmdir(dir_path);

    Arena *ba = arena_create(256);
    char *msg = arena_sprintf(ba, "{\"event\":\"table_deleted\",\"table\":\"%s\"}", name);
    app_ws_broadcast(&g_app, msg);
    arena_destroy(ba);
    http_resp_json(resp, 200, "{\"status\":\"deleted\"}");
}

/* forward declaration — определение ниже */
static Table *recreate_table(App *app, Arena *a, const char *tname, Schema *schema);

/* ── POST /api/ingest/csv ── */
static void h_ingest_csv(HttpReq *req, HttpResp *resp) {
    const char *table_name=hm_get(&req->params,"table");
    if(!table_name) table_name=hm_get(&req->params,"name");
    const char *qs=req->query;
    char tname[128]="uploaded";
    if(qs){const char*p=strstr(qs,"table=");if(p)sscanf(p+6,"%127[^&]",tname);table_name=tname;}

    if (!valid_table_name(tname)) { http_resp_error(resp,400,"invalid table name"); return; }
    if(!req->body||req->body_len==0){http_resp_error(resp,400,"empty body");return;}

    char tmp_path[256]; snprintf(tmp_path,sizeof(tmp_path),"/tmp/dfo_upload_%lld.csv",(long long)time(NULL));
    FILE *f=fopen(tmp_path,"w"); if(!f){http_resp_error(resp,500,"can't write tmp");return;}
    fwrite(req->body,1,req->body_len,f); fclose(f);

    Arena *a=arena_create(131072);
    const char *body=req->body;
    const char *nl=memchr(body,'\n',req->body_len);
    if(!nl){http_resp_error(resp,400,"no header");arena_destroy(a);return;}

    int ncols=0; char *header=arena_strndup(a,body,(size_t)(nl-body));
    char delim = detect_delim(body, (size_t)(nl-body));
    for(char*p=header;*p;p++) if(*p==delim) ncols++;
    ncols++;
    if (ncols > MAX_COLS) { arena_destroy(a); http_resp_error(resp, 400, "too many columns"); return; }

    Schema *schema=arena_calloc(a,sizeof(Schema));
    schema->ncols=ncols; schema->cols=arena_alloc(a,ncols*sizeof(ColDef));
    char dstr[2] = {delim, '\0'};
    char *tok=strtok(header,dstr);
    for(int i=0;i<ncols&&tok;i++){
        size_t tl=strlen(tok); while(tl>0&&(tok[tl-1]=='\r'||tok[tl-1]==' ')) tok[--tl]='\0';
        schema->cols[i].name=arena_strdup(a,tok);
        schema->cols[i].type=COL_TEXT; schema->cols[i].nullable=true;
        tok=strtok(NULL,dstr);
    }

    pthread_mutex_lock(&g_app.tables_mu);
    Table *t=hm_get(&g_app.tables,tname);
    if(!t){
        t=table_create(tname,schema,g_app.data_dir);
        hm_set(&g_app.tables,tname,t);
        catalog_register_table(g_app.catalog,tname,schema);
    }
    pthread_mutex_unlock(&g_app.tables_mu);

    int row_count = 0;
    const char *p = nl + 1;
    char ***col_vals = arena_alloc(a, (size_t)ncols * sizeof(char **));
    for (int c = 0; c < ncols; c++) col_vals[c] = arena_alloc(a, BATCH_SIZE * sizeof(char *));
    ColBatch batch = {0}; batch.schema = schema; batch.ncols = ncols;
    for (int c = 0; c < ncols; c++) batch.values[c] = col_vals[c];
    char *row_copy = arena_alloc(a, 65536);

    while (p < body + req->body_len) {
        const char *ne = memchr(p, '\n', (size_t)(body + req->body_len - p));
        if (!ne) ne = body + req->body_len;
        size_t rlen = (size_t)(ne - p);
        if (rlen == 0 || (rlen == 1 && p[0] == '\r')) { p = ne + 1; continue; }
        if (rlen >= 65536) rlen = 65535;
        memcpy(row_copy, p, rlen);
        if (rlen > 0 && row_copy[rlen-1] == '\r') rlen--;
        row_copy[rlen] = '\0';
        int col = 0; char *cell = strtok(row_copy, dstr);
        while (cell && col < ncols) { col_vals[col][batch.nrows] = arena_strdup(a, cell); col++; cell = strtok(NULL, dstr); }
        for (; col < ncols; col++) col_vals[col][batch.nrows] = arena_strdup(a, "");
        batch.nrows++; row_count++;
        if (batch.nrows == BATCH_SIZE) {
            if (table_append(t, &batch) != 0) { arena_destroy(a); http_resp_error(resp, 500, "ingest write failed"); return; }
            batch.nrows = 0;
        }
        p = ne + 1;
    }
    if (batch.nrows > 0) {
        if (table_append(t, &batch) != 0) { arena_destroy(a); http_resp_error(resp, 500, "ingest write failed"); return; }
    }

    metrics_push(&g_app.metrics->rows_ingested,(double)row_count);
    g_app.metrics->total_rows+=row_count;
    catalog_update_table_meta(g_app.catalog, tname, "ingest", (int64_t)row_count);
    unlink(tmp_path);

    Arena *ba=arena_create(256);
    char *msg=arena_sprintf(ba,"{\"event\":\"table_updated\",\"table\":\"%s\",\"rows\":%d}",tname,row_count);
    app_ws_broadcast(&g_app,msg); arena_destroy(ba);

    Arena *ra=arena_create(256);
    JBuf jb; jb_init(&jb,ra,256);
    jb_obj_begin(&jb);
    jb_key(&jb,"table");  jb_str(&jb,tname);
    jb_key(&jb,"rows");   jb_int(&jb,row_count);
    jb_key(&jb,"columns");jb_int(&jb,ncols);
    jb_key(&jb,"status"); jb_str(&jb,"ok");
    jb_obj_end(&jb);
    http_resp_json(resp,200,jb_done(&jb));
}

/* ── POST /api/ingest/parquet?table=X ── */
static void h_ingest_parquet(HttpReq *req, HttpResp *resp) {
    const char *qs=req->query;
    char tname[128]="uploaded";
    if (qs) { const char *p=strstr(qs,"table="); if(p) sscanf(p+6,"%127[^&]",tname); }

    if (!valid_table_name(tname)) { http_resp_error(resp,400,"invalid table name"); return; }
    if (!req->body||req->body_len==0) { http_resp_error(resp,400,"empty body"); return; }

    /* Сохраняем тело во временный файл */
    char tmp_path[256];
    snprintf(tmp_path,sizeof(tmp_path),"/tmp/dfo_pq_%lld.parquet",(long long)time(NULL));
    FILE *f=fopen(tmp_path,"wb");
    if (!f) { http_resp_error(resp,500,"can't write tmp"); return; }
    fwrite(req->body,1,req->body_len,f); fclose(f);

    /* Конфиг коннектора: путь к только что записанному файлу */
    char cfg[600]; snprintf(cfg,sizeof(cfg),"{\"path\":\"%s\"}",tmp_path);
    char so_path[512];
    snprintf(so_path,sizeof(so_path),"%s/parquet_connector.so",g_app.plugins_dir);

    Arena *a=arena_create(4194304);
    ConnectorInst *inst=connector_load(so_path,cfg,a);
    if (!inst) {
        remove(tmp_path); arena_destroy(a);
        http_resp_error(resp,500,"parquet connector not found"); return;
    }
    const DfoConnector *api=connector_api(inst);
    void *ctx=connector_ctx(inst);

    int total_rows=0; Table *t=NULL; bool table_created=false;
    char cursor_buf[32]="0";

    for (;;) {
        DfoReadReq rreq={ .cursor=cursor_buf, .limit=BATCH_SIZE };
        ColBatch *batch=NULL;
        if (api->read_batch(ctx,a,&rreq,"",&batch)!=0||!batch||batch->nrows==0) break;

        if (!table_created) {
            t=recreate_table(&g_app,a,tname,batch->schema);
            catalog_register_table(g_app.catalog,tname,batch->schema);
            table_created=true;
        }
        table_append(t,batch);
        total_rows+=batch->nrows;
        snprintf(cursor_buf,sizeof(cursor_buf),"%d",total_rows);
        if (batch->nrows<BATCH_SIZE) break;
    }

    connector_unload(inst);
    arena_destroy(a);
    remove(tmp_path);

    Arena *ra=arena_create(256);
    JBuf jb; jb_init(&jb,ra,256);
    jb_obj_begin(&jb);
    jb_key(&jb,"table");  jb_str(&jb,tname);
    jb_key(&jb,"rows");   jb_int(&jb,total_rows);
    jb_key(&jb,"status"); jb_str(&jb,"ok");
    jb_obj_end(&jb);
    http_resp_json(resp,200,jb_done(&jb));
}

/* ── GET /api/pipelines ── */
static void h_pipelines_list(HttpReq *req, HttpResp *resp) {
    (void)req;
    Arena *a=arena_create(8192);
    JBuf jb; jb_init(&jb,a,1024);
    jb_arr_begin(&jb);
    pthread_mutex_lock(&g_app.scheduler->mu);
    for(int i=0;i<g_app.scheduler->npipelines;i++){
        char *pj=pipeline_to_json(&g_app.scheduler->pipelines[i],a);
        jb_raw(&jb,pj);
    }
    pthread_mutex_unlock(&g_app.scheduler->mu);
    jb_arr_end(&jb);
    http_resp_json(resp,200,jb_done(&jb));
}

/* ── POST /api/pipelines ── */
static void h_pipeline_create(HttpReq *req, HttpResp *resp) {
    if(!req->body||!req->body_len){http_resp_error(resp,400,"empty body");return;}
    Pipeline p; memset(&p,0,sizeof(p));
    if(pipeline_from_json(&p,req->body)<0){http_resp_error(resp,400,"invalid pipeline json");return;}
    if(!p.id[0]) {
        static volatile int _pid_seq = 0;
        int seq = __atomic_fetch_add(&_pid_seq, 1, __ATOMIC_RELAXED);
        snprintf(p.id,sizeof(p.id),"p_%lld_%d",(long long)time(NULL), seq);
    }
    scheduler_add(g_app.scheduler,&p);
    Arena *a=arena_create(4096);
    char *pj=pipeline_to_json(&p,a);
    catalog_save_pipeline(g_app.catalog,p.id,pj);
    http_resp_json(resp,201,pj);
    Arena *ba=arena_create(256);
    app_ws_broadcast(&g_app,arena_sprintf(ba,"{\"event\":\"pipeline_created\",\"id\":\"%s\"}",p.id));
    arena_destroy(ba);
}

/* ── GET /api/pipelines/:id ── */
static void h_pipeline_get(HttpReq *req, HttpResp *resp) {
    const char *id=hm_get(&req->params,"id");
    if(!id){http_resp_error(resp,400,"missing id");return;}
    Pipeline *p=scheduler_find(g_app.scheduler,id);
    if(!p){
        Arena *a=arena_create(4096); char *json=NULL;
        if(catalog_load_pipeline(g_app.catalog,id,&json,a)==0) http_resp_json(resp,200,json);
        else http_resp_error(resp,404,"not found");
        arena_destroy(a); return;
    }
    Arena *a=arena_create(4096);
    http_resp_json(resp,200,pipeline_to_json(p,a));
}

/* ── Drop and recreate a target table, return open Table* ── */
static Table *recreate_table(App *app, Arena *a, const char *tname, Schema *schema) {
    pthread_mutex_lock(&app->tables_mu);
    Table *old = hm_get(&app->tables, tname);
    if (old) { table_close(old); hm_del(&app->tables, tname); }
    pthread_mutex_unlock(&app->tables_mu);

    catalog_drop_table(app->catalog, tname);
    char wp[1024], dp[1024];
    snprintf(dp, sizeof(dp), "%s/%s", app->data_dir, tname);
    snprintf(wp, sizeof(wp), "%s/wal.bin", dp);
    unlink(wp); rmdir(dp);

    pthread_mutex_lock(&app->tables_mu);
    Table *t = table_create(tname, schema, app->data_dir);
    hm_set(&app->tables, tname, t);
    pthread_mutex_unlock(&app->tables_mu);
    catalog_register_table(app->catalog, tname, schema);
    (void)a;
    return t;
}

/* ── Write an RS (result set) into a Table ── */
static int write_rs_to_table(App *app, Arena *a, const char *tname, RS *rs) {
    if (!rs || rs->ncols == 0) return 0;

    Schema *schema = arena_calloc(a, sizeof(Schema));
    schema->ncols = rs->ncols;
    schema->cols  = arena_alloc(a, (size_t)rs->ncols * sizeof(ColDef));
    for (int c = 0; c < rs->ncols; c++) {
        schema->cols[c].name     = rs->col_names[c] ? rs->col_names[c] : "col";
        schema->cols[c].type     = COL_TEXT;
        schema->cols[c].nullable = true;
    }
    Table *t = recreate_table(app, a, tname, schema);

    char ***cv = arena_alloc(a, (size_t)rs->ncols * sizeof(char **));
    for (int c = 0; c < rs->ncols; c++)
        cv[c] = arena_alloc(a, BATCH_SIZE * sizeof(char *));
    ColBatch batch = {0};
    batch.schema = schema; batch.ncols = rs->ncols;
    for (int c = 0; c < rs->ncols; c++) batch.values[c] = cv[c];

    int total = 0;
    for (int r = 0; r < rs->nrows; r++) {
        for (int c = 0; c < rs->ncols; c++) {
            const char *v = rs->rows[r].cells ? rs->rows[r].cells[c] : "";
            cv[c][batch.nrows] = (char *)(v ? v : "");
        }
        if (++batch.nrows == BATCH_SIZE) { table_append(t, &batch); batch.nrows = 0; }
        total++;
    }
    if (batch.nrows > 0) table_append(t, &batch);
    catalog_update_table_meta(app->catalog, tname, "pipeline", (int64_t)rs->nrows);
    return total;
}

static void send_pipeline_alert(Pipeline *p, const char *message, bool success) {
    if (!p || !p->webhook_url[0]) return;
    if (p->alert_cooldown > 0 && time(NULL) - p->last_alert_at < p->alert_cooldown) return;

    bool should_send = false;
    if (strcmp(p->webhook_on, "all") == 0) should_send = true;
    else if (strcmp(p->webhook_on, "success") == 0) should_send = success;
    else should_send = !success;
    if (!should_send) return;

    char body[1024];
    if (success) {
        snprintf(body, sizeof(body), "{\"text\":\"Pipeline *%s* succeeded\"}", p->name[0] ? p->name : p->id);
    } else {
        snprintf(body, sizeof(body), "{\"text\":\"Pipeline *%s* failed: %s\"}",
                 p->name[0] ? p->name : p->id,
                 message ? message : "unknown error");
    }

    bool sent = http_post_json(p->webhook_url, body, 5000) == 0;
    if (!sent) {
        LOG_WARN("alert webhook failed for pipeline %s", p->id);
    } else {
        p->last_alert_at = time(NULL);
    }
}

/* ── Run one connector step: pull all batches into target_table ── */
static int run_connector_step(App *app, Arena *a, PipelineStep *st, char *errbuf, size_t errsz) {
    char so_path[1024];
    snprintf(so_path, sizeof(so_path), "%s/%s_connector.so",
             app->plugins_dir, st->connector_type);

    ConnectorInst *inst = connector_load(so_path, st->connector_config, a);
    if (!inst) {
        snprintf(errbuf, errsz, "connector_load(%s) failed", so_path);
        return -1;
    }
    const DfoConnector *api = connector_api(inst);
    void *ctx = connector_ctx(inst);

    /* Determine entity name: if transform_sql is a table name (no spaces), use it;
     * otherwise pass the full SQL as the filter for the connector to execute */
    const char *entity = st->transform_sql[0] ? st->transform_sql : "";
    const char *filter = NULL;
    /* If the SQL contains spaces it's a query, not a bare table name */
    if (strchr(entity, ' ')) { filter = entity; entity = ""; }

    /* Describe schema to create the target table correctly */
    Schema *schema = NULL;
    if (!filter && entity[0] && api->describe) {
        api->describe(ctx, a, entity, &schema);
    }

    /* Create target table (schema may be NULL — will be set from first batch) */
    Table *t = NULL;
    bool table_created = false;

    int total_rows = 0;
    char cursor_buf[32] = "0";

    for (;;) {
        DfoReadReq req = { .cursor = cursor_buf, .limit = BATCH_SIZE, .filter = filter };
        ColBatch *batch = NULL;
        if (api->read_batch(ctx, a, &req, entity, &batch) != 0 || !batch || batch->nrows == 0)
            break;

        /* Create the table on first non-empty batch */
        if (!table_created) {
            Schema *sc = batch->schema ? batch->schema : schema;
            if (!sc) {
                /* Build a minimal text schema from batch metadata */
                sc = arena_calloc(a, sizeof(Schema));
                sc->ncols = batch->ncols;
                sc->cols  = arena_alloc(a, (size_t)batch->ncols * sizeof(ColDef));
                for (int c = 0; c < batch->ncols; c++) {
                    sc->cols[c].name     = arena_sprintf(a, "col%d", c);
                    sc->cols[c].type     = COL_TEXT;
                    sc->cols[c].nullable = true;
                }
            }
            t = recreate_table(app, a, st->target_table, sc);
            table_created = true;
        }

        table_append(t, batch);
        total_rows += batch->nrows;

        /* Advance cursor */
        snprintf(cursor_buf, sizeof(cursor_buf), "%d", total_rows);

        /* If batch was smaller than requested, we're done */
        if (batch->nrows < BATCH_SIZE) break;
    }

    if (table_created)
        catalog_update_table_meta(app->catalog, st->target_table, st->connector_type, (int64_t)total_rows);

    connector_unload(inst);
    LOG_INFO("connector step '%s' → %s: %d rows", st->connector_type, st->target_table, total_rows);
    return total_rows;
}

/* ── Execute all steps of a pipeline ── */
void pipeline_execute_steps(Pipeline *p, App *app) {
    Arena *a = arena_create(4194304); /* 4 MiB */
    int64_t started = (int64_t)time(NULL);
    int total_rows = 0;
    const char *run_error = NULL;
    int total_retries = 0;

    for (int si = 0; si < p->nsteps; si++) {
        PipelineStep *st = &p->steps[si];
        st->retry_count = 0;
        st->retry_after = 0;

        while (true) {
            st->status = STEP_RUNNING;

            if (st->connector_type[0]) {
                int n = run_connector_step(app, a, st, p->error_msg, sizeof(p->error_msg));
                if (n < 0) {
                    st->status = STEP_FAILED;
                } else {
                    total_rows += n;
                    st->status = STEP_SUCCESS;
                }
            } else if (!st->transform_sql[0]) {
                st->status = STEP_SUCCESS;
            } else {
                Stmt *stmt = sql_parse(a, st->transform_sql, strlen(st->transform_sql));
                if (stmt->error) {
                    st->status = STEP_FAILED;
                    snprintf(p->error_msg, sizeof(p->error_msg), "step[%d] parse: %s", si, stmt->error);
                } else {
                    RS *rs = exec_stmt(a, stmt, NULL);
                    if (!rs) {
                        st->status = STEP_FAILED;
                        snprintf(p->error_msg, sizeof(p->error_msg), "step[%d]: execution returned null", si);
                    } else {
                        if (st->target_table[0])
                            total_rows += write_rs_to_table(app, a, st->target_table, rs);
                        st->status = STEP_SUCCESS;
                    }
                }
            }

            if (st->status == STEP_SUCCESS) {
                st->retry_count = 0;
                st->retry_after = 0;
                break;
            }

            if (st->retry_count >= st->max_retries) {
                p->run_status = RUN_FAILED;
                run_error = p->error_msg[0] ? p->error_msg : "step failed";
                goto done;
            }

            st->retry_count++;
            total_retries += 1;
            int delay = st->retry_delay_sec * (1 << (st->retry_count - 1));
            st->retry_after = (int64_t)time(NULL) + delay;
            st->status = STEP_PENDING;
            LOG_WARN("step %s: retry %d/%d in %ds", st->id, st->retry_count, st->max_retries, delay);
            sleep(delay);
        }
    }
    p->run_status = RUN_SUCCESS;

done:
    catalog_log_run(app->catalog, p->id, started, (int64_t)time(NULL),
                    p->run_status == RUN_SUCCESS ? 0 : 1, run_error, total_retries);
    send_pipeline_alert(p, run_error, p->run_status == RUN_SUCCESS);
    Arena *ba = arena_create(512);
    app_ws_broadcast(app, arena_sprintf(ba,
        "{\"event\":\"pipeline_done\",\"id\":\"%s\",\"status\":\"%s\",\"rows_written\":%d}",
        p->id, p->run_status == RUN_SUCCESS ? "success" : "failed", total_rows));
    arena_destroy(ba);
    arena_destroy(a);
}

/* ── POST /api/pipelines/:id/run ── */
static void h_pipeline_run(HttpReq *req, HttpResp *resp) {
    const char *id=hm_get(&req->params,"id");
    if(!id){http_resp_error(resp,400,"missing id");return;}
    Pipeline *p=scheduler_find(g_app.scheduler,id);
    if(!p){http_resp_error(resp,404,"not found");return;}
    if(p->run_status==RUN_RUNNING){http_resp_error(resp,409,"already running");return;}
    p->run_status=RUN_RUNNING; p->last_run=(int64_t)time(NULL);
    g_app.metrics->total_pipelines_run++;
    Arena *ba=arena_create(256);
    app_ws_broadcast(&g_app,arena_sprintf(ba,"{\"event\":\"pipeline_run_started\",\"id\":\"%s\"}",id));
    arena_destroy(ba);
    pipeline_execute_steps(p, &g_app);
    http_resp_json(resp,200,"{\"status\":\"triggered\"}");
}

/* ── DELETE /api/pipelines/:id ── */
static void h_pipeline_delete(HttpReq *req, HttpResp *resp) {
    const char *id=hm_get(&req->params,"id");
    if(!id){http_resp_error(resp,400,"missing id");return;}
    scheduler_remove(g_app.scheduler,id); catalog_delete_pipeline(g_app.catalog,id);
    http_resp_json(resp,200,"{\"status\":\"deleted\"}");
}

/* ── GET /api/pipelines/:id/runs ── */
static void h_pipeline_runs(HttpReq *req, HttpResp *resp) {
    const char *id=hm_get(&req->params,"id");
    if(!id){http_resp_error(resp,400,"missing id");return;}
    Arena *a=arena_create(8192); char *json=NULL;
    catalog_list_runs(g_app.catalog,id,&json,a);
    http_resp_json(resp,200,json?json:"[]");
}

/* ── GET /api/metrics ── */
static void h_metrics(HttpReq *req, HttpResp *resp) {
    (void)req;
    Arena *a=arena_create(2048);
    http_resp_json(resp,200,metrics_to_json(g_app.metrics,a));
}

/* ── JVal serializer helper ── */
static void jval_serialize(JBuf *jb, JVal *v) {
    if (!v) { jb_null(jb); return; }
    switch (v->type) {
        case JV_NULL:   jb_null(jb); break;
        case JV_BOOL:   jb_bool(jb, v->b); break;
        case JV_NUMBER: jb_double(jb, v->n); break;
        case JV_STRING: jb_str(jb, v->s ? v->s : ""); break;
        case JV_ARRAY:
            jb_arr_begin(jb);
            for (size_t i = 0; i < v->nitems; i++) jval_serialize(jb, v->items[i]);
            jb_arr_end(jb);
            break;
        case JV_OBJECT:
            jb_obj_begin(jb);
            for (size_t i = 0; i < v->nkeys; i++) {
                jb_key(jb, v->keys[i] ? v->keys[i] : "");
                jval_serialize(jb, v->vals[i]);
            }
            jb_obj_end(jb);
            break;
        default: jb_null(jb); break;
    }
}

/* ── POST /api/analytics/results ── */
static void h_result_save(HttpReq *req, HttpResp *resp) {
    if (!req->body || !req->body_len) { http_resp_error(resp,400,"empty body"); return; }
    Arena *a = arena_create(524288); /* 512 KiB */
    JVal *root = json_parse(a, req->body, req->body_len);
    if (!root || root->type != JV_OBJECT) {
        http_resp_error(resp,400,"invalid json"); arena_destroy(a); return;
    }
    const char *name = json_str(json_get(root,"name"), "");
    if (!*name) { http_resp_error(resp,400,"missing name"); arena_destroy(a); return; }
    const char *sql  = json_str(json_get(root,"sql"), "");
    JVal *cols_v = json_get(root,"columns");
    JVal *rows_v = json_get(root,"rows");

    /* Serialize columns and rows arrays back to JSON strings */
    JBuf cjb; jb_init(&cjb, a, 512);
    jval_serialize(&cjb, cols_v);
    const char *columns_json = jb_done(&cjb);

    JBuf rjb; jb_init(&rjb, a, 65536);
    jval_serialize(&rjb, rows_v);
    const char *rows_json = jb_done(&rjb);

    int row_count = (int)json_int(json_get(root,"row_count"),
                    (rows_v && rows_v->type==JV_ARRAY) ? (long long)rows_v->nitems : 0);

    int64_t new_id = 0;
    if (catalog_save_result(g_app.catalog, name, sql, columns_json, rows_json,
                            row_count, &new_id) != 0) {
        http_resp_error(resp,500,"save failed"); arena_destroy(a); return;
    }
    JBuf jb; jb_init(&jb,a,64);
    jb_obj_begin(&jb);
    jb_key(&jb,"id"); jb_int(&jb,new_id);
    jb_obj_end(&jb);
    http_resp_json(resp,201,(char*)jb_done(&jb));
}

/* ── GET /api/analytics/results ── */
static void h_result_list(HttpReq *req, HttpResp *resp) {
    (void)req;
    Arena *a = arena_create(16384);
    char *json = NULL;
    catalog_list_results(g_app.catalog, &json, a);
    http_resp_json(resp, 200, json ? json : "[]");
}

/* ── GET /api/analytics/results/:id ── */
static void h_result_get(HttpReq *req, HttpResp *resp) {
    const char *ids = hm_get(&req->params,"id");
    if (!ids) { http_resp_error(resp,400,"missing id"); return; }
    int64_t id = (int64_t)strtoll(ids, NULL, 10);
    Arena *a = arena_create(65536);
    char *json = NULL;
    if (catalog_get_result(g_app.catalog, id, &json, a) != 0) {
        http_resp_error(resp,404,"not found"); arena_destroy(a); return;
    }
    http_resp_json(resp,200,json);
}

/* ── DELETE /api/analytics/results/:id ── */
static void h_result_delete(HttpReq *req, HttpResp *resp) {
    const char *ids = hm_get(&req->params,"id");
    if (!ids) { http_resp_error(resp,400,"missing id"); return; }
    int64_t id = (int64_t)strtoll(ids, NULL, 10);
    if (catalog_delete_result(g_app.catalog, id) != 0) {
        http_resp_error(resp,500,"delete failed"); return;
    }
    http_resp_json(resp,200,"{\"ok\":true}");
}

/* ── POST /api/tables/:name/indexes  {"column":"col_name"} ── */
static void h_index_create(HttpReq *req, HttpResp *resp) {
    const char *tname = hm_get(&req->params, "name");
    if (!tname) { http_resp_error(resp,400,"missing name"); return; }

    Arena *a = arena_create(8192);
    JVal *root = json_parse(a, req->body, req->body_len);
    if (!root) { http_resp_error(resp,400,"invalid json"); arena_destroy(a); return; }
    const char *col_name = json_str(json_get(root,"column"), "");
    if (!col_name[0]) { http_resp_error(resp,400,"missing column"); arena_destroy(a); return; }

    /* find open Table* in hashmap */
    pthread_mutex_lock(&g_app.tables_mu);
    Table *tbl = hm_get(&g_app.tables, tname);
    pthread_mutex_unlock(&g_app.tables_mu);
    if (!tbl) { http_resp_error(resp,404,"table not found"); arena_destroy(a); return; }

    /* resolve col_idx from catalog schema */
    Schema *sc = NULL;
    catalog_get_schema(g_app.catalog, tname, &sc, a);
    if (!sc) { http_resp_error(resp,500,"no schema"); arena_destroy(a); return; }
    int col_idx = -1;
    for (int i = 0; i < sc->ncols; i++) {
        if (strcasecmp(sc->cols[i].name, col_name) == 0) { col_idx = i; break; }
    }
    if (col_idx < 0) { http_resp_error(resp,400,"column not found"); arena_destroy(a); return; }
    if (sc->cols[col_idx].type != COL_INT64) {
        http_resp_error(resp,400,"only INT64 columns can be indexed"); arena_destroy(a); return;
    }

    if (table_create_index(tbl, col_idx, g_app.catalog) != 0) {
        http_resp_error(resp,500,"index creation failed"); arena_destroy(a); return;
    }
    http_resp_json(resp, 200, "{\"ok\":true}");
    arena_destroy(a);
}

/* ── GET /api/tables/:name/indexes ── */
static void h_index_list(HttpReq *req, HttpResp *resp) {
    const char *tname = hm_get(&req->params, "name");
    if (!tname) { http_resp_error(resp,400,"missing name"); return; }
    Arena *a = arena_create(16384);
    char *json = NULL;
    if (catalog_list_indexes_json(g_app.catalog, tname, &json, a) != 0 || !json) {
        http_resp_json(resp, 200, "[]"); arena_destroy(a); return;
    }
    http_resp_json(resp, 200, json);
    arena_destroy(a);
}

/* ─────────────────────────────────────────────────────────────────────
   Connector probe endpoints — test a connector config from the UI
   POST /api/connector/probe/entities
     Body: {"type":"postgresql","config":{...}}
   POST /api/connector/probe/schema
     Body: {"type":"postgresql","config":{...},"entity":"users"}
   ───────────────────────────────────────────────────────────────────── */

/* Load connector by type name → ConnectorInst* (caller must unload) */
static ConnectorInst *load_connector_by_type(Arena *a, const char *type,
                                               const char *cfg_json) {
    char so_path[1024];
    snprintf(so_path, sizeof(so_path), "%s/%s_connector.so",
             g_app.plugins_dir, type);
    return connector_load(so_path, cfg_json, a);
}

static void h_connector_probe_entities(HttpReq *req, HttpResp *resp) {
    Arena *a = arena_create(65536);
    JVal *root = json_parse(a, req->body, req->body_len);
    if (!root) { http_resp_error(resp,400,"invalid json"); arena_destroy(a); return; }

    const char *type = json_str(json_get(root,"type"), "");
    if (!type[0]) { http_resp_error(resp,400,"missing type"); arena_destroy(a); return; }

    /* config may be object or string */
    JVal *cfg_v = json_get(root,"config");
    const char *cfg_json = "{}";
    if (cfg_v) {
        if (cfg_v->type == JV_STRING) {
            cfg_json = json_str(cfg_v, "{}");
        } else if (cfg_v->type == JV_OBJECT) {
            /* Serialize object back to JSON string */
            JBuf jb; jb_init(&jb, a, 512);
            jb_obj_begin(&jb);
            for (size_t i = 0; i < cfg_v->nkeys; i++) {
                jb_key(&jb, cfg_v->keys[i]);
                JVal *vv = cfg_v->vals[i];
                if (vv->type == JV_STRING)       jb_strn(&jb, vv->s, vv->len);
                else if (vv->type == JV_NUMBER)  jb_double(&jb, vv->n);
                else if (vv->type == JV_BOOL)    jb_bool(&jb, vv->b);
                else                             jb_null(&jb);
            }
            jb_obj_end(&jb);
            cfg_json = jb_done(&jb);
        }
    }

    ConnectorInst *inst = load_connector_by_type(a, type, cfg_json);
    if (!inst) { http_resp_error(resp,500,"connector load failed"); arena_destroy(a); return; }

    DfoEntityList el = {0};
    if (connector_api(inst)->list_entities(connector_ctx(inst), a, &el) != 0) {
        connector_unload(inst);
        http_resp_error(resp,500,"list_entities failed"); arena_destroy(a); return;
    }

    JBuf jb; jb_init(&jb, a, 1024);
    jb_arr_begin(&jb);
    for (int i = 0; i < el.count; i++) {
        jb_obj_begin(&jb);
        jb_key(&jb,"name"); jb_str(&jb, el.items[i].entity);
        jb_key(&jb,"type"); jb_str(&jb, el.items[i].type);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);

    connector_unload(inst);
    http_resp_json(resp, 200, (char *)jb_done(&jb));
    arena_destroy(a);
}

static void h_connector_probe_schema(HttpReq *req, HttpResp *resp) {
    Arena *a = arena_create(65536);
    JVal *root = json_parse(a, req->body, req->body_len);
    if (!root) { http_resp_error(resp,400,"invalid json"); arena_destroy(a); return; }

    const char *type   = json_str(json_get(root,"type"), "");
    const char *entity = json_str(json_get(root,"entity"), "");
    if (!type[0] || !entity[0]) {
        http_resp_error(resp,400,"missing type or entity"); arena_destroy(a); return;
    }

    JVal *cfg_v = json_get(root,"config");
    const char *cfg_json = "{}";
    if (cfg_v && cfg_v->type == JV_OBJECT) {
        JBuf jb; jb_init(&jb, a, 512);
        jb_obj_begin(&jb);
        for (size_t i = 0; i < cfg_v->nkeys; i++) {
            jb_key(&jb, cfg_v->keys[i]);
            JVal *vv = cfg_v->vals[i];
            if (vv->type == JV_STRING)       jb_strn(&jb, vv->s, vv->len);
            else if (vv->type == JV_NUMBER)  jb_double(&jb, vv->n);
            else if (vv->type == JV_BOOL)    jb_bool(&jb, vv->b);
            else                             jb_null(&jb);
        }
        jb_obj_end(&jb);
        cfg_json = jb_done(&jb);
    } else if (cfg_v && cfg_v->type == JV_STRING) {
        cfg_json = json_str(cfg_v, "{}");
    }

    ConnectorInst *inst = load_connector_by_type(a, type, cfg_json);
    if (!inst) { http_resp_error(resp,500,"connector load failed"); arena_destroy(a); return; }

    Schema *sc = NULL;
    if (connector_api(inst)->describe(connector_ctx(inst), a, entity, &sc) != 0 || !sc) {
        connector_unload(inst);
        http_resp_error(resp,500,"describe failed"); arena_destroy(a); return;
    }

    JBuf jb; jb_init(&jb, a, 1024);
    jb_obj_begin(&jb);
    jb_key(&jb,"entity"); jb_str(&jb, entity);
    jb_key(&jb,"columns"); jb_arr_begin(&jb);
    static const char *type_names[] = {"int64","double","text","bool","null"};
    for (int c = 0; c < sc->ncols; c++) {
        jb_obj_begin(&jb);
        jb_key(&jb,"name");     jb_str(&jb, sc->cols[c].name);
        jb_key(&jb,"type");     jb_str(&jb, type_names[sc->cols[c].type]);
        jb_key(&jb,"nullable"); jb_bool(&jb, sc->cols[c].nullable);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);
    jb_obj_end(&jb);

    connector_unload(inst);
    http_resp_json(resp, 200, (char *)jb_done(&jb));
    arena_destroy(a);
}

/* ── Auth handlers ── */
static void h_auth_token(HttpReq *req, HttpResp *resp) {
    if (!req->body || req->body_len == 0) {
        http_resp_error(resp, 400, "missing body");
        return;
    }
    Arena *a = req->arena;  // Use request arena, don't create new one
    JVal *body = json_parse(a, req->body, req->body_len);
    if (!body || body->type != JV_OBJECT) {
        http_resp_error(resp, 400, "invalid json");
        return;
    }
    const char *username = json_str(json_get(body, "username"), NULL);
    const char *password = json_str(json_get(body, "password"), NULL);
    if (!username || !password) {
        http_resp_error(resp, 400, "missing username or password");
        return;
    }
    if (strcmp(username, "admin") != 0 || strcmp(password, g_app.admin_password) != 0) {
        http_resp_error(resp, 401, "invalid credentials");
        return;
    }
    AuthClaims claims;
    strncpy(claims.user_id, username, sizeof(claims.user_id) - 1);
    claims.role = ROLE_ADMIN;
    claims.exp = (int64_t)time(NULL) + 86400;  // 1 day
    char token[1024];
    if (auth_jwt_sign(g_app.jwt_secret, &claims, token, sizeof(token)) != 0) {
        http_resp_error(resp, 500, "token generation failed");
        return;
    }
    JBuf jb; jb_init(&jb, a, 512);
    jb_obj_begin(&jb);
    jb_key(&jb, "token"); jb_str(&jb, token);
    jb_key(&jb, "expires_in"); jb_int(&jb, 86400);
    jb_obj_end(&jb);
    http_resp_json(resp, 200, jb_done(&jb));
}

static void h_auth_apikey_create(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) {
        http_resp_error(resp, 403, "admin required");
        return;
    }
    if (!req->body || req->body_len == 0) {
        http_resp_error(resp, 400, "missing body");
        return;
    }
    Arena *a = req->arena;
    JVal *body = json_parse(a, req->body, req->body_len);
    if (!body || body->type != JV_OBJECT) {
        http_resp_error(resp, 400, "invalid json");
        return;
    }
    const char *user_id = json_str(json_get(body, "user_id"), NULL);
    const char *role_str = json_str(json_get(body, "role"), NULL);
    if (!user_id || !role_str) {
        http_resp_error(resp, 400, "missing user_id or role");
        return;
    }
    AuthRole role;
    if (strcmp(role_str, "admin") == 0) role = ROLE_ADMIN;
    else if (strcmp(role_str, "analyst") == 0) role = ROLE_ANALYST;
    else if (strcmp(role_str, "viewer") == 0) role = ROLE_VIEWER;
    else {
        http_resp_error(resp, 400, "invalid role");
        return;
    }
    char key[128];
    if (auth_apikey_create(g_app.auth_store, user_id, role, key, sizeof(key)) != 0) {
        http_resp_error(resp, 500, "key creation failed");
        return;
    }
    char *json_resp = arena_sprintf(a, "{\"key\":\"%s\",\"user_id\":\"%s\"}", key, user_id);
    http_resp_json(resp, 200, json_resp);
}

static void h_auth_apikey_list(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) {
        http_resp_error(resp, 403, "admin required");
        return;
    }
    // Simple implementation: query SQLite
    sqlite3_stmt *stmt;
    const char *sql = "SELECT key, user_id, role, created_at FROM auth_keys WHERE revoked = 0;";
    if (sqlite3_prepare_v2(g_app.auth_store->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        http_resp_error(resp, 500, "db error");
        return;
    }
    Arena *a = req->arena;
    JBuf jb; jb_init(&jb, a, 4096);
    jb_arr_begin(&jb);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *key = (const char *)sqlite3_column_text(stmt, 0);
        const char *user_id = (const char *)sqlite3_column_text(stmt, 1);
        int role = sqlite3_column_int(stmt, 2);
        int64_t created_at = sqlite3_column_int64(stmt, 3);
        jb_obj_begin(&jb);
        jb_key(&jb, "key"); jb_str(&jb, key);
        jb_key(&jb, "user_id"); jb_str(&jb, user_id);
        jb_key(&jb, "role"); jb_str(&jb, role == ROLE_ADMIN ? "admin" : role == ROLE_ANALYST ? "analyst" : "viewer");
        jb_key(&jb, "created_at"); jb_int(&jb, created_at);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);
    sqlite3_finalize(stmt);
    http_resp_json(resp, 200, jb_done(&jb));
}

static void h_auth_apikey_delete(HttpReq *req, HttpResp *resp) {
    if (req->auth.role != ROLE_ADMIN) {
        http_resp_error(resp, 403, "admin required");
        return;
    }
    const char *key = hm_get(&req->params, "key");
    if (!key) {
        http_resp_error(resp, 400, "missing key");
        return;
    }
    if (auth_apikey_revoke(g_app.auth_store, key) != 0) {
        http_resp_error(resp, 404, "key not found");
        return;
    }
    http_resp_json(resp, 200, "{\"ok\":true}");
}

static void h_auth_me(HttpReq *req, HttpResp *resp) {
    const char *role_str = req->auth.role == ROLE_ADMIN ? "admin" :
                           req->auth.role == ROLE_ANALYST ? "analyst" : "viewer";
    char *json_resp = arena_sprintf(req->arena, "{\"user_id\":\"%s\",\"role\":\"%s\"}",
                                    req->auth.user_id, role_str);
    http_resp_json(resp, 200, json_resp);
}

void api_register_routes(Router *r) {
    router_add(r,"GET",  "/",           h_ui_html);
    router_add(r,"GET",  "/style.css",  h_ui_css);
    router_add(r,"GET",  "/app.js",     h_ui_js);
    router_add(r,"GET",  "/health",                  h_health);
    router_add(r,"GET",  "/api/tables",              h_tables_list);
    router_add(r,"POST", "/api/tables/query",        h_query);
    router_add(r,"GET",  "/api/tables/:name/schema", h_table_schema);
    router_add(r,"DELETE","/api/tables/:name",       h_table_delete);
    router_add(r,"POST", "/api/ingest/csv",            h_ingest_csv);
    router_add(r,"POST", "/api/ingest/parquet",        h_ingest_parquet);
    router_add(r,"GET",  "/api/pipelines",           h_pipelines_list);
    router_add(r,"POST", "/api/pipelines",           h_pipeline_create);
    router_add(r,"GET",  "/api/pipelines/:id",       h_pipeline_get);
    router_add(r,"POST", "/api/pipelines/:id/run",   h_pipeline_run);
    router_add(r,"DELETE","/api/pipelines/:id",      h_pipeline_delete);
    router_add(r,"GET",  "/api/pipelines/:id/runs",  h_pipeline_runs);
    router_add(r,"GET",  "/api/metrics",             h_metrics);
    router_add(r,"POST",   "/api/analytics/results",     h_result_save);
    router_add(r,"GET",    "/api/analytics/results",     h_result_list);
    router_add(r,"GET",    "/api/analytics/results/:id", h_result_get);
    router_add(r,"DELETE", "/api/analytics/results/:id", h_result_delete);
    router_add(r,"POST",  "/api/tables/:name/indexes",  h_index_create);
    router_add(r,"GET",   "/api/tables/:name/indexes",  h_index_list);
    router_add(r,"POST",  "/api/connector/probe/entities", h_connector_probe_entities);
    router_add(r,"POST",  "/api/connector/probe/schema",   h_connector_probe_schema);
    // Auth endpoints
    router_add(r,"POST", "/api/auth/token",    h_auth_token);
    router_add(r,"POST", "/api/auth/apikeys",  h_auth_apikey_create);
    router_add(r,"GET",  "/api/auth/apikeys",  h_auth_apikey_list);
    router_add(r,"DELETE","/api/auth/apikeys/:key", h_auth_apikey_delete);
    router_add(r,"GET",  "/api/auth/me",       h_auth_me);
}
