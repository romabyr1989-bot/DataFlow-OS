#include "json.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <ctype.h>

/* ── Builder ── */
static void jb_ensure(JBuf *j, size_t need) {
    if (j->len + need <= j->cap) return;
    size_t nc = j->cap * 2 + need + 64;
    char *nb = arena_alloc(j->a, nc);
    memcpy(nb, j->buf, j->len);
    j->buf = nb; j->cap = nc;
}

static void jb_append(JBuf *j, const char *s, size_t n) {
    jb_ensure(j, n); memcpy(j->buf + j->len, s, n); j->len += n;
}

static void jb_ch(JBuf *j, char c) { jb_ensure(j, 1); j->buf[j->len++] = c; }

static void jb_comma(JBuf *j) {
    /* If last non-space char is value/string end, add comma */
    if (j->len > 0) {
        char last = j->buf[j->len-1];
        if (last != '[' && last != '{' && last != ':') jb_ch(j, ',');
    }
}

void jb_init(JBuf *j, Arena *a, size_t init) {
    j->a = a; j->cap = init ? init : 256;
    j->buf = arena_alloc(a, j->cap); j->len = 0;
}

void jb_obj_begin(JBuf *j) { jb_comma(j); jb_ch(j, '{'); }
void jb_obj_end(JBuf *j)   { jb_ch(j, '}'); }
void jb_arr_begin(JBuf *j) { jb_comma(j); jb_ch(j, '['); }
void jb_arr_end(JBuf *j)   { jb_ch(j, ']'); }

void jb_key(JBuf *j, const char *k) {
    jb_comma(j);
    jb_ch(j, '"');
    jb_append(j, k, strlen(k));
    jb_append(j, "\":", 2);
}

void jb_str(JBuf *j, const char *s) {
    if (!s) { jb_raw(j, "null"); return; }
    jb_strn(j, s, strlen(s));
}

void jb_strn(JBuf *j, const char *s, size_t n) {
    jb_comma(j); jb_ch(j, '"');
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c == '"')  { jb_append(j, "\\\"", 2); }
        else if (c == '\\') { jb_append(j, "\\\\", 2); }
        else if (c == '\n') { jb_append(j, "\\n", 2); }
        else if (c == '\r') { jb_append(j, "\\r", 2); }
        else if (c == '\t') { jb_append(j, "\\t", 2); }
        else if (c < 0x20)  { char esc[8]; snprintf(esc,sizeof(esc),"\\u%04x",c); jb_append(j,esc,6); }
        else jb_ch(j, (char)c);
    }
    jb_ch(j, '"');
}

void jb_int(JBuf *j, long long v) {
    char buf[32]; int n = snprintf(buf, sizeof(buf), "%lld", v);
    jb_comma(j); jb_append(j, buf, (size_t)n);
}

void jb_double(JBuf *j, double v) {
    char buf[64]; int n = snprintf(buf, sizeof(buf), "%.10g", v);
    jb_comma(j); jb_append(j, buf, (size_t)n);
}

void jb_bool(JBuf *j, bool v) { jb_comma(j); jb_append(j, v?"true":"false", v?4:5); }
void jb_null(JBuf *j)         { jb_comma(j); jb_append(j, "null", 4); }
void jb_raw(JBuf *j, const char *raw) { jb_comma(j); jb_append(j, raw, strlen(raw)); }

const char *jb_done(JBuf *j) {
    jb_ensure(j, 1); j->buf[j->len] = '\0'; return j->buf;
}

/* ── Parser ── */
typedef struct { const char *src; size_t pos, len; Arena *a; } JP;

static void skip_ws(JP *p) {
    while (p->pos < p->len && isspace((unsigned char)p->src[p->pos])) p->pos++;
}

static JVal *jv_new(JP *p, JValType t) {
    JVal *v = arena_calloc(p->a, sizeof(JVal)); v->type = t; return v;
}

static JVal *parse_value(JP *p);

static JVal *parse_string(JP *p) {
    if (p->src[p->pos] != '"') return jv_new(p, JV_ERROR);
    p->pos++;
    size_t start = p->pos;
    char *out = arena_alloc(p->a, p->len - p->pos + 1);
    size_t outlen = 0;
    while (p->pos < p->len && p->src[p->pos] != '"') {
        if (p->src[p->pos] == '\\') {
            p->pos++;
            switch (p->src[p->pos]) {
                case '"': out[outlen++]='"'; break;
                case '\\': out[outlen++]='\\'; break;
                case '/': out[outlen++]='/'; break;
                case 'n': out[outlen++]='\n'; break;
                case 'r': out[outlen++]='\r'; break;
                case 't': out[outlen++]='\t'; break;
                case 'u': {
                    unsigned int cp = 0;
                    for (int _k = 0; _k < 4; _k++) {
                        if (p->pos+1 >= p->len) break;
                        p->pos++;
                        unsigned char h = (unsigned char)p->src[p->pos];
                        cp <<= 4;
                        if (h>='0'&&h<='9') cp|=(h-'0');
                        else if (h>='a'&&h<='f') cp|=(h-'a'+10);
                        else if (h>='A'&&h<='F') cp|=(h-'A'+10);
                    }
                    if (cp < 0x80) { out[outlen++]=(char)cp; }
                    else if (cp < 0x800) {
                        out[outlen++]=(char)(0xC0|(cp>>6));
                        out[outlen++]=(char)(0x80|(cp&0x3F));
                    } else {
                        out[outlen++]=(char)(0xE0|(cp>>12));
                        out[outlen++]=(char)(0x80|((cp>>6)&0x3F));
                        out[outlen++]=(char)(0x80|(cp&0x3F));
                    }
                    break;
                }
                default:  out[outlen++]=p->src[p->pos]; break;
            }
        } else { out[outlen++] = p->src[p->pos]; }
        p->pos++;
    }
    (void)start;
    if (p->pos < p->len) p->pos++; /* skip closing " */
    out[outlen] = '\0';
    JVal *v = jv_new(p, JV_STRING); v->s = out; v->len = outlen;
    return v;
}

static JVal *parse_array(JP *p) {
    p->pos++; /* skip [ */
    JVal *v = jv_new(p, JV_ARRAY);
    size_t cap = 8;
    JVal **items = arena_alloc(p->a, cap * sizeof(JVal *));
    size_t n = 0;
    skip_ws(p);
    if (p->pos < p->len && p->src[p->pos] == ']') { p->pos++; v->items=items; v->nitems=0; return v; }
    for (;;) {
        skip_ws(p);
        if (n == cap) { cap*=2; JVal **nb=arena_alloc(p->a,cap*sizeof(JVal*)); memcpy(nb,items,n*sizeof(JVal*)); items=nb; }
        items[n++] = parse_value(p);
        skip_ws(p);
        if (p->pos >= p->len || p->src[p->pos] == ']') break;
        if (p->src[p->pos] == ',') p->pos++;
    }
    if (p->pos < p->len) p->pos++;
    v->items = items; v->nitems = n;
    return v;
}

static JVal *parse_object(JP *p) {
    p->pos++; /* skip { */
    JVal *v = jv_new(p, JV_OBJECT);
    size_t cap = 8;
    const char **keys = arena_alloc(p->a, cap * sizeof(char *));
    JVal **vals = arena_alloc(p->a, cap * sizeof(JVal *));
    size_t n = 0;
    skip_ws(p);
    if (p->pos < p->len && p->src[p->pos] == '}') { p->pos++; v->keys=keys;v->vals=vals;v->nkeys=0; return v; }
    for (;;) {
        skip_ws(p);
        if (n == cap) {
            cap*=2;
            const char **nk=arena_alloc(p->a,cap*sizeof(char*)); memcpy(nk,keys,n*sizeof(char*)); keys=nk;
            JVal **nv=arena_alloc(p->a,cap*sizeof(JVal*)); memcpy(nv,vals,n*sizeof(JVal*)); vals=nv;
        }
        JVal *ks = parse_string(p); keys[n] = ks->s;
        skip_ws(p);
        if (p->pos < p->len && p->src[p->pos] == ':') p->pos++;
        skip_ws(p);
        vals[n] = parse_value(p); n++;
        skip_ws(p);
        if (p->pos >= p->len || p->src[p->pos] == '}') break;
        if (p->src[p->pos] == ',') p->pos++;
    }
    if (p->pos < p->len) p->pos++;
    v->keys=keys; v->vals=vals; v->nkeys=n;
    return v;
}

static JVal *parse_value(JP *p) {
    skip_ws(p);
    if (p->pos >= p->len) return jv_new(p, JV_ERROR);
    char c = p->src[p->pos];
    if (c == '"') return parse_string(p);
    if (c == '[') return parse_array(p);
    if (c == '{') return parse_object(p);
    if (strncmp(p->src+p->pos,"null",4)==0) { p->pos+=4; return jv_new(p,JV_NULL); }
    if (strncmp(p->src+p->pos,"true",4)==0) { p->pos+=4; JVal *v=jv_new(p,JV_BOOL); v->b=true; return v; }
    if (strncmp(p->src+p->pos,"false",5)==0){ p->pos+=5; JVal *v=jv_new(p,JV_BOOL); v->b=false; return v; }
    if (c=='-'||isdigit((unsigned char)c)) {
        char *end; double d = strtod(p->src+p->pos, &end);
        p->pos = (size_t)(end - p->src);
        JVal *v = jv_new(p,JV_NUMBER); v->n=d; return v;
    }
    return jv_new(p, JV_ERROR);
}

JVal *json_parse(Arena *a, const char *src, size_t len) {
    JP p = {src, 0, len, a};
    return parse_value(&p);
}

JVal *json_get(JVal *obj, const char *key) {
    if (!obj || obj->type != JV_OBJECT) return NULL;
    for (size_t i = 0; i < obj->nkeys; i++)
        if (strcmp(obj->keys[i], key)==0) return obj->vals[i];
    return NULL;
}

const char *json_str(JVal *v, const char *def) {
    return (v && v->type==JV_STRING) ? v->s : def;
}
long long json_int(JVal *v, long long def) {
    return (v && v->type==JV_NUMBER) ? (long long)v->n : def;
}
double json_dbl(JVal *v, double def) {
    return (v && v->type==JV_NUMBER) ? v->n : def;
}
bool json_bool(JVal *v, bool def) {
    return (v && v->type==JV_BOOL) ? v->b : def;
}
