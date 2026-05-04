/* parquet_connector.c — минимальный ридер Apache Parquet v2 для DFO.
 * Поддерживает: INT32/64, INT96 (как INT64), FLOAT/DOUBLE, BYTE_ARRAY,
 * BOOLEAN; кодировка PLAIN; GZIP и UNCOMPRESSED; один файл или glob. */
#include "../../connector.h"
#include "../../../core/log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <zlib.h>
#include <glob.h>

/* ── LE-читалки (не зависят от endian.h) ── */
static uint32_t le32r(const uint8_t *p) {
    return (uint32_t)p[0]|((uint32_t)p[1]<<8)|((uint32_t)p[2]<<16)|((uint32_t)p[3]<<24);
}
static uint64_t le64r(const uint8_t *p) { return (uint64_t)le32r(p)|((uint64_t)le32r(p+4)<<32); }
static float    lef32(const uint8_t *p) { float f; memcpy(&f,p,4); return f; }
static double   lef64(const uint8_t *p) { double d; memcpy(&d,p,8); return d; }

/* ── Thrift Compact Protocol ── */
typedef struct { const uint8_t *d; size_t pos, len; } TBuf;

static uint64_t tc_uv(TBuf *b) {
    uint64_t r=0; int sh=0;
    while (b->pos < b->len) {
        uint8_t c=b->d[b->pos++];
        r|=(uint64_t)(c&0x7F)<<sh;
        if(!(c&0x80)) return r;
        sh+=7;
    }
    return r;
}
static int64_t tc_i(TBuf *b) { uint64_t n=tc_uv(b); return (int64_t)((n>>1)^-(n&1)); }

/* Типы полей Compact Protocol */
#define TCT_BOOLT 1
#define TCT_BOOLF 2
#define TCT_BYTE  3
#define TCT_I16   4
#define TCT_I32   5
#define TCT_I64   6
#define TCT_DBL   7
#define TCT_BIN   8
#define TCT_LIST  9
#define TCT_SET   10
#define TCT_MAP   11
#define TCT_STRUCT 12

static void tc_skip(TBuf *b, int t);

static void tc_skip_list(TBuf *b) {
    if (b->pos>=b->len) return;
    uint8_t h=b->d[b->pos++];
    int et=h&0x0F, cnt=(h>>4)&0x0F;
    if (cnt==15) cnt=(int)tc_uv(b);
    for (int i=0;i<cnt&&b->pos<b->len;i++) tc_skip(b,et);
}

static void tc_skip(TBuf *b, int t) {
    size_t l;
    switch(t) {
        case TCT_BOOLT: case TCT_BOOLF: break;
        case TCT_BYTE:  b->pos++; break;
        case TCT_I16: case TCT_I32: case TCT_I64: tc_uv(b); break;
        case TCT_DBL: if(b->pos+8<=b->len) b->pos+=8; break;
        case TCT_BIN: l=(size_t)tc_uv(b); b->pos+=l; break;
        case TCT_LIST: case TCT_SET: tc_skip_list(b); break;
        case TCT_MAP: {
            uint64_t cnt=tc_uv(b);
            if(!cnt) break;
            uint8_t types=b->d[b->pos++];
            int kt=(types>>4)&0x0F, vt=types&0x0F;
            for (uint64_t i=0;i<cnt&&b->pos<b->len;i++) { tc_skip(b,kt); tc_skip(b,vt); }
            break;
        }
        case TCT_STRUCT: {
            int last=0;
            while (b->pos<b->len) {
                uint8_t fh=b->d[b->pos++];
                if(!fh) break;
                int delta=(fh>>4)&0x0F, ft=fh&0x0F;
                if(delta) last+=delta; else last=(int)tc_i(b);
                (void)last;
                tc_skip(b,ft);
            }
            break;
        }
    }
}

/* Читает следующий field header; возвращает 0 на stop-байте или конце буфера */
static int tc_next(TBuf *b, int *last_fid, int *ftype) {
    if (b->pos>=b->len) return 0;
    uint8_t fh=b->d[b->pos++];
    if (!fh) return 0;
    int delta=(fh>>4)&0x0F;
    *ftype=fh&0x0F;
    if (delta) *last_fid+=delta; else *last_fid=(int)tc_i(b);
    return 1;
}

/* ── Константы Parquet ── */
#define PQ_BOOLEAN  0
#define PQ_INT32    1
#define PQ_INT64    2
#define PQ_INT96    3
#define PQ_FLOAT    4
#define PQ_DOUBLE   5
#define PQ_BYTEARRAY 6
#define PQ_FIXEDLEN  7

#define PQ_UNCOMP   0
#define PQ_SNAPPY   1
#define PQ_GZIP     2

#define PQ_DATA_PAGE   0
#define PQ_DICT_PAGE   2
#define PQ_DATA_V2     3

#define PQ_PLAIN        0

#define PQ_MAX_COLS 256
#define PQ_MAX_RG  4096

/* ── Внутренние структуры ── */
typedef struct {
    char name[128];
    int  pq_type;    /* -1 для групповых узлов */
    int  repetition; /* 0=REQUIRED, 1=OPTIONAL */
    int  num_children;
} PqRawSchema;

typedef struct {
    char name[128];
    int  pq_type;
    bool optional;
} PqColDef;

typedef struct {
    int64_t data_page_offset;
    int64_t num_values;
    int64_t total_compressed;
    int     pq_type;
    int     codec;
} PqColMeta;

typedef struct {
    int64_t   num_rows;
    PqColMeta cols[PQ_MAX_COLS];
    int       ncols;
} PqRowGroup;

typedef struct {
    FILE     *fp;
    char      path[512];
    PqColDef  cols[PQ_MAX_COLS];
    int       ncols;
    PqRowGroup *rgs;
    int        nrgs;
    Arena     *arena;
} PqFile;

/* Позиция чтения — хранится в PqCtx, не в курсоре */
typedef struct {
    char    **paths;
    int       npaths;
    PqFile   *files;
    Arena    *arena;
    int       pos_file;
    int       pos_rg;
    int64_t   pos_row; /* ряд внутри текущей row group */
} PqCtx;

/* ── GZIP деcompression ── */
static int gzip_decomp(const uint8_t *src, size_t slen,
                        uint8_t *dst, size_t dlen) {
    z_stream z={0};
    z.next_in=(Bytef*)src; z.avail_in=(uInt)slen;
    z.next_out=(Bytef*)dst; z.avail_out=(uInt)dlen;
    /* windowBits 47 = автодетект gzip/zlib */
    if (inflateInit2(&z,47)!=Z_OK) return -1;
    int r=inflate(&z,Z_FINISH);
    inflateEnd(&z);
    return (r==Z_STREAM_END)?0:-1;
}

/* ── RLE/Bit-Packing для уровней определения ── */
static int rle_bp_dec(const uint8_t *data, size_t dlen,
                       int bw, uint8_t *out, int nmax) {
    TBuf b={data,0,dlen};
    int n=0;
    while (b.pos<b.len && n<nmax) {
        uint64_t hdr=tc_uv(&b);
        if (!hdr) break;
        if (hdr&1) {
            int grps=(int)(hdr>>1);
            int mask=(1<<bw)-1;
            for (int g=0;g<grps&&b.pos<b.len&&n<nmax;g++) {
                uint8_t byte=b.d[b.pos++];
                for (int v=0;v<8&&n<nmax;v++)
                    out[n++]=(byte>>(v*bw))&mask;
            }
        } else {
            int cnt=(int)(hdr>>1);
            int vb=(bw+7)/8; uint32_t val=0;
            for (int i=0;i<vb&&b.pos<b.len;i++)
                val|=(uint32_t)b.d[b.pos++]<<(i*8);
            for (int i=0;i<cnt&&n<nmax;i++) out[n++]=(uint8_t)val;
        }
    }
    return n;
}

/* ── Парсер схемы ── */
static int pq_parse_schema_list(TBuf *b, PqRawSchema *raw, int maxn) {
    if (b->pos>=b->len) return 0;
    uint8_t h=b->d[b->pos++];
    int et=h&0x0F, cnt=(h>>4)&0x0F;
    if (cnt==15) cnt=(int)tc_uv(b);
    if (et!=TCT_STRUCT) { for(int i=0;i<cnt;i++) tc_skip(b,et); return 0; }
    int n=0;
    for (int i=0;i<cnt&&n<maxn&&b->pos<b->len;i++) {
        PqRawSchema *s=&raw[n]; memset(s,0,sizeof(*s)); s->pq_type=-1;
        int last=0,ft;
        while (tc_next(b,&last,&ft)) {
            switch(last) {
                case 1: s->pq_type=(int)tc_i(b); break;
                case 3: s->repetition=(int)tc_i(b); break;
                case 4: { size_t l=(size_t)tc_uv(b);
                    size_t cp=l<127?l:127; memcpy(s->name,b->d+b->pos,cp); s->name[cp]='\0';
                    b->pos+=l; break; }
                case 5: s->num_children=(int)tc_i(b); break;
                default: tc_skip(b,ft); break;
            }
        }
        n++;
    }
    return n;
}

static int pq_collect_leaves(PqRawSchema *raw, int nraw, PqColDef *cols, int max) {
    int n=0;
    for (int i=1;i<nraw&&n<max;i++) /* 0 = root */
        if (raw[i].num_children==0) {
            cols[n].pq_type=raw[i].pq_type;
            cols[n].optional=(raw[i].repetition==1);
            memcpy(cols[n].name,raw[i].name,128);
            n++;
        }
    return n;
}

/* ── Парсер ColumnMetaData ── */
static void pq_parse_col_meta(TBuf *b, PqColMeta *cm) {
    int last=0,ft;
    while (tc_next(b,&last,&ft)) {
        switch(last) {
            case 1: cm->pq_type=(int)tc_i(b); break;
            case 2: tc_skip(b,ft); break; /* encodings */
            case 3: tc_skip(b,ft); break; /* path_in_schema */
            case 4: cm->codec=(int)tc_i(b); break;
            case 5: cm->num_values=tc_i(b); break;
            case 6: tc_i(b); break; /* total_uncompressed */
            case 7: cm->total_compressed=tc_i(b); break;
            case 8: tc_skip(b,ft); break; /* kv_metadata */
            case 9: cm->data_page_offset=tc_i(b); break;
            default: tc_skip(b,ft); break;
        }
    }
}

static void pq_parse_col_chunk(TBuf *b, PqColMeta *cm) {
    memset(cm,0,sizeof(*cm));
    int last=0,ft;
    while (tc_next(b,&last,&ft)) {
        switch(last) {
            case 1: tc_skip(b,ft); break; /* file_path */
            case 2: tc_i(b); break;       /* file_offset */
            case 3: pq_parse_col_meta(b,cm); break;
            default: tc_skip(b,ft); break;
        }
    }
}

static int pq_parse_col_list(TBuf *b, PqRowGroup *rg, int maxcols) {
    if (b->pos>=b->len) return 0;
    uint8_t h=b->d[b->pos++];
    int et=h&0x0F, cnt=(h>>4)&0x0F;
    if (cnt==15) cnt=(int)tc_uv(b);
    if (et!=TCT_STRUCT) { for(int i=0;i<cnt;i++) tc_skip(b,et); return 0; }
    int n=0;
    for (int i=0;i<cnt&&n<maxcols&&b->pos<b->len;i++)
        pq_parse_col_chunk(b,&rg->cols[n++]);
    return n;
}

static void pq_parse_row_group(TBuf *b, PqRowGroup *rg) {
    memset(rg,0,sizeof(*rg));
    int last=0,ft;
    while (tc_next(b,&last,&ft)) {
        switch(last) {
            case 1: rg->ncols=pq_parse_col_list(b,rg,PQ_MAX_COLS); break;
            case 2: tc_i(b); break; /* total_byte_size */
            case 3: rg->num_rows=tc_i(b); break;
            default: tc_skip(b,ft); break;
        }
    }
}

static int pq_parse_rg_list(TBuf *b, PqRowGroup *rgs, int maxrg) {
    if (b->pos>=b->len) return 0;
    uint8_t h=b->d[b->pos++];
    int et=h&0x0F, cnt=(h>>4)&0x0F;
    if (cnt==15) cnt=(int)tc_uv(b);
    if (et!=TCT_STRUCT) { for(int i=0;i<cnt;i++) tc_skip(b,et); return 0; }
    int n=0;
    for (int i=0;i<cnt&&n<maxrg&&b->pos<b->len;i++)
        pq_parse_row_group(b,&rgs[n++]);
    return n;
}

static int pq_parse_footer(const uint8_t *data, size_t len, PqFile *f) {
    TBuf b={data,0,len};
    PqRawSchema raw[PQ_MAX_COLS*2]; int nraw=0;
    int last=0,ft;
    while (tc_next(&b,&last,&ft)) {
        switch(last) {
            case 1: tc_i(&b); break; /* version */
            case 2: nraw=pq_parse_schema_list(&b,raw,PQ_MAX_COLS*2); break;
            case 3: tc_i(&b); break; /* num_rows */
            case 4: f->nrgs=pq_parse_rg_list(&b,f->rgs,PQ_MAX_RG); break;
            default: tc_skip(&b,ft); break;
        }
    }
    f->ncols=pq_collect_leaves(raw,nraw,f->cols,PQ_MAX_COLS);
    return (f->ncols>0)?0:-1;
}

/* ── Открытие файла и чтение футера ── */
static int pq_open(PqFile *f, Arena *a) {
    f->fp=fopen(f->path,"rb");
    if (!f->fp) return -1;
    uint8_t magic[4];
    fread(magic,1,4,f->fp);
    if (memcmp(magic,"PAR1",4)!=0) { fclose(f->fp); f->fp=NULL; return -1; }
    fseek(f->fp,-8,SEEK_END);
    uint8_t tail[8]; fread(tail,1,8,f->fp);
    if (memcmp(tail+4,"PAR1",4)!=0) { fclose(f->fp); f->fp=NULL; return -1; }
    uint32_t flen=le32r(tail);
    fseek(f->fp,-(long)(8+flen),SEEK_END);
    uint8_t *footer=arena_alloc(a,flen);
    fread(footer,1,flen,f->fp);
    f->rgs=arena_calloc(a,PQ_MAX_RG*sizeof(PqRowGroup));
    f->arena=a;
    return pq_parse_footer(footer,flen,f);
}

/* ── PageHeader (DATA_PAGE / DATA_V2) ── */
typedef struct {
    int     page_type;
    int32_t uncompressed_size;
    int32_t compressed_size;
    int32_t num_values;
    int     encoding;
    int     def_enc;
    /* V2 extras */
    int32_t def_bytes;
    int32_t rep_bytes;
    bool    v2_compressed;
    size_t  hdr_bytes; /* размер самого заголовка */
} PageHdr;

static void pq_parse_page_hdr(TBuf *b, PageHdr *ph) {
    memset(ph,0,sizeof(*ph)); ph->v2_compressed=true;
    int last=0,ft;
    while (tc_next(b,&last,&ft)) {
        switch(last) {
            case 1: ph->page_type=(int)tc_i(b); break;
            case 2: ph->uncompressed_size=(int32_t)tc_i(b); break;
            case 3: ph->compressed_size=(int32_t)tc_i(b); break;
            case 4: tc_i(b); break; /* crc */
            case 5: { /* DataPageHeader */
                int l2=0,ft2;
                while (tc_next(b,&l2,&ft2)) {
                    switch(l2) {
                        case 1: ph->num_values=(int32_t)tc_i(b); break;
                        case 2: ph->encoding=(int)tc_i(b); break;
                        case 3: ph->def_enc=(int)tc_i(b); break;
                        default: tc_skip(b,ft2); break;
                    }
                }
                break;
            }
            case 7: { /* DataPageHeaderV2 */
                int l2=0,ft2;
                while (tc_next(b,&l2,&ft2)) {
                    switch(l2) {
                        case 1: ph->num_values=(int32_t)tc_i(b); break;
                        case 4: ph->encoding=(int)tc_i(b); break;
                        case 5: ph->def_bytes=(int32_t)tc_i(b); break;
                        case 6: ph->rep_bytes=(int32_t)tc_i(b); break;
                        case 7: ph->v2_compressed=(ft2==TCT_BOOLT); break;
                        default: tc_skip(b,ft2); break;
                    }
                }
                break;
            }
            case 8: { /* DictionaryPageHeader — skip */
                int l2=0,ft2; while(tc_next(b,&l2,&ft2)) tc_skip(b,ft2);
                break;
            }
            default: tc_skip(b,ft); break;
        }
    }
    ph->hdr_bytes=b->pos;
}

/* ── Пропуск n значений в PLAIN-потоке ── */
static const uint8_t *skip_plain(const uint8_t *p, const uint8_t *end, int pq_type, int n) {
    if (n<=0) return p;
    switch (pq_type) {
        case PQ_INT32: case PQ_FLOAT:   return p+4*(size_t)n<end?p+4*(size_t)n:end;
        case PQ_INT64: case PQ_DOUBLE:  return p+8*(size_t)n<end?p+8*(size_t)n:end;
        case PQ_INT96:                  return p+12*(size_t)n<end?p+12*(size_t)n:end;
        case PQ_BYTEARRAY:
            for (int i=0;i<n&&p+4<=end;i++) { uint32_t l=le32r(p); p+=4; p+=l; if(p>end)p=end; }
            return p;
        default: return end; /* BOOLEAN и неизвестные — пропустить страницу */
    }
}

/* ── Запись одного значения из PLAIN-потока в ColBatch ── */
/* Возвращает указатель за прочитанными байтами */
static const uint8_t *write_plain_val(const uint8_t *p, const uint8_t *end,
                                       int pq_type, int bool_bit,
                                       ColBatch *batch, int slot, int row, Arena *a) {
    switch (pq_type) {
        case PQ_INT32:
            if (p+4>end) return end;
            ((int64_t*)batch->values[slot])[row]=(int64_t)(int32_t)le32r(p);
            return p+4;
        case PQ_INT64:
            if (p+8>end) return end;
            ((int64_t*)batch->values[slot])[row]=(int64_t)le64r(p);
            return p+8;
        case PQ_INT96:
            if (p+12>end) return end;
            ((int64_t*)batch->values[slot])[row]=(int64_t)le64r(p);
            return p+12;
        case PQ_FLOAT:
            if (p+4>end) return end;
            ((double*)batch->values[slot])[row]=(double)lef32(p);
            return p+4;
        case PQ_DOUBLE:
            if (p+8>end) return end;
            ((double*)batch->values[slot])[row]=lef64(p);
            return p+8;
        case PQ_BYTEARRAY: {
            if (p+4>end) return end;
            uint32_t sl=le32r(p); p+=4;
            if (p+sl>end) sl=(uint32_t)(end-p);
            ((const char**)batch->values[slot])[row]=arena_strndup(a,(const char*)p,sl);
            return p+sl;
        }
        case PQ_BOOLEAN: {
            /* bool_bit = позиция бита в потоке */
            const uint8_t *bp=p+bool_bit/8;
            int v=(bp<end)?(((*bp)>>(bool_bit%8))&1):0;
            ((int64_t*)batch->values[slot])[row]=(int64_t)v;
            return p; /* указатель не меняем — продвигаем через bool_bit */
        }
        default:
            ((const char**)batch->values[slot])[row]="";
            return p;
    }
}

/* ── Основная функция чтения одной колонки ── */
/* Читает [start_row, start_row+nrows) из row group rg_idx, колонка col_idx.
 * Записывает в batch->values[slot] начиная с batch_start. */
static void pq_read_col_range(PqFile *f, int rg_idx, int col_idx,
                               int64_t start_row, int nrows,
                               ColBatch *batch, int slot, int batch_start, Arena *a) {
    PqRowGroup *rg=&f->rgs[rg_idx];
    PqColMeta *cm=&rg->cols[col_idx];
    PqColDef  *cd=&f->cols[col_idx];

    if (nrows<=0) return;

    fseek(f->fp,cm->data_page_offset,SEEK_SET);

    int64_t rows_seen=0;   /* строки в уже обработанных страницах */
    int     rows_written=0;/* записано в batch */
    int     bool_bit=0;    /* для BOOLEAN: бит в потоке нон-нулл значений */

    while (rows_written<nrows && rows_seen<cm->num_values) {
        /* Читаем заголовок страницы */
        uint8_t hdr_buf[512];
        long hdr_pos=(long)ftell(f->fp);
        size_t hr=fread(hdr_buf,1,sizeof(hdr_buf),f->fp);
        if (!hr) break;
        TBuf tb={hdr_buf,0,hr};
        PageHdr ph; pq_parse_page_hdr(&tb,&ph);
        fseek(f->fp,hdr_pos+(long)ph.hdr_bytes,SEEK_SET);

        if (ph.page_type==PQ_DICT_PAGE) {
            fseek(f->fp,ph.compressed_size,SEEK_CUR); continue;
        }
        if (ph.page_type!=PQ_DATA_PAGE && ph.page_type!=PQ_DATA_V2) {
            fseek(f->fp,ph.compressed_size,SEEK_CUR); continue;
        }

        /* Страница полностью до start_row — пропускаем */
        if (rows_seen+ph.num_values <= start_row) {
            rows_seen+=ph.num_values;
            fseek(f->fp,ph.compressed_size,SEEK_CUR);
            continue;
        }

        /* Читаем данные страницы */
        uint8_t *comp=arena_alloc(a,(size_t)ph.compressed_size);
        fread(comp,1,(size_t)ph.compressed_size,f->fp);

        /* Распаковка */
        const uint8_t *page_data; size_t page_len;
        uint8_t *decomp=NULL;
        bool v2_data_compressed=(ph.page_type==PQ_DATA_V2&&ph.v2_compressed&&cm->codec==PQ_GZIP);

        if (cm->codec==PQ_GZIP && ph.page_type==PQ_DATA_PAGE) {
            decomp=arena_alloc(a,(size_t)ph.uncompressed_size+1);
            if (gzip_decomp(comp,(size_t)ph.compressed_size,
                            decomp,(size_t)ph.uncompressed_size)!=0) break;
            page_data=decomp; page_len=(size_t)ph.uncompressed_size;
        } else {
            page_data=comp; page_len=(size_t)ph.compressed_size;
        }

        const uint8_t *p=page_data, *end=page_data+page_len;

        /* V2: уровни повторения (для плоской схемы = 0 байт) */
        if (ph.page_type==PQ_DATA_V2) {
            p+=ph.rep_bytes; if(p>end)p=end;
        }

        /* Уровни определения для опциональных колонок */
        uint8_t *def_lvls=NULL;
        if (cd->optional) {
            if (ph.page_type==PQ_DATA_V2) {
                def_lvls=arena_alloc(a,(size_t)ph.num_values);
                rle_bp_dec(p,(size_t)ph.def_bytes,1,def_lvls,ph.num_values);
                p+=ph.def_bytes; if(p>end)p=end;
                /* V2: тело значений может быть сжато отдельно */
                if (v2_data_compressed) {
                    size_t val_clen=(size_t)(end-p);
                    size_t val_ulen=(size_t)(ph.uncompressed_size-ph.def_bytes-ph.rep_bytes);
                    uint8_t *dv=arena_alloc(a,val_ulen+1);
                    if (gzip_decomp(p,val_clen,dv,val_ulen)==0) { p=dv; end=dv+val_ulen; }
                }
            } else if (page_len>=4) {
                uint32_t def_len=le32r(p); p+=4; if(p+def_len>end) def_len=(uint32_t)(end-p);
                def_lvls=arena_alloc(a,(size_t)ph.num_values);
                rle_bp_dec(p,def_len,1,def_lvls,ph.num_values);
                p+=def_len; if(p>end)p=end;
            }
        }

        /* Сколько строк пропустить в начале этой страницы */
        int skip_in_page=(int)(start_row>rows_seen ? start_row-rows_seen : 0);
        int avail=ph.num_values-skip_in_page;
        int take=(avail<nrows-rows_written)?avail:nrows-rows_written;

        /* Пропускаем нон-нулл значений до skip_in_page */
        int nn_skip=0;
        if (def_lvls) {
            for (int i=0;i<skip_in_page;i++) if(def_lvls[i]) nn_skip++;
        } else {
            nn_skip=skip_in_page;
        }
        p=skip_plain(p,end,cm->pq_type,nn_skip);
        /* Для BOOLEAN: nn_skip уже проинициализирован */
        bool_bit=nn_skip;

        /* Записываем 'take' строк в batch */
        int nn_written=0;
        for (int i=0;i<take&&rows_written<nrows;i++) {
            int r=batch_start+rows_written;
            bool is_null=cd->optional&&def_lvls&&(def_lvls[skip_in_page+i]==0);
            if (is_null) {
                batch->null_bitmap[slot][r/8]|=(uint8_t)(1u<<(r%8));
            } else {
                p=write_plain_val(p,end,cm->pq_type,bool_bit,batch,slot,r,a);
                if (cm->pq_type==PQ_BOOLEAN) bool_bit++;
                nn_written++;
            }
            rows_written++;
        }
        (void)nn_written;
        rows_seen+=ph.num_values;
    }
}

/* ── Маппинг Parquet → ColType ── */
static ColType pq_to_col_type(int pq_type) {
    switch(pq_type) {
        case PQ_INT32: case PQ_INT64: case PQ_INT96: case PQ_BOOLEAN: return COL_INT64;
        case PQ_FLOAT: case PQ_DOUBLE: return COL_DOUBLE;
        default: return COL_TEXT;
    }
}

/* ── cfg_get — простой парсер JSON-конфига ── */
static void cfg_get(const char *json, const char *key,
                     char *out, size_t outsz, const char *def) {
    if (def) { strncpy(out,def,outsz-1); out[outsz-1]='\0'; }
    if (!json) return;
    char search[128]; snprintf(search,sizeof(search),"\"%s\"",key);
    const char *p=strstr(json,search);
    if (!p) return;
    p+=strlen(search);
    while(*p==' '||*p=='\t'||*p=='\n') p++;
    if(*p!=':') return; p++;
    while(*p==' '||*p=='\t'||*p=='\n') p++;
    if(*p=='"') {
        p++;
        const char *e=strchr(p,'"');
        if (!e) return;
        size_t n=(size_t)(e-p); if(n>=outsz)n=outsz-1;
        memcpy(out,p,n); out[n]='\0';
    } else {
        const char *e=p;
        while(*e&&*e!=','&&*e!='}'&&*e!=' '&&*e!='\n') e++;
        size_t n=(size_t)(e-p); if(n>=outsz)n=outsz-1;
        memcpy(out,p,n); out[n]='\0';
    }
}

/* ── Connector lifecycle ── */

static void *pq_create(const char *cfg, Arena *a) {
    PqCtx *ctx=arena_calloc(a,sizeof(PqCtx));
    ctx->arena=a;

    char path[512]=""; cfg_get(cfg,"path",path,sizeof(path),"");
    if (!path[0]) { LOG_ERROR("parquet_connector: no path in config"); return ctx; }

    /* Glob expansion */
    glob_t gl; memset(&gl,0,sizeof(gl));
    int gr=glob(path,0,NULL,&gl);
    if (gr!=0||gl.gl_pathc==0) { globfree(&gl); LOG_ERROR("parquet: no files for %s",path); return ctx; }

    ctx->npaths=(int)gl.gl_pathc;
    ctx->paths=arena_alloc(a,(size_t)ctx->npaths*sizeof(char*));
    ctx->files=arena_calloc(a,(size_t)ctx->npaths*sizeof(PqFile));

    for (int i=0;i<ctx->npaths;i++) {
        ctx->paths[i]=arena_strdup(a,gl.gl_pathv[i]);
        strncpy(ctx->files[i].path,gl.gl_pathv[i],511);
        if (pq_open(&ctx->files[i],a)!=0)
            LOG_WARN("parquet: failed to open %s",gl.gl_pathv[i]);
    }
    globfree(&gl);
    return ctx;
}

static void pq_destroy(void *vctx) {
    PqCtx *ctx=vctx;
    if (!ctx) return;
    for (int i=0;i<ctx->npaths;i++)
        if (ctx->files[i].fp) { fclose(ctx->files[i].fp); ctx->files[i].fp=NULL; }
}

static int pq_ping(void *vctx) {
    PqCtx *ctx=vctx;
    if (!ctx||ctx->npaths==0) return -1;
    FILE *f=fopen(ctx->paths[0],"rb");
    if (!f) return -1;
    fclose(f); return 0;
}

static int pq_list_entities(void *vctx, Arena *a, DfoEntityList *out) {
    PqCtx *ctx=vctx;
    out->items=arena_calloc(a,(size_t)ctx->npaths*sizeof(DfoEntity));
    out->count=ctx->npaths;
    for (int i=0;i<ctx->npaths;i++) {
        const char *base=strrchr(ctx->paths[i],'/');
        out->items[i].entity=arena_strdup(a,base?base+1:ctx->paths[i]);
        out->items[i].type="table";
    }
    return 0;
}

static int pq_describe(void *vctx, Arena *a, const char *entity, Schema **out) {
    (void)entity;
    PqCtx *ctx=vctx;
    if (!ctx||ctx->npaths==0) return -1;
    PqFile *f=&ctx->files[0];
    Schema *sc=arena_calloc(a,sizeof(Schema));
    sc->ncols=f->ncols;
    sc->cols=arena_alloc(a,(size_t)f->ncols*sizeof(ColDef));
    for (int c=0;c<f->ncols;c++) {
        sc->cols[c].name=arena_strdup(a,f->cols[c].name);
        sc->cols[c].type=pq_to_col_type(f->cols[c].pq_type);
        sc->cols[c].nullable=f->cols[c].optional;
    }
    *out=sc; return 0;
}

/* read_batch использует внутреннее состояние pos_file/pos_rg/pos_row.
 * Курсор req->cursor игнорируется: каждый экземпляр коннектора живёт один сеанс. */
static int pq_read_batch(void *vctx, Arena *a, DfoReadReq *req,
                          const char *entity, ColBatch **out_batch) {
    (void)entity; (void)req;
    PqCtx *ctx=vctx;
    if (!ctx||ctx->npaths==0) return 1;

    /* Находим текущий файл/rg */
    while (ctx->pos_file<ctx->npaths) {
        PqFile *f=&ctx->files[ctx->pos_file];
        if (ctx->pos_rg<f->nrgs && f->fp) break;
        ctx->pos_file++; ctx->pos_rg=0; ctx->pos_row=0;
    }
    if (ctx->pos_file>=ctx->npaths) return 1; /* конец данных */

    PqFile *f=&ctx->files[ctx->pos_file];
    int ncols=f->ncols;

    /* Строим batch, объединяя строки из нескольких rg при необходимости */
    Schema *sc=arena_calloc(a,sizeof(Schema));
    sc->ncols=ncols; sc->cols=arena_alloc(a,(size_t)ncols*sizeof(ColDef));
    for (int c=0;c<ncols;c++) {
        sc->cols[c].name=arena_strdup(a,f->cols[c].name);
        sc->cols[c].type=pq_to_col_type(f->cols[c].pq_type);
        sc->cols[c].nullable=f->cols[c].optional;
    }

    ColBatch *batch=arena_calloc(a,sizeof(ColBatch));
    batch->schema=sc; batch->ncols=ncols;
    for (int c=0;c<ncols;c++) {
        batch->null_bitmap[c]=arena_calloc(a,(BATCH_SIZE+7)/8);
        switch(sc->cols[c].type) {
            case COL_INT64:  batch->values[c]=arena_alloc(a,BATCH_SIZE*sizeof(int64_t)); break;
            case COL_DOUBLE: batch->values[c]=arena_alloc(a,BATCH_SIZE*sizeof(double));  break;
            default:         batch->values[c]=arena_alloc(a,BATCH_SIZE*sizeof(char*));   break;
        }
    }

    int batch_row=0;
    while (batch_row<BATCH_SIZE && ctx->pos_file<ctx->npaths) {
        f=&ctx->files[ctx->pos_file];
        if (!f->fp || ctx->pos_rg>=f->nrgs) {
            ctx->pos_file++; ctx->pos_rg=0; ctx->pos_row=0; continue;
        }
        PqRowGroup *rg=&f->rgs[ctx->pos_rg];
        int64_t avail_in_rg=rg->num_rows-ctx->pos_row;
        int take=(int)(avail_in_rg<BATCH_SIZE-batch_row ? avail_in_rg : BATCH_SIZE-batch_row);
        if (take<=0) { ctx->pos_rg++; ctx->pos_row=0; continue; }

        /* Нужно убедиться что rg->ncols совпадает с f->ncols */
        if (rg->ncols!=ncols) { ctx->pos_rg++; ctx->pos_row=0; continue; }

        for (int c=0;c<ncols;c++)
            pq_read_col_range(f,ctx->pos_rg,c,ctx->pos_row,take,batch,c,batch_row,a);

        batch_row+=take;
        ctx->pos_row+=take;
        if (ctx->pos_row>=rg->num_rows) { ctx->pos_rg++; ctx->pos_row=0; }
    }

    batch->nrows=batch_row;
    *out_batch=batch;
    return (batch_row>0)?0:1;
}

const DfoConnector dfo_connector_entry = {
    .abi_version  = DFO_CONNECTOR_ABI_VERSION,
    .name         = "parquet",
    .version      = "0.1.0",
    .description  = "Apache Parquet v2 reader (PLAIN, GZIP/uncompressed)",
    .create       = pq_create,
    .destroy      = pq_destroy,
    .list_entities= pq_list_entities,
    .describe     = pq_describe,
    .read_batch   = pq_read_batch,
    .cdc_start    = NULL,
    .cdc_stop     = NULL,
    .ping         = pq_ping,
};
