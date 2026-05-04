#include "scheduler.h"
#include "../core/log.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <stdio.h>

#ifdef __APPLE__
/* timegm is not exposed under strict POSIX on macOS — provide it inline */
static time_t timegm(struct tm *tm) {
    int y = tm->tm_year + 1900, m = tm->tm_mon + 1;
    if (m <= 2) { m += 12; y--; }
    long d = 365L*y + y/4 - y/100 + y/400 + (153*m - 457)/5 + tm->tm_mday - 719469L;
    return (time_t)(d * 86400L + tm->tm_hour * 3600L + tm->tm_min * 60L + tm->tm_sec);
}
#endif

/* ── Cron parser ── */
typedef struct { int vals[60]; int n; bool star; } CronField;

static void parse_field(const char *s, CronField *f, int lo, int hi) {
    f->n = 0;
    if (strcmp(s,"*")==0) { f->star=true; for(int i=lo;i<=hi;i++) f->vals[f->n++]=i; return; }
    f->star = false;
    /* handle comma-separated list */
    char buf[64]; strncpy(buf,s,sizeof(buf)-1); buf[sizeof(buf)-1]='\0';
    char *tok=strtok(buf,",");
    while(tok) {
        char *slash=strchr(tok,'/');
        char *dash=strchr(tok,'-');
        if(slash) {
            int start=atoi(tok), step=atoi(slash+1);
            if(step<1)step=1;
            for(int v=start;v<=hi;v+=step) f->vals[f->n++]=v;
        } else if(dash) {
            int a=atoi(tok), b=atoi(dash+1);
            for(int v=a;v<=b;v++) f->vals[f->n++]=v;
        } else {
            int v=atoi(tok); if(v>=lo&&v<=hi) f->vals[f->n++]=v;
        }
        tok=strtok(NULL,",");
    }
}

static bool field_has(CronField *f, int v) {
    for(int i=0;i<f->n;i++) if(f->vals[i]==v) return true;
    return false;
}

int64_t cron_next(const char *expr, int64_t after) {
    /* Handle aliases */
    if(strcmp(expr,"@hourly")==0)   expr="0 * * * *";
    if(strcmp(expr,"@daily")==0)    expr="0 0 * * *";
    if(strcmp(expr,"@weekly")==0)   expr="0 0 * * 0";
    if(strcmp(expr,"@monthly")==0)  expr="0 0 1 * *";
    if(strcmp(expr,"@yearly")==0)   expr="0 0 1 1 *";
    if(strcmp(expr,"@reboot")==0)   return after; /* triggers once; scheduler pins next_run=INT64_MAX after */

    char fields[5][64];
    if(sscanf(expr,"%63s %63s %63s %63s %63s",fields[0],fields[1],fields[2],fields[3],fields[4])!=5)
        return -1;

    CronField min,hr,dom,mon,dow;
    parse_field(fields[0],&min,0,59);
    parse_field(fields[1],&hr,0,23);
    parse_field(fields[2],&dom,1,31);
    parse_field(fields[3],&mon,1,12);
    parse_field(fields[4],&dow,0,6);

    time_t t = (time_t)(after + 60); /* start from next minute */
    struct tm tm; gmtime_r(&t,&tm); tm.tm_sec=0;

    for(int iter=0; iter<527040; iter++) { /* max 1 year of minutes */
        if(field_has(&mon,tm.tm_mon+1) &&
           field_has(&dom,tm.tm_mday) &&
           field_has(&dow,tm.tm_wday) &&
           field_has(&hr,tm.tm_hour) &&
           field_has(&min,tm.tm_min)) {
            return (int64_t)timegm(&tm);
        }
        tm.tm_min++;
        if(tm.tm_min>=60){tm.tm_min=0;tm.tm_hour++;}
        if(tm.tm_hour>=24){tm.tm_hour=0;tm.tm_mday++;}
        /* simple overflow handling */
        if(tm.tm_mday>28){time_t t2=timegm(&tm);gmtime_r(&t2,&tm);tm.tm_sec=0;}
    }
    return -1;
}

/* ── DAG topological sort (Kahn) ── */
static int topo_sort(Pipeline *p, int *order) {
    int indegree[MAX_STEPS] = {0};
    for(int i=0;i<p->nsteps;i++)
        for(int j=0;j<p->steps[i].ndeps;j++)
            indegree[p->steps[i].deps[j]]++;
    int queue[MAX_STEPS], qh=0, qt=0, n=0;
    for(int i=0;i<p->nsteps;i++) if(!indegree[i]) queue[qt++]=i;
    while(qh<qt) {
        int u=queue[qh++]; order[n++]=u;
        for(int i=0;i<p->nsteps;i++)
            for(int j=0;j<p->steps[i].ndeps;j++)
                if(p->steps[i].deps[j]==u && --indegree[i]==0) queue[qt++]=i;
    }
    return n == p->nsteps ? 0 : -1; /* -1 = cycle */
}

/* ── JSON serialization ── */
/* Serialize any JVal back to compact JSON — used to preserve object-valued fields */
static void jval_write(JBuf *jb, JVal *v) {
    if (!v) { jb_null(jb); return; }
    switch (v->type) {
        case JV_NULL:    jb_null(jb); break;
        case JV_BOOL:    jb_bool(jb, v->b); break;
        case JV_NUMBER:  jb_double(jb, v->n); break;
        case JV_STRING:  jb_strn(jb, v->s, v->len); break;
        case JV_ARRAY:
            jb_arr_begin(jb);
            for (size_t i = 0; i < v->nitems; i++) jval_write(jb, v->items[i]);
            jb_arr_end(jb);
            break;
        case JV_OBJECT:
            jb_obj_begin(jb);
            for (size_t i = 0; i < v->nkeys; i++) {
                jb_key(jb, v->keys[i]);
                jval_write(jb, v->vals[i]);
            }
            jb_obj_end(jb);
            break;
        default: jb_null(jb); break;
    }
}

static const char *jval_to_json(JVal *v, Arena *a) {
    if (!v) return "null";
    JBuf jb; jb_init(&jb, a, 256);
    jval_write(&jb, v);
    return jb_done(&jb);
}

/* Copy JVal field into a fixed-size char buffer.
 * If the field is a JSON object/array, serialise it back to JSON first. */
static void copy_jval_str(JVal *v, char *dst, size_t dstsz, Arena *a) {
    if (!v) { dst[0] = '\0'; return; }
    if (v->type == JV_STRING) {
        size_t n = v->len < dstsz-1 ? v->len : dstsz-1;
        memcpy(dst, v->s, n); dst[n] = '\0';
    } else if (v->type == JV_OBJECT || v->type == JV_ARRAY) {
        const char *s = jval_to_json(v, a);
        strncpy(dst, s, dstsz-1); dst[dstsz-1] = '\0';
    } else {
        const char *s = json_str(v, "");
        strncpy(dst, s, dstsz-1); dst[dstsz-1] = '\0';
    }
}

int pipeline_from_json(Pipeline *p, const char *json) {
    Arena *a = arena_create(32768);
    JVal *root = json_parse(a, json, strlen(json));
    if (!root || root->type != JV_OBJECT) { arena_destroy(a); return -1; }

    strncpy(p->id,   json_str(json_get(root,"id"),""),  sizeof(p->id)-1);
    strncpy(p->name, json_str(json_get(root,"name"),""),sizeof(p->name)-1);
    strncpy(p->cron, json_str(json_get(root,"cron"),""),sizeof(p->cron)-1);
    p->enabled = json_bool(json_get(root,"enabled"),true);

    JVal *steps = json_get(root,"steps");
    if(steps && steps->type==JV_ARRAY) {
        p->nsteps=(int)steps->nitems;
        if(p->nsteps>MAX_STEPS) p->nsteps=MAX_STEPS;
        for(int i=0;i<p->nsteps;i++){
            JVal *s=steps->items[i]; PipelineStep *st=&p->steps[i];
            memset(st,0,sizeof(*st));
            strncpy(st->id,  json_str(json_get(s,"id"),""),  sizeof(st->id)-1);
            strncpy(st->name,json_str(json_get(s,"name"),""),sizeof(st->name)-1);
            strncpy(st->connector_type, json_str(json_get(s,"connector_type"),""), sizeof(st->connector_type)-1);
            copy_jval_str(json_get(s,"connector_config"), st->connector_config, sizeof(st->connector_config), a);
            strncpy(st->transform_sql,   json_str(json_get(s,"transform_sql"),""),   sizeof(st->transform_sql)-1);
            strncpy(st->target_table,    json_str(json_get(s,"target_table"),""),    sizeof(st->target_table)-1);
            JVal *deps=json_get(s,"deps");
            if(deps&&deps->type==JV_ARRAY)
                for(int j=0;j<(int)deps->nitems&&j<MAX_STEPS;j++)
                    st->deps[st->ndeps++]=(int)json_int(deps->items[j],0);
        }
    }
    arena_destroy(a);
    return 0;
}

char *pipeline_to_json(const Pipeline *p, Arena *a) {
    JBuf jb; jb_init(&jb,a,4096);
    jb_obj_begin(&jb);
    jb_key(&jb,"id");      jb_str(&jb,p->id);
    jb_key(&jb,"name");    jb_str(&jb,p->name);
    jb_key(&jb,"cron");    jb_str(&jb,p->cron);
    jb_key(&jb,"enabled"); jb_bool(&jb,p->enabled);
    jb_key(&jb,"last_run");jb_int(&jb,p->last_run);
    jb_key(&jb,"next_run");jb_int(&jb,p->next_run);
    jb_key(&jb,"status");  jb_int(&jb,(int)p->run_status);
    jb_key(&jb,"steps"); jb_arr_begin(&jb);
    for(int i=0;i<p->nsteps;i++){
        const PipelineStep *st=&p->steps[i];
        jb_obj_begin(&jb);
        jb_key(&jb,"id");               jb_str(&jb,st->id);
        jb_key(&jb,"name");             jb_str(&jb,st->name);
        jb_key(&jb,"connector_type");   jb_str(&jb,st->connector_type);
        jb_key(&jb,"connector_config"); jb_str(&jb,st->connector_config);
        jb_key(&jb,"transform_sql");    jb_str(&jb,st->transform_sql);
        jb_key(&jb,"target_table");     jb_str(&jb,st->target_table);
        jb_key(&jb,"status");           jb_int(&jb,(int)st->status);
        jb_key(&jb,"deps"); jb_arr_begin(&jb);
        for(int j=0;j<st->ndeps;j++) jb_int(&jb,st->deps[j]);
        jb_arr_end(&jb);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);
    jb_obj_end(&jb);
    return (char*)jb_done(&jb);
}

/* ── Scheduler thread ── */
static void *sched_loop(void *arg) {
    Scheduler *s = arg;
    LOG_INFO("scheduler started");
    while(s->running) {
        sleep(30);
        if(!s->running) break;
        int64_t now = (int64_t)time(NULL);
        pthread_mutex_lock(&s->mu);
        for(int i=0;i<s->npipelines;i++){
            Pipeline *p=&s->pipelines[i];
            if(!p->enabled) continue;
            if(p->next_run==0 && p->cron[0])
                p->next_run=cron_next(p->cron,now);
            if(p->next_run>0 && now>=p->next_run && p->run_status!=RUN_RUNNING){
                p->last_run=now;
                /* @reboot runs once only */
                if(strcmp(p->cron,"@reboot")==0)
                    p->next_run=INT64_MAX;
                else
                    p->next_run=cron_next(p->cron,now);
                p->run_status=RUN_RUNNING;
                LOG_INFO("scheduler triggering pipeline %s", p->id);
                if(s->on_run) s->on_run(p, s->on_run_data);
            }
        }
        pthread_mutex_unlock(&s->mu);
    }
    return NULL;
}

Scheduler *scheduler_create(RunCallback cb, void *ud) {
    Scheduler *s=calloc(1,sizeof(Scheduler));
    pthread_mutex_init(&s->mu,NULL);
    s->on_run=cb; s->on_run_data=ud;
    return s;
}

void scheduler_add(Scheduler *s, Pipeline *p) {
    pthread_mutex_lock(&s->mu);
    if(s->npipelines<256) s->pipelines[s->npipelines++]=*p;
    pthread_mutex_unlock(&s->mu);
}

void scheduler_remove(Scheduler *s, const char *id) {
    pthread_mutex_lock(&s->mu);
    for(int i=0;i<s->npipelines;i++)
        if(strcmp(s->pipelines[i].id,id)==0){
            s->pipelines[i]=s->pipelines[--s->npipelines]; break;
        }
    pthread_mutex_unlock(&s->mu);
}

Pipeline *scheduler_find(Scheduler *s, const char *id) {
    pthread_mutex_lock(&s->mu);
    Pipeline *found = NULL;
    for(int i=0;i<s->npipelines;i++)
        if(strcmp(s->pipelines[i].id,id)==0){ found=&s->pipelines[i]; break; }
    pthread_mutex_unlock(&s->mu);
    return found;
}

void scheduler_start(Scheduler *s) { s->running=1; pthread_create(&s->thread,NULL,sched_loop,s); }
void scheduler_stop(Scheduler *s)  { s->running=0; pthread_join(s->thread,NULL); }
