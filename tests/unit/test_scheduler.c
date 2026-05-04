#include "../../lib/scheduler/scheduler.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

static int plan=0,pass=0,fail=0;
#define ok(c,...) do{plan++;if(c){pass++;printf("ok %d — " __VA_ARGS__);puts("");}else{fail++;printf("not ok %d — " __VA_ARGS__);puts("");}}while(0)

int main(void) {
    puts("TAP version 14");

    int64_t now=(int64_t)time(NULL);
    /* every minute */
    int64_t next=cron_next("* * * * *",now);
    ok(next>now,"next minute > now");
    ok(next-now<=60,"within 60 seconds");

    /* hourly alias */
    int64_t nh=cron_next("@hourly",now);
    ok(nh>now,"@hourly next > now");

    /* specific minute */
    int64_t n30=cron_next("30 * * * *",now);
    struct tm tm; time_t t=(time_t)n30; gmtime_r(&t,&tm);
    ok(tm.tm_min==30,"cron '30 * * * *' lands on minute 30");

    /* pipeline json round-trip */
    Arena *a=arena_create(0);
    const char *json="{\"id\":\"p1\",\"name\":\"Test\",\"cron\":\"@daily\","
                     "\"enabled\":true,\"steps\":[{\"id\":\"s1\",\"name\":\"step\","
                     "\"connector_type\":\"csv\",\"target_table\":\"out\","
                     "\"connector_config\":\"\",\"transform_sql\":\"\",\"deps\":[]}]}";
    Pipeline p; memset(&p,0,sizeof(p));
    int rc=pipeline_from_json(&p,json);
    ok(rc==0,"pipeline_from_json ok");
    ok(strcmp(p.id,"p1")==0,"id parsed");
    ok(strcmp(p.name,"Test")==0,"name parsed");
    ok(p.nsteps==1,"one step");

    char *back=pipeline_to_json(&p,a);
    ok(back&&strlen(back)>0,"pipeline_to_json produces output");

    arena_destroy(a);
    printf("1..%d\n# pass=%d fail=%d\n",plan,pass,fail);
    return fail>0?1:0;
}
