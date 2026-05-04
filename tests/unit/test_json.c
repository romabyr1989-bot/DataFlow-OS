#include "../../lib/core/json.h"
#include "../../lib/core/arena.h"
#include <stdio.h>
#include <string.h>
#include <math.h>

static int plan=0,pass=0,fail=0;
#define ok(c,...) do{plan++;if(c){pass++;printf("ok %d — " __VA_ARGS__);puts("");}else{fail++;printf("not ok %d — " __VA_ARGS__);puts("");}}while(0)

int main(void) {
    puts("TAP version 14");
    Arena *a = arena_create(0);

    /* builder */
    JBuf jb; jb_init(&jb,a,64);
    jb_obj_begin(&jb);
    jb_key(&jb,"n"); jb_int(&jb,42);
    jb_key(&jb,"s"); jb_str(&jb,"hello");
    jb_key(&jb,"b"); jb_bool(&jb,true);
    jb_key(&jb,"arr"); jb_arr_begin(&jb);
    jb_int(&jb,1); jb_int(&jb,2); jb_int(&jb,3);
    jb_arr_end(&jb);
    jb_obj_end(&jb);
    const char *built = jb_done(&jb);
    ok(built && strlen(built)>0, "builder produces output: %s", built);

    /* parser round-trip */
    JVal *root = json_parse(a, built, strlen(built));
    ok(root && root->type==JV_OBJECT, "parsed as object");
    ok(json_int(json_get(root,"n"),0)==42, "int field n=42");
    ok(strcmp(json_str(json_get(root,"s"),""),"hello")==0, "string field s=hello");
    ok(json_bool(json_get(root,"b"),false)==true, "bool field b=true");

    JVal *arr = json_get(root,"arr");
    ok(arr && arr->type==JV_ARRAY && arr->nitems==3, "array has 3 items");

    /* special strings */
    JBuf jb2; jb_init(&jb2,a,64);
    jb_obj_begin(&jb2);
    jb_key(&jb2,"q"); jb_str(&jb2,"say \"hi\"");
    jb_obj_end(&jb2);
    const char *escaped = jb_done(&jb2);
    JVal *r2 = json_parse(a, escaped, strlen(escaped));
    ok(strcmp(json_str(json_get(r2,"q"),""),"say \"hi\"")==0, "escaped strings round-trip");

    /* null value */
    const char *nulljson = "{\"x\":null}";
    JVal *rn = json_parse(a, nulljson, strlen(nulljson));
    JVal *x  = json_get(rn,"x");
    ok(x && x->type==JV_NULL, "null parses correctly");

    arena_destroy(a);
    printf("1..%d\n# pass=%d fail=%d\n",plan,pass,fail);
    return fail>0?1:0;
}
