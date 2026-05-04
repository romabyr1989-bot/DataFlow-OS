#include "../../lib/sql_parser/sql.h"
#include "../../lib/core/arena.h"
#include <stdio.h>
#include <string.h>

static int plan=0,pass=0,fail=0;
#define ok(c,...) do{plan++;if(c){pass++;printf("ok %d — " __VA_ARGS__);puts("");}else{fail++;printf("not ok %d — " __VA_ARGS__);puts("");}}while(0)

static Stmt *parse(Arena *a, const char *q){return sql_parse(a,q,strlen(q));}

int main(void) {
    puts("TAP version 14");
    Arena *a = arena_create(0);

    /* basic select */
    Stmt *s1=parse(a,"SELECT * FROM employees");
    ok(s1->type==STMT_SELECT,"basic select parsed");
    ok(s1->select.nfrom==1,"one from table");
    ok(strcmp(s1->select.from[0].table,"employees")==0,"table name correct");
    ok(s1->select.nselect==1,"one select expr (star)");

    /* with where */
    Stmt *s2=parse(a,"SELECT id, name FROM users WHERE age > 18 LIMIT 10");
    ok(s2->type==STMT_SELECT,"select with where");
    ok(s2->select.nselect==2,"two select cols");
    ok(s2->select.where!=NULL,"has where clause");
    ok(s2->select.limit==10,"limit=10");

    /* group by + having */
    Stmt *s3=parse(a,"SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 5");
    ok(s3->type==STMT_SELECT,"group by parsed");
    ok(s3->select.ngroup==1,"one group key");
    ok(s3->select.having!=NULL,"has having");

    /* order by */
    Stmt *s4=parse(a,"SELECT name FROM t ORDER BY name DESC, age ASC");
    ok(s4->select.norder==2,"two order items");
    ok(s4->select.order_by[0].desc==true,"first order DESC");
    ok(s4->select.order_by[1].desc==false,"second order ASC");

    /* join */
    Stmt *s5=parse(a,"SELECT a.id FROM a JOIN b ON a.id=b.aid");
    ok(s5->type==STMT_SELECT,"join parsed");
    ok(s5->select.nfrom>=2,"two from items after join");

    /* planner */
    PlanNode *plan5 = sql_plan(a,s2);
    ok(plan5!=NULL,"planner produces plan");

    arena_destroy(a);
    printf("1..%d\n# pass=%d fail=%d\n",plan,pass,fail);
    return fail>0?1:0;
}
