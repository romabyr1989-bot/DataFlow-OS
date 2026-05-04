#pragma once
#include "../core/arena.h"
#include "../storage/storage.h"
#include <stdbool.h>

/* ── AST ── */
typedef enum {
    EXPR_COL, EXPR_STAR, EXPR_LITERAL_INT, EXPR_LITERAL_FLOAT,
    EXPR_LITERAL_STR, EXPR_LITERAL_NULL, EXPR_LITERAL_BOOL,
    EXPR_FUNC, EXPR_BINOP, EXPR_UNOP, EXPR_ALIAS, EXPR_SUBQUERY,
    EXPR_CASE, EXPR_LIST, EXPR_WINDOW
} ExprType;

typedef enum {
    OP_EQ, OP_NE, OP_LT, OP_LE, OP_GT, OP_GE,
    OP_AND, OP_OR, OP_NOT,
    OP_ADD, OP_SUB, OP_MUL, OP_DIV, OP_MOD,
    OP_LIKE, OP_ILIKE, OP_NOT_LIKE, OP_NOT_ILIKE,
    OP_IS_NULL, OP_IS_NOT_NULL,
    OP_IN, OP_NOT_IN, OP_BETWEEN, OP_NOT_BETWEEN,
    OP_CONCAT
} OpType;

typedef struct Expr Expr;
typedef struct Stmt Stmt;
typedef struct WindowSpec WindowSpec;

struct Expr {
    ExprType    type;
    WindowSpec *win_spec;   /* non-NULL for EXPR_WINDOW */
    union {
        struct { const char *table; const char *name; };    /* EXPR_COL */
        int64_t     ival;                                   /* LITERAL_INT */
        double      fval;                                   /* LITERAL_FLOAT */
        const char *sval;                                   /* LITERAL_STR */
        bool        bval;                                   /* LITERAL_BOOL */
        struct { OpType op; Expr *left; Expr *right; };     /* BINOP/UNOP */
        struct { const char *func_name; Expr **args; int nargs; }; /* FUNC */
        struct { Expr *expr; const char *alias; };          /* ALIAS */
        struct { Expr **items; int nitems; };               /* LIST */
        struct {                                            /* CASE */
            Expr  *case_op;     /* NULL = searched CASE */
            Expr **whens;
            Expr **thens;
            int    nwhens;
            Expr  *else_expr;
        };
        Stmt *subq;                                         /* SUBQUERY */
    };
};

typedef enum { JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_FULL, JOIN_CROSS } JoinType;

typedef struct {
    const char *table;      /* NULL if subquery */
    const char *alias;
    JoinType    join_type;
    Expr       *on;
    Stmt       *subquery;   /* non-NULL = derived table */
} FromItem;

typedef struct {
    Expr       *expr;
    bool        desc;
    bool        nulls_last;
} OrderItem;

typedef enum { WF_ROWS, WF_RANGE } WinFrameType;

typedef enum {
    WBOUND_UNBOUNDED_PREC,
    WBOUND_N_PREC,
    WBOUND_CURRENT_ROW,
    WBOUND_N_FOLL,
    WBOUND_UNBOUNDED_FOLL
} WinBoundKind;

typedef struct { WinBoundKind kind; int64_t n; } WinFrameBound;

struct WindowSpec {
    Expr         **partition_by;
    int            npartition;
    OrderItem     *order_by;
    int            norder;
    WinFrameType   frame_type;
    WinFrameBound  frame_start;
    WinFrameBound  frame_end;
    bool           has_frame;
};

typedef struct {
    const char  *name;
    struct SelectStmt_s *body;
} CTE;

typedef struct SelectStmt_s {
    /* WITH */
    CTE        *ctes;
    int         nctes;
    /* SELECT */
    Expr      **select_list;
    int         nselect;
    bool        distinct;
    /* FROM */
    FromItem   *from;
    int         nfrom;
    /* WHERE */
    Expr       *where;
    /* GROUP BY */
    Expr      **group_by;
    int         ngroup;
    /* HAVING */
    Expr       *having;
    /* ORDER BY */
    OrderItem  *order_by;
    int         norder;
    /* LIMIT / OFFSET */
    int64_t     limit;
    int64_t     offset;
} SelectStmt;

typedef struct {
    const char  *table;
    const char **columns;
    Expr       **values;
    int          ncols;
} InsertStmt;

typedef enum { SET_UNION, SET_UNION_ALL, SET_INTERSECT, SET_EXCEPT } SetOpType;

typedef enum { STMT_SELECT, STMT_INSERT, STMT_SET_OP, STMT_UNKNOWN } StmtType;

struct Stmt {
    StmtType     type;
    union {
        SelectStmt select;
        InsertStmt insert;
        struct {
            SetOpType  set_op;
            Stmt      *set_left;
            Stmt      *set_right;
        };
    };
    const char  *error;
};

/* ── Public API ── */
Stmt  *sql_parse(Arena *a, const char *query, size_t len);
char  *stmt_to_str(Arena *a, const Stmt *s);
void   stmt_dump(const Stmt *s);

/* ── Plan nodes ── */
typedef enum {
    PLAN_SCAN, PLAN_FILTER, PLAN_PROJECT, PLAN_JOIN,
    PLAN_SORT, PLAN_LIMIT, PLAN_AGG, PLAN_DISTINCT,
    PLAN_SET_OP, PLAN_WINDOW
} PlanNodeType;

typedef struct PlanNode PlanNode;
struct PlanNode {
    PlanNodeType type;
    PlanNode    *left;
    PlanNode    *right;
    const char  *table;
    Expr        *predicate;
    Expr       **exprs;
    int          nexprs;
    OrderItem   *order;
    int          norder;
    int64_t      limit;
    int64_t      offset;
    JoinType     join_type;
    Expr        *join_on;
    Expr       **group_keys;
    int          ngroup_keys;
    Expr       **agg_exprs;
    int          nagg_exprs;
    SetOpType    set_op;
    Expr       **window_exprs;
    int          nwindow_exprs;
};

PlanNode *sql_plan(Arena *a, const Stmt *s);
