# Role-Based Access Control (RBAC)

## Overview

RBAC restricts which users can perform which SQL operations on which tables.
Policies are stored in `rbac.db` (SQLite). The check runs before query execution;
a denied request returns HTTP 403.

**Admin is always a superuser** — RBAC is never evaluated for the admin account.

## Enabling RBAC

```json
{
  "rbac_enabled": true
}
```

## Policy Fields

| Field        | Type   | Description                                      |
|-------------|--------|--------------------------------------------------|
| `username`   | string | Target user                                      |
| `table_name` | string | Target table (`*` for all)                       |
| `action`     | string | `SELECT`, `INSERT`, `UPDATE`, or `DELETE`        |
| `allow`      | bool   | `true` = permit, `false` = explicit deny         |
| `row_filter` | string | Optional glob matched against first column value |

## API

All endpoints require `Authorization: Bearer <token>`.

| Method   | Path                   | Description          |
|----------|------------------------|----------------------|
| `GET`    | `/api/rbac/policies`   | List all policies    |
| `POST`   | `/api/rbac/policies`   | Create a policy      |
| `DELETE` | `/api/rbac/policies`   | Delete a policy      |

### Create a policy

```bash
curl -X POST /api/rbac/policies \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"username":"analyst","table_name":"sales","action":"SELECT","allow":true,"row_filter":"region_west*"}'
```

## Row-Level Security

Set `row_filter` to a glob pattern. The filter is matched against the **first column
value** of each candidate row. Rows that do not match are excluded before results
are returned.

```json
{"username":"analyst","table_name":"sales","action":"SELECT","allow":true,"row_filter":"region_west*"}
```

## Evaluation Order

1. If `rbac_enabled` is `false` — allow all.
2. If user is admin — allow all.
3. Look up matching policy (`username` + `table_name` + `action`).
4. If no policy found — deny by default.
5. If `allow: false` — return 403.
6. Apply `row_filter` if present.
