# Audit Logging

## Overview

Every significant platform action is recorded in an in-memory ring buffer
(1 024 slots) and asynchronously flushed to `audit.db` (SQLite). Audit logging
is **non-blocking** and never adds latency to the request path.

## Configuration

```json
{
  "audit_log_file": "/var/log/dfo_audit.log"
}
```

`audit_log_file` is optional. When set, events are also appended to the file
in addition to `audit.db`.

## Event Types

| Event type      | Triggered by                              |
|----------------|-------------------------------------------|
| `QUERY`         | Any SQL query executed via the API        |
| `INGEST`        | Batch data ingestion                      |
| `AUTH_FAIL`     | Failed authentication attempt             |
| `SCHEMA_CHANGE` | Table creation, alteration, or deletion   |

## Event Fields

| Field           | Type   | Description                        |
|----------------|--------|------------------------------------|
| `event_type`    | string | One of the types listed above      |
| `username`      | string | User who triggered the event       |
| `client_ip`     | string | Remote IP address                  |
| `resource`      | string | Table name or endpoint path        |
| `action_detail` | string | Query text or action description   |
| `ts`            | int    | Unix timestamp (seconds)           |

## API

Requires `Authorization: Bearer <token>`.

```
GET /api/audit?limit=N&offset=M
```

Example:

```bash
curl "/api/audit?limit=50&offset=0" \
  -H "Authorization: Bearer <token>"
```

Response is a JSON array of event objects ordered by most recent first.

## Implementation Notes

- Ring buffer holds 1 024 events in memory before the oldest slot is overwritten.
- A background worker thread drains the buffer and writes to SQLite.
- If `audit.db` is unreachable at startup the server continues without audit persistence
  (events still accumulate in the ring buffer).
