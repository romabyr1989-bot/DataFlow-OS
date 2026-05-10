# Python pipeline steps

A pipeline step that hands data to a `python3` subprocess and reads the
result back. The user code operates on a **pandas `DataFrame`** named
`df`; whatever `df` looks like at the end of the script is what gets
ingested into `target_table`.

This unlocks transformations that aren't reachable from DFO's SQL
engine — joins on Python objects, ML inference, regex-heavy munging,
calls into pandas/numpy, etc.

## Requirements

- `python3` on `$PATH` (the gateway uses `execlp`).
- `pandas` available to that interpreter (`python3 -m pip install pandas`).

If pandas isn't installed, the step fails fast with a helpful message
in `pipelines.error_msg` (and `stderr` tail in the run log).

## Step shape

A Python step is just a regular `PipelineStep` with two extra fields:

```json
{
  "id": "score_filter",
  "name": "double-and-filter",
  "transform_sql":      "SELECT user_id, score FROM raw_events",
  "python_code":        "df['score'] = df['score'].astype(int) * 2\ndf = df[df['score'] > 60]",
  "python_timeout_sec": 60,
  "target_table":       "high_scorers"
}
```

| Field                | Purpose                                                                           |
|----------------------|-----------------------------------------------------------------------------------|
| `python_code`        | User Python (≤ 8 KB). Operates on `df`. Required for the step to be a Python step.|
| `python_timeout_sec` | Wall-clock deadline. Default 300. After this the gateway sends `SIGKILL`.         |
| `transform_sql`      | Optional. If set, its result is fed to the script as CSV on **stdin**.            |
| `target_table`       | Optional. If set, the final `df` is written there.                                |

## Execution model

```
                ┌──────────────┐
   transform_sql│ exec_stmt    │ → CSV bytes
                └─────┬────────┘
                      │ stdin
                      ▼
            ┌──────────────────┐  stderr ─► error tail (1 KB)
            │ python3 -c <wrap>│
            └─────┬────────────┘
                  │ stdout
                  ▼
              CSV bytes ─► write_rs_to_table → target_table
```

The wrapper script DFO compiles around your code is roughly:

```python
import sys, io, pandas as pd
_csv_in = sys.stdin.read()
df = pd.read_csv(io.StringIO(_csv_in)) if _csv_in.strip() else pd.DataFrame()
# ── user code ──
<your code>
# ── /user code ──
df.to_csv(sys.stdout, index=False)
```

So:
- if `transform_sql` is empty, `df` starts as an empty DataFrame
- you can construct `df` from scratch (e.g. `df = pd.DataFrame({...})`)
- output goes through `to_csv`, so non-string dtypes round-trip via CSV
  (pandas re-infers types when ingested — that's usually fine)

## Failure modes

| What                          | Result                                                |
|-------------------------------|-------------------------------------------------------|
| `python3` not on `$PATH`      | step fails, `error_msg` says exit=127                 |
| Missing pandas                | step fails, helpful pip-install hint in `error_msg`   |
| Syntax error in `python_code` | step fails, last 1 KB of stderr in `error_msg`        |
| Script exceeds timeout        | gateway sends `SIGKILL`, step fails with timeout msg  |
| Output CSV malformed          | step fails (no header / unparseable rows)             |

Failures use the same retry policy as any other step (`max_retries`,
`retry_delay_sec`). Set `max_retries: 0` if a deterministic failure
should not be retried.

## UI

The pipeline builder has a connector dropdown option **🐍 Python-скрипт
(pandas DataFrame)**. Selecting it:

- clears `connector_type` / `connector_config`
- exposes a code textarea for `python_code`
- exposes a numeric input for `python_timeout_sec`
- a starter snippet is pre-filled the first time you select it

## Security

This step runs arbitrary code as the gateway user. It is gated by:

- the JWT/RBAC required to *create* a pipeline (a user has to be allowed
  to write pipelines in the first place)
- the timeout — the script is killed at the deadline regardless

There is **no** sandbox / chroot / seccomp around the child. If you
deploy DFO in a multi-tenant context, restrict pipeline-write
permissions to trusted users (RBAC: `PIPELINE_WRITE`).

## Limits

- `python_code` ≤ 8192 bytes (`PipelineStep.python_code`)
- input + output CSV go through pipes — no hard cap, but everything is
  buffered in the gateway's arena, so very large frames (≫ 100 MB)
  should use a `connector` step instead
- one subprocess per step (no pool / no warm interpreter)
