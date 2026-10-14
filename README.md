# pipeline-observability

A small run-metadata store for data pipelines. Wrap a pipeline run, and it
records status, timing, row counts, and per-task events in SQLite. Summary
views answer the on-call questions: did the last run succeed, how long do
runs take, which pipelines are stale, and what failed recently.

No server, no ORM, no dependencies beyond the standard library. This is the
kind of observability layer you bolt onto a set of cron-driven pipelines
before (or instead of) standing up a full orchestrator.

## Quickstart

```bash
# Wrap a pipeline run (2 lines of instrumentation)
python - <<'EOF'
from pipeline_observability import RunStore, RunTracker

store = RunStore("runs.db")
with RunTracker(store, "orders_etl") as run:
    with run.task("extract") as task:
        rows = [{"id": 1}, {"id": 2}]  # your extract here
        task.rows_processed = len(rows)
    with run.task("load"):
        pass  # your load here
    run.rows_out = len(rows)
EOF
```

```bash
# Or drive it from the CLI
python -m pipeline_observability --db runs.db summary
python -m pipeline_observability --db runs.db failures --days 7
python -m pipeline_observability --db runs.db freshness
python -m pipeline_observability --db runs.db trend --pipeline orders_etl
```

Seeded demo (two weeks of history, three pipelines, a silent zero-row run
and a never-green pipeline), then inspect the views:

```bash
python examples/demo.py
python -m pipeline_observability --db demo.db summary
```

Example output (`summary` after the demo seed):

```
pipeline            runs  ok  fail  success_rate  avg_dur_s  last_run              last_status  rows_out
------------------  ----  --  ----  ------------  ---------  --------------------  -----------  --------
inventory_snapshot  1     0   1     0.0           5.0        2026-10-14T...      failed       0
orders_etl          16    15  1     0.938         121.1      2026-10-14T...      failed       142194
users_sync          5     4   1     0.8           38.6       2026-10-14T...      failed       18346
```

And `freshness` flags the pipeline that has never gone green:

```
pipeline            last_success_at       hours_since_success
------------------  --------------------  -------------------
inventory_snapshot  NEVER                 NEVER
orders_etl          2026-10-14T...        1.0
users_sync          2026-10-14T...        12.0
```

## How it works

Two tables in SQLite:

- `runs` — one row per pipeline run: `run_id`, `pipeline`, `status`
  (`running` / `succeeded` / `failed`), `started_at`, `ended_at`,
  `duration_seconds`, `rows_in`, `rows_out`, `error_message`, plus a JSON
  `metadata` blob for anything else.
- `task_events` — one row per task inside a run, same shape, keyed by
  `run_id`.

`RunTracker` is a context manager: normal exit marks the run `succeeded`,
an escaping exception marks it `failed` with the exception message and
re-raises. `run.task(name)` does the same per task, so a run that dies
mid-task records both failures. `views.py` is a set of plain functions
over the store returning dicts/lists:

| View | Answers |
| --- | --- |
| `pipeline_summary` | runs, success rate, avg duration, last status, total rows out per pipeline |
| `failure_report(days)` | failed runs in the window, with error messages |
| `freshness` | hours since each pipeline's last *successful* run (`None` = never green) |
| `row_count_trend` | `rows_out` over recent runs — catches silent zero-row "successes" |
| `recent_runs` | latest N runs, newest first |

## Design notes

- **SQLite, not Postgres.** The target is a single host running a handful
  of pipelines; SQLite gives you a durable, queryable store with zero
  ops. The schema is two tables and `init_db()` is idempotent.
- **Timestamps are ISO-8601 UTC strings**, stored as text. That keeps
  comparisons, `ORDER BY started_at`, and SQLite's `datetime()` modifiers
  working without a type layer, and makes the DB greppable.
- **Duration is derived**, not measured by the caller: `finish_run`
  computes it from `started_at`/`ended_at`, so clock skew inside a run
  can't produce a negative duration from two inconsistent readings.
- **A "succeeded" run writing zero rows is still recorded as success.**
  The store doesn't decide what success means for your pipeline — that's
  the trend view's job. Treating a zero-row run as a failure at write
  time would hide legitimate empty-source days.
- **Not a replacement for an orchestrator.** Airflow/Prefect give you
  scheduling, retries, and a UI. This is the metadata layer underneath:
  the same tables you'd build if you needed run history queryable from a
  notebook or a cron health check without standing up more infra.
- **Demo data is deterministic** (`random.Random(42)`, fixed run ids), so
  the example output above is reproducible.

## Tests

```bash
pip install pytest
python -m pytest -q
```

Covers the store (CRUD, filters, idempotent init), the tracker (success,
failure propagation, task failure without run failure, explicit run ids),
the views (summary math, failure window, freshness never-green case,
chronological trend, seeded demo shape), and the CLI end to end.

## Layout

```
pipeline_observability/
  store.py    # RunStore: SQLite schema + CRUD for runs and task events
  tracker.py  # RunTracker / TaskTracker context managers
  views.py    # summary, failures, freshness, row-count trend
  cli.py      # python -m pipeline_observability ...
  seed.py     # deterministic demo data
examples/demo.py   # seed + print every view
tests/             # pytest suite
```
