"""Seed realistic-looking demo data into a store.

Three pipelines, ~2 weeks of history, a couple of instructive failures:
an extract that died on a timeout, and a run that "succeeded" while writing
zero rows (the silent failure the row-count trend view exists to catch).
Timestamps are relative to now so the freshness view shows real values.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from .store import RunStore, TaskEvent, utcnow_iso


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()


def seed_demo(store: RunStore) -> None:
    rng = random.Random(42)
    now = datetime.now(timezone.utc)

    # (pipeline, runs_ago_days, duration_s, rows_in, rows_out, status, error)
    runs: list[tuple[str, float, float, int, int, str, str | None]] = []
    for days_ago in [13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0.1]:
        runs.append(
            ("orders_etl", days_ago, rng.uniform(90, 180),
             rng.randint(9000, 12000), rng.randint(9000, 12000),
             "succeeded", None)
        )
    # The interesting ones, appended after the healthy history:
    runs.append(("orders_etl", 0.04, 45.0, 10500, 0, "succeeded", None))  # silent 0-row run
    runs.append(("orders_etl", 0.02, 12.0, 0, 0, "failed",
                 "TimeoutError: extract timed out after 30s"))

    for days_ago in [6, 4, 2, 0.5]:
        runs.append(
            ("users_sync", days_ago, rng.uniform(20, 60),
             rng.randint(4000, 5000), rng.randint(4000, 5000),
             "succeeded", None)
        )
    runs.append(("users_sync", 0.3, 8.0, 0, 0, "failed",
                 "ConnectionError: source warehouse unreachable"))

    # inventory_snapshot has never had a successful run: freshness must flag it.
    runs.append(("inventory_snapshot", 0.2, 5.0, 0, 0, "failed",
                 "ValueError: unexpected schema: column 'sku' missing"))

    for i, (pipeline, days_ago, dur, rows_in, rows_out, status, error) in enumerate(runs):
        end = now - timedelta(days=days_ago)
        start = end - timedelta(seconds=dur)
        run = store.start_run(
            pipeline,
            run_id=f"demo-{i:03d}",
            metadata={"seeded": True},
            started_at=_iso(start),
        )
        # One task event per run, so the task table is exercised too.
        store.record_task(
            TaskEvent(
                run_id=run.run_id,
                task_name="extract_and_load",
                status=status,
                started_at=_iso(start),
                ended_at=_iso(end),
                duration_seconds=dur,
                rows_processed=rows_out,
                error_message=error,
            )
        )
        store.finish_run(
            run.run_id, status,
            rows_in=rows_in, rows_out=rows_out,
            error_message=error, ended_at=_iso(end),
        )
