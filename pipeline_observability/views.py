"""Queryable summary views over the run store.

Each function returns plain dicts/lists so the CLI, a dashboard, or a
notebook can consume them without depending on the store internals.

Views:
- ``recent_runs`` — latest N runs, newest first.
- ``pipeline_summary`` — one row per pipeline: run counts, success rate,
  average duration, last run time/status, total rows written.
- ``failure_report`` — failed runs in the last ``days`` days, with errors.
- ``freshness`` — hours since each pipeline's last *successful* run.
  A pipeline with no successful run reports ``hours_since_success=None``,
  which is the "never ran green" case — more alarming than merely stale.
- ``row_count_trend`` — rows_out over the last N runs of one pipeline,
  for spotting sudden drops (a run that writes 0 rows usually means the
  source was empty or the extract silently failed).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from .store import RunStore


def recent_runs(store: RunStore, limit: int = 20) -> list[dict[str, Any]]:
    return [_run_dict(r) for r in store.list_runs(limit=limit)]


def pipeline_summary(store: RunStore) -> list[dict[str, Any]]:
    rows = store.conn.execute(
        """
        SELECT
            pipeline,
            COUNT(*) AS total_runs,
            SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS succeeded,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running,
            AVG(CASE WHEN status != 'running'
                     THEN duration_seconds END) AS avg_duration_seconds,
            MAX(started_at) AS last_run_at,
            COALESCE(SUM(rows_out), 0) AS total_rows_out
        FROM runs
        GROUP BY pipeline
        ORDER BY pipeline
        """
    ).fetchall()
    out = []
    for r in rows:
        total = r["total_runs"]
        succeeded = r["succeeded"] or 0
        # Last status: status of the most recent run for this pipeline.
        last = store.conn.execute(
            "SELECT status FROM runs WHERE pipeline = ? "
            "ORDER BY started_at DESC LIMIT 1",
            (r["pipeline"],),
        ).fetchone()
        out.append(
            {
                "pipeline": r["pipeline"],
                "total_runs": total,
                "succeeded": succeeded,
                "failed": r["failed"] or 0,
                "running": r["running"] or 0,
                "success_rate": round(succeeded / total, 3) if total else None,
                "avg_duration_seconds": (
                    round(r["avg_duration_seconds"], 1)
                    if r["avg_duration_seconds"] is not None
                    else None
                ),
                "last_run_at": r["last_run_at"],
                "last_status": last["status"] if last else None,
                "total_rows_out": r["total_rows_out"],
            }
        )
    return out


def failure_report(store: RunStore, days: int = 7) -> list[dict[str, Any]]:
    rows = store.conn.execute(
        """
        SELECT * FROM runs
        WHERE status = 'failed'
          AND started_at >= datetime('now', ?)
        ORDER BY started_at DESC
        """,
        (f"-{days} days",),
    ).fetchall()
    from .store import _row_to_run  # local import avoids a cycle at module load

    return [_run_dict(_row_to_run(r)) for r in rows]


def freshness(store: RunStore, now: Optional[datetime] = None) -> list[dict[str, Any]]:
    """Hours since each pipeline's last successful run."""
    now = now or datetime.now(timezone.utc)
    pipelines = [
        r["pipeline"]
        for r in store.conn.execute("SELECT DISTINCT pipeline FROM runs")
    ]
    out = []
    for pipeline in sorted(pipelines):
        row = store.conn.execute(
            "SELECT started_at FROM runs "
            "WHERE pipeline = ? AND status = 'succeeded' "
            "ORDER BY started_at DESC LIMIT 1",
            (pipeline,),
        ).fetchone()
        if row is None:
            out.append(
                {
                    "pipeline": pipeline,
                    "last_success_at": None,
                    "hours_since_success": None,
                }
            )
            continue
        last = datetime.fromisoformat(row["started_at"])
        out.append(
            {
                "pipeline": pipeline,
                "last_success_at": row["started_at"],
                "hours_since_success": round(
                    (now - last).total_seconds() / 3600, 2
                ),
            }
        )
    return out


def row_count_trend(
    store: RunStore, pipeline: str, limit: int = 20
) -> list[dict[str, Any]]:
    runs = store.list_runs(pipeline=pipeline, limit=limit)
    return [
        {
            "run_id": r.run_id,
            "started_at": r.started_at,
            "status": r.status,
            "rows_out": r.rows_out,
        }
        for r in reversed(runs)  # oldest first: chronological trend
    ]


def _run_dict(run: Any) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "pipeline": run.pipeline,
        "status": run.status,
        "started_at": run.started_at,
        "ended_at": run.ended_at,
        "duration_seconds": run.duration_seconds,
        "rows_in": run.rows_in,
        "rows_out": run.rows_out,
        "error_message": run.error_message,
    }
