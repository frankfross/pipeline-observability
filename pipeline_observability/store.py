"""SQLite-backed store for pipeline run metadata.

Schema (two tables, deliberately boring):

    runs
        run_id         TEXT PRIMARY KEY   -- caller-supplied or generated UUID
        pipeline       TEXT NOT NULL      -- e.g. "orders_etl"
        status         TEXT NOT NULL      -- running | succeeded | failed
        started_at     TEXT NOT NULL      -- ISO-8601 UTC
        ended_at       TEXT               -- NULL while running
        duration_seconds REAL            -- NULL while running
        rows_in        INTEGER            -- rows read, if the pipeline reports it
        rows_out       INTEGER            -- rows written
        error_message  TEXT               -- populated on failure
        metadata       TEXT               -- JSON blob for anything else

    task_events
        id             INTEGER PRIMARY KEY AUTOINCREMENT
        run_id         TEXT NOT NULL      -- FK -> runs.run_id
        task_name      TEXT NOT NULL
        status         TEXT NOT NULL      -- running | succeeded | failed
        started_at     TEXT NOT NULL
        ended_at       TEXT
        duration_seconds REAL
        rows_processed INTEGER
        error_message  TEXT

The store is a thin wrapper over sqlite3: no ORM, no migrations framework.
`init_db()` is idempotent, so opening an existing database is safe.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional


def utcnow_iso() -> str:
    """Current UTC time as an ISO-8601 string (second precision)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def new_run_id() -> str:
    return uuid.uuid4().hex[:12]


@dataclass
class Run:
    run_id: str
    pipeline: str
    status: str = "running"
    started_at: str = field(default_factory=utcnow_iso)
    ended_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    rows_in: Optional[int] = None
    rows_out: Optional[int] = None
    error_message: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskEvent:
    run_id: str
    task_name: str
    status: str = "running"
    started_at: str = field(default_factory=utcnow_iso)
    ended_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    rows_processed: Optional[int] = None
    error_message: Optional[str] = None
    id: Optional[int] = None


class RunStore:
    """A SQLite database holding pipeline runs and their per-task events."""

    def __init__(self, path: str | Path = "pipeline_observability.db"):
        self.path = str(path)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.init_db()

    def init_db(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                pipeline TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                duration_seconds REAL,
                rows_in INTEGER,
                rows_out INTEGER,
                error_message TEXT,
                metadata TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_runs_pipeline_started
                ON runs (pipeline, started_at);

            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL REFERENCES runs(run_id),
                task_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                duration_seconds REAL,
                rows_processed INTEGER,
                error_message TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_task_events_run
                ON task_events (run_id);
            """
        )
        self.conn.commit()

    # -- runs ---------------------------------------------------------------

    def start_run(
        self,
        pipeline: str,
        run_id: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        started_at: Optional[str] = None,
    ) -> Run:
        run = Run(
            run_id=run_id or new_run_id(),
            pipeline=pipeline,
            started_at=started_at or utcnow_iso(),
            metadata=metadata or {},
        )
        self.conn.execute(
            """
            INSERT INTO runs (run_id, pipeline, status, started_at, metadata)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run.run_id, run.pipeline, run.status, run.started_at,
             json.dumps(run.metadata)),
        )
        self.conn.commit()
        return run

    def finish_run(
        self,
        run_id: str,
        status: str,
        rows_in: Optional[int] = None,
        rows_out: Optional[int] = None,
        error_message: Optional[str] = None,
        ended_at: Optional[str] = None,
    ) -> Run:
        ended = ended_at or utcnow_iso()
        row = self.get_run(run_id)
        if row is None:
            raise KeyError(f"unknown run_id: {run_id}")
        start = datetime.fromisoformat(row.started_at)
        end = datetime.fromisoformat(ended)
        duration = (end - start).total_seconds()
        self.conn.execute(
            """
            UPDATE runs
            SET status = ?, ended_at = ?, duration_seconds = ?,
                rows_in = COALESCE(?, rows_in),
                rows_out = COALESCE(?, rows_out),
                error_message = COALESCE(?, error_message)
            WHERE run_id = ?
            """,
            (status, ended, duration, rows_in, rows_out, error_message, run_id),
        )
        self.conn.commit()
        updated = self.get_run(run_id)
        assert updated is not None
        return updated

    def get_run(self, run_id: str) -> Optional[Run]:
        row = self.conn.execute(
            "SELECT * FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        return _row_to_run(row) if row else None

    def list_runs(
        self,
        pipeline: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> list[Run]:
        sql = "SELECT * FROM runs WHERE 1=1"
        params: list[Any] = []
        if pipeline:
            sql += " AND pipeline = ?"
            params.append(pipeline)
        if status:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY started_at DESC, rowid DESC LIMIT ?"
        params.append(limit)
        return [_row_to_run(r) for r in self.conn.execute(sql, params)]

    # -- task events ---------------------------------------------------------

    def record_task(self, event: TaskEvent) -> TaskEvent:
        cur = self.conn.execute(
            """
            INSERT INTO task_events
                (run_id, task_name, status, started_at, ended_at,
                 duration_seconds, rows_processed, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (event.run_id, event.task_name, event.status, event.started_at,
             event.ended_at, event.duration_seconds, event.rows_processed,
             event.error_message),
        )
        self.conn.commit()
        event.id = cur.lastrowid
        return event

    def list_tasks(self, run_id: str) -> list[TaskEvent]:
        rows = self.conn.execute(
            "SELECT * FROM task_events WHERE run_id = ? ORDER BY started_at",
            (run_id,),
        ).fetchall()
        return [_row_to_task(r) for r in rows]

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "RunStore":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def _row_to_run(row: sqlite3.Row) -> Run:
    return Run(
        run_id=row["run_id"],
        pipeline=row["pipeline"],
        status=row["status"],
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        duration_seconds=row["duration_seconds"],
        rows_in=row["rows_in"],
        rows_out=row["rows_out"],
        error_message=row["error_message"],
        metadata=json.loads(row["metadata"] or "{}"),
    )


def _row_to_task(row: sqlite3.Row) -> TaskEvent:
    return TaskEvent(
        id=row["id"],
        run_id=row["run_id"],
        task_name=row["task_name"],
        status=row["status"],
        started_at=row["started_at"],
        ended_at=row["ended_at"],
        duration_seconds=row["duration_seconds"],
        rows_processed=row["rows_processed"],
        error_message=row["error_message"],
    )


# Re-exported for convenience so callers only need one import.
__all__ = ["Run", "TaskEvent", "RunStore", "utcnow_iso", "new_run_id"]
