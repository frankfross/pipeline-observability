"""Context managers that make recording a run a one-liner.

Usage::

    store = RunStore("runs.db")
    with RunTracker(store, "orders_etl") as run:
        with run.task("extract") as task:
            rows = extract_orders()
            task.rows_processed = len(rows)
        with run.task("load"):
            load_orders(rows)
        run.rows_out = len(rows)

On normal exit the run is marked ``succeeded``; if an exception escapes, the
run is marked ``failed`` with the exception message, and the exception still
propagates. A failed task does the same for its own row and re-raises, so a
run that dies mid-task records both the task failure and the run failure.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import TracebackType
from typing import Optional

from .store import Run, RunStore, TaskEvent, utcnow_iso


class RunTracker:
    def __init__(
        self,
        store: RunStore,
        pipeline: str,
        run_id: Optional[str] = None,
        metadata: Optional[dict] = None,
    ):
        self.store = store
        self.pipeline = pipeline
        self.run_id = run_id
        self.metadata = metadata or {}
        self.run: Optional[Run] = None
        self.rows_in: Optional[int] = None
        self.rows_out: Optional[int] = None

    def __enter__(self) -> "RunTracker":
        self.run = self.store.start_run(
            self.pipeline, run_id=self.run_id, metadata=self.metadata
        )
        self.run_id = self.run.run_id
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        assert self.run is not None
        if exc_type is None:
            self.store.finish_run(
                self.run.run_id, "succeeded",
                rows_in=self.rows_in, rows_out=self.rows_out,
            )
        else:
            self.store.finish_run(
                self.run.run_id, "failed",
                rows_in=self.rows_in, rows_out=self.rows_out,
                error_message=f"{exc_type.__name__}: {exc}",
            )
        return False  # never swallow the exception

    def task(self, name: str) -> "TaskTracker":
        assert self.run is not None, "use RunTracker as a context manager"
        return TaskTracker(self.store, self.run.run_id, name)


class TaskTracker:
    def __init__(self, store: RunStore, run_id: str, task_name: str):
        self.store = store
        self.run_id = run_id
        self.task_name = task_name
        self.rows_processed: Optional[int] = None
        self._started_at: Optional[str] = None

    def __enter__(self) -> "TaskTracker":
        self._started_at = utcnow_iso()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        ended = utcnow_iso()
        start = datetime.fromisoformat(self._started_at or ended)
        duration = (datetime.fromisoformat(ended) - start).total_seconds()
        if exc_type is None:
            status, error = "succeeded", None
        else:
            status, error = "failed", f"{exc_type.__name__}: {exc}"
        self.store.record_task(
            TaskEvent(
                run_id=self.run_id,
                task_name=self.task_name,
                status=status,
                started_at=self._started_at or ended,
                ended_at=ended,
                duration_seconds=duration,
                rows_processed=self.rows_processed,
                error_message=error,
            )
        )
        return False
