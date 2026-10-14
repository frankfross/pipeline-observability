"""pipeline-observability: a small run-metadata store for data pipelines.

Every pipeline run is recorded in SQLite (status, timing, row counts, per-task
events), and a set of summary views answers the questions an on-call data
engineer actually asks: did the last run succeed, how long do runs take,
which pipelines are stale, and what failed recently.
"""

from .store import RunStore
from .tracker import RunTracker, TaskTracker
from . import views

__all__ = ["RunStore", "RunTracker", "TaskTracker", "views"]
__version__ = "0.1.0"
