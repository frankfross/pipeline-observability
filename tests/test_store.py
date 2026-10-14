"""Tests for the run store: CRUD on runs and task events."""

import pytest

from pipeline_observability import RunStore
from pipeline_observability.store import TaskEvent


@pytest.fixture()
def store(tmp_path):
    s = RunStore(tmp_path / "test.db")
    yield s
    s.close()


def test_start_and_finish_run(store):
    run = store.start_run("orders_etl", metadata={"owner": "data-team"})
    assert run.status == "running"
    assert run.ended_at is None

    finished = store.finish_run(run.run_id, "succeeded", rows_in=100, rows_out=95)
    assert finished.status == "succeeded"
    assert finished.rows_in == 100
    assert finished.rows_out == 95
    assert finished.duration_seconds is not None
    assert finished.duration_seconds >= 0
    assert finished.metadata == {"owner": "data-team"}


def test_finish_unknown_run_raises(store):
    with pytest.raises(KeyError):
        store.finish_run("no-such-run", "succeeded")


def test_list_runs_filters_and_orders(store):
    a = store.start_run("orders_etl")
    b = store.start_run("users_sync")
    store.finish_run(a.run_id, "succeeded", rows_out=10)
    store.finish_run(b.run_id, "failed", error_message="boom")

    assert [r.run_id for r in store.list_runs()] == [b.run_id, a.run_id]  # newest first
    assert [r.run_id for r in store.list_runs(pipeline="orders_etl")] == [a.run_id]
    assert [r.run_id for r in store.list_runs(status="failed")] == [b.run_id]
    assert store.list_runs(pipeline="missing") == []


def test_task_events_round_trip(store):
    run = store.start_run("orders_etl")
    event = store.record_task(
        TaskEvent(run_id=run.run_id, task_name="extract",
                  status="succeeded", rows_processed=500)
    )
    assert event.id is not None

    tasks = store.list_tasks(run.run_id)
    assert len(tasks) == 1
    assert tasks[0].task_name == "extract"
    assert tasks[0].rows_processed == 500
    assert store.list_tasks("other-run") == []


def test_init_db_is_idempotent(store):
    # Re-running init on a populated store must not wipe data.
    run = store.start_run("orders_etl")
    store.init_db()
    assert store.get_run(run.run_id) is not None
