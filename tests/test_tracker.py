"""Tests for RunTracker / TaskTracker context managers."""

import pytest

from pipeline_observability import RunStore, RunTracker


@pytest.fixture()
def store(tmp_path):
    s = RunStore(tmp_path / "test.db")
    yield s
    s.close()


def test_successful_run_and_tasks(store):
    with RunTracker(store, "orders_etl") as run:
        with run.task("extract") as task:
            task.rows_processed = 120
        with run.task("load") as task:
            task.rows_processed = 120
        run.rows_out = 120

    finished = store.get_run(run.run_id)
    assert finished.status == "succeeded"
    assert finished.rows_out == 120
    tasks = store.list_tasks(run.run_id)
    assert [t.task_name for t in tasks] == ["extract", "load"]
    assert all(t.status == "succeeded" for t in tasks)
    assert tasks[0].rows_processed == 120


def test_exception_marks_run_failed_and_propagates(store):
    with pytest.raises(ValueError, match="bad source"):
        with RunTracker(store, "orders_etl") as run:
            raise ValueError("bad source")

    finished = store.get_run(run.run_id)
    assert finished.status == "failed"
    assert "ValueError: bad source" in (finished.error_message or "")


def test_failed_task_marks_task_failed_but_run_continues(store):
    with RunTracker(store, "orders_etl") as run:
        try:
            with run.task("extract"):
                raise RuntimeError("timeout")
        except RuntimeError:
            pass  # pipeline caught it and kept going
        with run.task("load"):
            pass

    tasks = store.list_tasks(run.run_id)
    assert tasks[0].status == "failed"
    assert "RuntimeError: timeout" in (tasks[0].error_message or "")
    assert tasks[1].status == "succeeded"
    # Run itself succeeded: the pipeline handled the task failure.
    assert store.get_run(run.run_id).status == "succeeded"


def test_explicit_run_id_is_respected(store):
    with RunTracker(store, "orders_etl", run_id="run-123") as run:
        assert run.run_id == "run-123"
    assert store.get_run("run-123") is not None
