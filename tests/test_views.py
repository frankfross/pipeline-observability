"""Tests for the summary views."""

from datetime import datetime, timedelta, timezone

import pytest

from pipeline_observability import RunStore, RunTracker
from pipeline_observability import views
from pipeline_observability.seed import seed_demo


@pytest.fixture()
def store(tmp_path):
    s = RunStore(tmp_path / "test.db")
    yield s
    s.close()


def _run(store, pipeline, status, rows_out=None, days_ago=0):
    """Create a finished run with a controlled start time."""
    start = datetime.now(timezone.utc) - timedelta(days=days_ago)
    run = store.start_run(pipeline, started_at=start.replace(microsecond=0).isoformat())
    end = start + timedelta(seconds=30)
    store.finish_run(
        run.run_id, status, rows_out=rows_out,
        ended_at=end.replace(microsecond=0).isoformat(),
    )
    return run


def test_pipeline_summary_math(store):
    _run(store, "orders_etl", "succeeded", rows_out=100)
    _run(store, "orders_etl", "succeeded", rows_out=200)
    _run(store, "orders_etl", "failed")
    _run(store, "users_sync", "succeeded", rows_out=50)

    summary = {s["pipeline"]: s for s in views.pipeline_summary(store)}
    orders = summary["orders_etl"]
    assert orders["total_runs"] == 3
    assert orders["succeeded"] == 2
    assert orders["failed"] == 1
    assert orders["success_rate"] == pytest.approx(2 / 3, abs=0.001)
    assert orders["total_rows_out"] == 300
    assert orders["last_status"] == "failed"
    assert orders["avg_duration_seconds"] == pytest.approx(30.0)


def test_pipeline_summary_empty_store(store):
    assert views.pipeline_summary(store) == []


def test_failure_report_window(store):
    _run(store, "orders_etl", "failed", days_ago=2)
    _run(store, "orders_etl", "failed", days_ago=30)
    _run(store, "orders_etl", "succeeded", days_ago=1)

    failures = views.failure_report(store, days=7)
    assert len(failures) == 1
    assert failures[0]["status"] == "failed"


def test_freshness_flags_never_green(store):
    _run(store, "orders_etl", "succeeded", days_ago=2)
    _run(store, "never_green", "failed", days_ago=1)

    fresh = {f["pipeline"]: f for f in views.freshness(store)}
    assert fresh["orders_etl"]["hours_since_success"] == pytest.approx(48.0, abs=1.0)
    assert fresh["never_green"]["hours_since_success"] is None
    assert fresh["never_green"]["last_success_at"] is None


def test_row_count_trend_is_chronological(store):
    _run(store, "orders_etl", "succeeded", rows_out=100, days_ago=3)
    _run(store, "orders_etl", "succeeded", rows_out=0, days_ago=1)

    trend = views.row_count_trend(store, "orders_etl")
    assert [t["rows_out"] for t in trend] == [100, 0]


def test_seed_demo_produces_expected_shape(store):
    seed_demo(store)
    summary = {s["pipeline"]: s for s in views.pipeline_summary(store)}
    assert set(summary) == {"orders_etl", "users_sync", "inventory_snapshot"}
    assert summary["orders_etl"]["failed"] == 1
    assert summary["inventory_snapshot"]["succeeded"] == 0
    fresh = {f["pipeline"]: f for f in views.freshness(store)}
    assert fresh["inventory_snapshot"]["hours_since_success"] is None
    # The silent zero-row run is the newest orders_etl run.
    assert views.row_count_trend(store, "orders_etl")[-1]["rows_out"] == 0


def test_tracker_integration_with_views(store):
    with RunTracker(store, "orders_etl") as run:
        run.rows_out = 42
    summary = views.pipeline_summary(store)
    assert summary[0]["total_rows_out"] == 42
    assert summary[0]["success_rate"] == 1.0
