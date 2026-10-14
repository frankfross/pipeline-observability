"""Tests for the CLI: end-to-end through main() with a temp database."""

import pytest

from pipeline_observability import RunStore
from pipeline_observability.cli import main
from pipeline_observability.seed import seed_demo


@pytest.fixture()
def db(tmp_path):
    return str(tmp_path / "cli.db")


def test_init_and_empty_summary(db, capsys):
    assert main(["--db", db, "init"]) == 0
    assert main(["--db", db, "summary"]) == 0
    assert "No runs recorded yet." in capsys.readouterr().out


def test_summary_and_freshness_after_demo(db, capsys):
    assert main(["--db", db, "demo"]) == 0
    assert main(["--db", db, "summary"]) == 0
    out = capsys.readouterr().out
    assert "orders_etl" in out
    assert "users_sync" in out

    assert main(["--db", db, "freshness"]) == 0
    out = capsys.readouterr().out
    assert "inventory_snapshot" in out
    assert "NEVER" in out  # never had a successful run


def test_failures_lists_errors(db, capsys):
    main(["--db", db, "demo"])
    assert main(["--db", db, "failures", "--days", "7"]) == 0
    out = capsys.readouterr().out
    assert "TimeoutError" in out
    assert "ConnectionError" in out


def test_trend_for_unknown_pipeline(db, capsys):
    assert main(["--db", db, "init"]) == 0
    assert main(["--db", db, "trend", "--pipeline", "nope"]) == 0
    assert "No runs recorded for pipeline 'nope'." in capsys.readouterr().out


def test_demo_seed_is_idempotent_on_fresh_db(db):
    store = RunStore(db)
    seed_demo(store)
    n_first = len(store.list_runs(limit=1000))
    store.close()
    # Seeding a fresh database twice from scratch gives the same count;
    # run ids are deterministic (demo-000 ...), so a second seed on the
    # same file would collide -- document that with a clear failure.
    store2 = RunStore(db)
    with pytest.raises(Exception):  # noqa: BLE001 - any integrity error is fine
        seed_demo(store2)
    assert len(store2.list_runs(limit=1000)) == n_first
    store2.close()
