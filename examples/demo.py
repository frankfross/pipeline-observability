"""Seed demo data and print the summary views.

Run from the repo root:

    python examples/demo.py

This is the fastest way to see what the project does: it creates
``demo.db`` in the repo root, fills it with two weeks of history across
three pipelines (including a silent zero-row run and a never-green
pipeline), then prints the summary, failures, and freshness views.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline_observability import RunStore, views  # noqa: E402
from pipeline_observability.seed import seed_demo  # noqa: E402

DB = Path(__file__).resolve().parent.parent / "demo.db"


def main() -> None:
    if DB.exists():
        DB.unlink()
    store = RunStore(DB)
    seed_demo(store)

    print("== pipeline summary ==")
    for row in views.pipeline_summary(store):
        print(row)

    print("\n== failures (last 7 days) ==")
    for row in views.failure_report(store, days=7):
        print(f"{row['pipeline']} {row['started_at']}: {row['error_message']}")

    print("\n== freshness ==")
    for row in views.freshness(store):
        print(row)

    print("\n== orders_etl row-count trend (last 5) ==")
    for row in views.row_count_trend(store, "orders_etl", limit=5):
        print(row)

    store.close()
    print(f"\nDemo database written to {DB}")


if __name__ == "__main__":
    main()
