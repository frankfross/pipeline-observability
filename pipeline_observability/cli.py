"""Command line interface.

    python -m pipeline_observability init [--db FILE]
    python -m pipeline_observability summary [--db FILE]
    python -m pipeline_observability failures [--db FILE] [--days N]
    python -m pipeline_observability freshness [--db FILE]
    python -m pipeline_observability trend --pipeline NAME [--db FILE]
    python -m pipeline_observability demo [--db FILE]   # seed sample data

Output is human-readable tables. The Python API (RunStore / views) is the
machine-readable interface; the CLI is for quick on-call checks.
"""

from __future__ import annotations

import argparse
import sys

from .store import RunStore
from . import views

DEFAULT_DB = "pipeline_observability.db"


def _open(args: argparse.Namespace) -> RunStore:
    return RunStore(args.db)


def _print_table(headers: list[str], rows: list[list[object]]) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    sep = "  ".join("-" * w for w in widths)
    header = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(header)
    print(sep)
    for row in rows:
        print("  ".join(str(c).ljust(widths[i]) for i, c in enumerate(row)))


def cmd_init(args: argparse.Namespace) -> int:
    store = _open(args)
    store.close()
    print(f"Initialized run store at {args.db}")
    return 0


def cmd_summary(args: argparse.Namespace) -> int:
    store = _open(args)
    summary = views.pipeline_summary(store)
    store.close()
    if not summary:
        print("No runs recorded yet.")
        return 0
    _print_table(
        ["pipeline", "runs", "ok", "fail", "success_rate",
         "avg_dur_s", "last_run", "last_status", "rows_out"],
        [
            [s["pipeline"], s["total_runs"], s["succeeded"], s["failed"],
             s["success_rate"], s["avg_duration_seconds"], s["last_run_at"],
             s["last_status"], s["total_rows_out"]]
            for s in summary
        ],
    )
    return 0


def cmd_failures(args: argparse.Namespace) -> int:
    store = _open(args)
    failures = views.failure_report(store, days=args.days)
    store.close()
    if not failures:
        print(f"No failures in the last {args.days} day(s).")
        return 0
    _print_table(
        ["run_id", "pipeline", "started_at", "error"],
        [[f["run_id"], f["pipeline"], f["started_at"], f["error_message"]]
         for f in failures],
    )
    return 0


def cmd_freshness(args: argparse.Namespace) -> int:
    store = _open(args)
    rows = views.freshness(store)
    store.close()
    if not rows:
        print("No runs recorded yet.")
        return 0
    _print_table(
        ["pipeline", "last_success_at", "hours_since_success"],
        [[
            r["pipeline"],
            r["last_success_at"] or "NEVER",
            r["hours_since_success"]
            if r["hours_since_success"] is not None
            else "NEVER",
        ] for r in rows],
    )
    return 0


def cmd_trend(args: argparse.Namespace) -> int:
    store = _open(args)
    trend = views.row_count_trend(store, args.pipeline, limit=args.limit)
    store.close()
    if not trend:
        print(f"No runs recorded for pipeline '{args.pipeline}'.")
        return 0
    _print_table(
        ["run_id", "started_at", "status", "rows_out"],
        [[t["run_id"], t["started_at"], t["status"], t["rows_out"]]
         for t in trend],
    )
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from .seed import seed_demo  # noqa: PLC0415 (lazy: keeps CLI import light)

    store = _open(args)
    seed_demo(store)
    store.close()
    print(f"Seeded demo runs into {args.db}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipeline-observability",
        description="Run-metadata store and summary views for data pipelines.",
    )
    parser.add_argument(
        "--db", default=DEFAULT_DB, help="SQLite database file (default: %(default)s)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="create the run store (idempotent)").set_defaults(
        func=cmd_init
    )
    sub.add_parser("summary", help="per-pipeline run summary").set_defaults(
        func=cmd_summary
    )
    p = sub.add_parser("failures", help="failed runs in the last N days")
    p.add_argument("--days", type=int, default=7)
    p.set_defaults(func=cmd_failures)
    sub.add_parser("freshness", help="time since each pipeline last succeeded").set_defaults(
        func=cmd_freshness
    )
    p = sub.add_parser("trend", help="rows_out trend for one pipeline")
    p.add_argument("--pipeline", required=True)
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_trend)
    sub.add_parser("demo", help="seed sample runs for a quick tour").set_defaults(
        func=cmd_demo
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
