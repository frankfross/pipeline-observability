"""Entry point so `python -m pipeline_observability ...` works."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
