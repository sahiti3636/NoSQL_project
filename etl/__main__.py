"""Command-line interface.

    python -m etl run --pipeline hive --batch-size 500000
    python -m etl run --pipeline reference --batch-size 500000      (test-only engine)
    python -m etl pipelines
"""
import argparse
import sys
from pathlib import Path

from .engine import ENGINES, load_engine


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m etl", description="NASA log ETL controller")
    sub = p.add_subparsers(dest="command", required=True)

    r = sub.add_parser("run", help="run the full ETL with one pipeline")
    r.add_argument("--pipeline", required=True, choices=sorted(ENGINES))
    r.add_argument("--batch-size", required=True, type=int, help="input log lines per batch")
    r.add_argument("--input", nargs="+", type=Path,
                   help="input files in order (default: the July and August logs in data/)")
    r.add_argument("--keep-files", action="store_true",
                   help="keep each batch's input copy under runs/<run_id>/ for debugging")

    sub.add_parser("pipelines", help="list available pipelines")
    args = p.parse_args(argv)

    if args.command == "pipelines":
        for name in sorted(ENGINES):
            try:
                eng = load_engine(name)
                note = "test only" if eng.test_only else "pipeline"
            except Exception as exc:          # a half-written engine should not hide the others
                note = f"cannot load: {exc}"
            print(f"  {name:<10} {note}")
        return 0

    from .runner import run                   # imported late so `pipelines` works without a DB driver
    s = run(args.pipeline, args.batch_size, args.input, keep_files=args.keep_files)
    print(f"\nrun {s.run_id} succeeded")
    print(f"  pipeline          {s.pipeline}")
    print(f"  records           {s.total_records:,} ({s.valid_records:,} valid, {s.malformed_records:,} malformed)")
    print(f"  batches           {s.num_batches} x {s.batch_size:,} (average {s.avg_batch_size:,.2f})")
    print(f"  runtime           {s.runtime_ms / 1000:,.2f} s")
    print("  full report       (reporting module - to be written by the team)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
