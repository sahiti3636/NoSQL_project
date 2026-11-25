"""Command-line interface.

    python -m etl run --pipeline hive --batch-size 500000
    python -m etl run --pipeline hive --no-batch                    (whole input as one batch)
    python -m etl run --pipeline reference --batch-size 500000      (test-only engine)
    python -m etl pipelines
    python -m etl report [--run-id ID | --pipeline NAME] [--rows N|all] [--html FILE] [--list] [--compare]
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
    size = r.add_mutually_exclusive_group(required=True)
    size.add_argument("--batch-size", type=int, help="input log lines per batch")
    size.add_argument("--no-batch", action="store_true",
                      help="no batching: the whole input is one batch (batch size = number of lines)")
    r.add_argument("--input", nargs="+", type=Path,
                   help="input files in order (default: the July and August logs in data/)")
    r.add_argument("--keep-files", action="store_true",
                   help="keep each batch's input copy under runs/<run_id>/ for debugging")

    sub.add_parser("pipelines", help="list available pipelines")

    rp = sub.add_parser("report", help="show a run's results and execution metadata from PostgreSQL")
    rp.add_argument("--run-id", help="run to show (default: the latest successful run)")
    rp.add_argument("--pipeline", choices=sorted(ENGINES), help="latest successful run of this pipeline")
    rp.add_argument("--rows", default="10", help="Q1/Q3 rows to print, or 'all' (default 10; Q2 is always all 20)")
    rp.add_argument("--html", type=Path, metavar="FILE", help="also write the full report (every row) as HTML")
    rp.add_argument("--list", action="store_true", help="list recent runs instead")
    rp.add_argument("--compare", action="store_true", help="runtime and equivalence across pipelines instead")
    args = p.parse_args(argv)

    if args.command == "report":
        from .report import main as report_main
        if args.rows != "all" and not args.rows.isdigit():
            p.error("--rows must be a number or 'all'")
        return report_main(args.run_id, args.pipeline, None if args.rows == "all" else int(args.rows),
                           args.html, args.list, args.compare)

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
    if args.batch_size is not None and args.batch_size < 1:
        p.error("--batch-size must be at least 1")
    s = run(args.pipeline, None if args.no_batch else args.batch_size, args.input, keep_files=args.keep_files)
    print(f"\nrun {s.run_id} succeeded")
    print(f"  pipeline          {s.pipeline}")
    print(f"  records           {s.total_records:,} ({s.valid_records:,} valid, {s.malformed_records:,} malformed)")
    if args.no_batch:
        print(f"  batches           1 (no batching: all {s.total_records:,} lines in one batch)")
    else:
        print(f"  batches           {s.num_batches} x {s.batch_size:,} (average {s.avg_batch_size:,.2f})")
    print(f"  runtime           {s.runtime_ms / 1000:,.2f} s")
    print(f"  full report       python -m etl report --run-id {s.run_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
