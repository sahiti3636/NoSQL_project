"""Phase 2 setup check: proves the hadoop container, Pig, the Java compiler, MapReduce and the
shared engine plumbing all work, before anyone writes engine code.

    .venv/bin/python scripts/check_phase2.py

It builds a small test batch (the last 1,000 lines of July, which end with the truncated
'alyssa.p' line, plus the first 1,000 lines of August), then:
  1. Pig       runs a script through PigEngine.prepare() + PigEngine._run() that counts lines and
               contract-envelope matches with REGEX_EXTRACT_ALL (checks the regex quoting too).
  2. MapReduce compiles mapreduce/src through MapReduceEngine.prepare() and runs etl.mr.LineCount.
Both answers must equal Python's count with the same contract regex: 2,000 lines, 1,999 matches.
"""
from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl import config, contracts                      # noqa: E402
from etl.engine import RunContext                      # noqa: E402
from etl.engines import hadoop_common as hc            # noqa: E402
from etl.engines.mapreduce import MapReduceEngine      # noqa: E402
from etl.engines.pig import PigEngine                  # noqa: E402

N = 1000


def make_batch(folder: Path) -> list:
    """Two unchanged byte pieces, kept as separate files like a real batch."""
    folder.mkdir(parents=True)
    jul, aug = config.DEFAULT_INPUT_FILES
    tail = jul.read_bytes().split(b"\n")[-N:]           # July has no final newline: last item is alyssa.p
    head = aug.read_bytes().split(b"\n", N)[:N]
    (folder / "piece_1").write_bytes(b"\n".join(tail))
    (folder / "piece_2").write_bytes(b"\n".join(head) + b"\n")
    return [line.decode("utf-8", "replace").removesuffix("\r") for line in tail + head]


def main() -> int:
    run_dir = config.RUNS_DIR / "phase2_check"
    shutil.rmtree(run_dir, ignore_errors=True)
    lines = make_batch(run_dir / "input")
    env = re.compile(contracts.envelope_regex())
    expected = (len(lines), sum(1 for s in lines if env.fullmatch(s)))
    print(f"test batch: {expected[0]:,} lines, {expected[1]:,} match the contract envelope (Python)")
    inp = hc.container_path(run_dir / "input")
    ok = True

    # 1. Pig, through the real engine plumbing
    pig = PigEngine()
    pig.prepare(RunContext("phase2_check", "pig", N, run_dir))
    ms = pig._run(f"""
raw = LOAD '{inp}' USING TextLoader() AS (line:chararray);
m = FOREACH raw GENERATE (REGEX_EXTRACT_ALL(line, {pig.env}) IS NULL ? 0L : 1L) AS ok;
g = GROUP m ALL;
c = FOREACH g GENERATE COUNT(m), SUM(m.ok);
STORE c INTO '{hc.container_path(run_dir / "pig_counts")}' USING PigStorage('\\t');
""", "check_counts")
    got = hc.read_counts(run_dir / "pig_counts", 2, "Pig")
    ok &= got == expected
    print(f"Pig 0.18       {got[0]:,} lines, {got[1]:,} matches  in {ms / 1000:.1f} s  "
          f"{'OK' if got == expected else 'MISMATCH'}")

    # 2. MapReduce: compile mapreduce/src, then run the LineCount job
    mr = MapReduceEngine()
    mr.prepare(RunContext("phase2_check", "mapreduce", N, run_dir))
    ms = mr._job("etl.mr.LineCount", inp, hc.container_path(run_dir / "mr_counts"), label="check_counts")
    got = hc.read_counts(run_dir / "mr_counts", 2, "MapReduce")
    ok &= got == expected
    print(f"MapReduce      {got[0]:,} lines, {got[1]:,} matches  in {ms / 1000:.1f} s  "
          f"{'OK' if got == expected else 'MISMATCH'}")

    shutil.rmtree(run_dir, ignore_errors=True)
    print("PHASE 2 SETUP OK" if ok else "PHASE 2 SETUP FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
