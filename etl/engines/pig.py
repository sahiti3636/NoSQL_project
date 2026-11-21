"""Pig pipeline (Phase 2).

Person C (Aman):    ingestion, parsing with the contract regexes, malformed records, Q1.
Person D (Prajwal): Q2, Q3, whole-run results (finalize), cleanup.

How the work is split between Python and Pig (same idea as Hive)
-------------------------------------------------------------------
Python only writes Pig Latin scripts and runs them with `pig -x local` inside the hadoop
container. It never reads a log line. Pig does everything else:

  batch_000001/input  --TextLoader-->  raw lines (1 chararray each)
  raw lines  --REGEX_EXTRACT_ALL(contract regex)-->  SPLIT into parsed / malformed
  parsed     --STORE-->  runs/<run>/pig_data/parsed/batch_000001     (kept for finalize)
  malformed  --STORE-->  runs/<run>/pig_data/malformed/batch_000001  (kept for finalize)
  parsed     --GROUP/COUNT/SUM-->  q1.tsv, q2.tsv, q3.tsv  (per batch; finalize: over all batches)

Already done for you (shared setup)
  * prepare(): folders, contract regexes as Pig string literals (self.env, self.req, self.fb),
    month map, script runner. Every script is kept in runs/<run>/pig_scripts, every log in
    runs/<run>/pig_logs.
  * self._run(script_text, label) runs one script and returns its time in ms.
  * hadoop_common.read_counts() reads a one-row counts output.

Things to get right (from contracts/README.md)
  * Lines end at \\n only: self._run() already tells Hadoop that (a lone \\r is not a line end);
    still strip a trailing \\r. Load the batch folder (it holds 1 or 2 pieces), never join files.
  * REGEX_EXTRACT_ALL uses Java matches() (whole line) and returns null when there is no match.
  * Month not in months.tsv -> malformed. Date = year-month-day by slicing, no date functions.
  * Missing parts are '' (never null); bytes '-' -> 0; counts are long.
  * Q2 ties: request_count desc, then resource_path in byte order. Q3 error = status 400-599.
  * Output files are PostgreSQL COPY text (tab-separated; escape \\\\ \\t \\n \\r inside fields),
    columns in the order of contracts/outputs.json. A STORE writes a folder of part files; name
    it q1.tsv etc. and the loader reads the folder.
  * Store parsed/malformed rows with BinStorage(), not PigStorage: some request lines contain
    byte 0x01 and could contain tabs, which a text format would split.
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional

from .. import contracts
from ..engine import BatchResult, BatchSpec, Engine, RunContext
from . import hadoop_common as hc

# First lines of every generated script.
_HEADER = "SET default_parallel 1;\n"

# Contract: lines end at \n only. Hadoop's default reader also ends a line at a lone \r. A Pig
# SET statement cannot hold a real newline character, so it is passed on the command line instead.
_NEWLINE_ONLY = "-Dtextinputformat.record.delimiter=\n"


class PigEngine(Engine):
    name = "pig"

    def __init__(self) -> None:
        self.run_dir: Optional[Path] = None
        self.scripts_dir: Optional[Path] = None
        self.logs_dir: Optional[Path] = None
        self.data_dir: Optional[Path] = None

    # ------------------------------------------------------------------ shared setup (done)
    def prepare(self, run: RunContext) -> None:
        self.run_dir = run.run_dir
        self.scripts_dir = run.run_dir / "pig_scripts"      # every script is kept for review
        self.logs_dir = run.run_dir / "pig_logs"
        self.data_dir = run.run_dir / "pig_data"            # parsed/ and malformed/, one folder per batch
        for d in (self.scripts_dir, self.logs_dir, self.data_dir / "parsed", self.data_dir / "malformed"):
            d.mkdir(parents=True, exist_ok=True)

        # Contract regexes as Pig string literals, ready to drop into REGEX_EXTRACT_ALL(...)
        self.env = hc.quote_string(contracts.envelope_regex())
        self.req = hc.quote_string(contracts.request_regex())
        self.fb = hc.quote_string(contracts.request_fallback_regex())
        self.months = contracts.months()                     # {'Jan': '01', ...}

    def parsed_path(self, batch_id: int) -> str:
        """Where batch <batch_id>'s parsed rows live, as Pig sees it."""
        return hc.container_path(self.data_dir / "parsed" / f"batch_{batch_id:06d}")

    def malformed_path(self, batch_id: int) -> str:
        return hc.container_path(self.data_dir / "malformed" / f"batch_{batch_id:06d}")

    # ------------------------------------------------------------------ Person C: parse one batch
    def process_batch(self, spec: BatchSpec) -> BatchResult:
        """TODO (Aman):
        1. Write a script that loads hc.container_path(spec.input_dir) with TextLoader(), parses
           each line with self.env / self.req / self.fb and the month map, SPLITs into parsed and
           malformed, and STOREs them at self.parsed_path(b) and self.malformed_path(b).
        2. In the same script STORE malformed.tsv (the raw line) into spec.output_dir, and a
           one-row counts output (records_read, valid, malformed) into spec.output_dir / 'counts'.
        3. Q1 into spec.output_dir / 'q1.tsv'. Q2 and Q3 (q2.tsv, q3.tsv) come from Prajwal: agree
           on one helper, e.g. self._queries(load_parsed_statement, out_dir, label), used by both
           process_batch and finalize, as the Hive engine does.
        4. read, valid, malformed = hc.read_counts(spec.output_dir / 'counts', 3, 'Pig')
        Run scripts with: ms['parse'] = self._run(script, f'batch_{b:06d}_01_parse')
        """
        raise NotImplementedError("Pig process_batch: Aman's part (ingestion, parsing, Q1)")

    # ------------------------------------------------------------------ Person D: whole run
    def finalize(self, run: RunContext) -> BatchResult:
        """TODO (Prajwal): load every batch folder under pig_data/parsed and pig_data/malformed
        (a glob such as .../parsed/batch_* works), compute Q1-Q3 over all of them into
        run.final_output_dir, and return the whole-run counts (batch_id=None)."""
        raise NotImplementedError("Pig finalize: Prajwal's part (Q2, Q3, whole-run results)")

    def cleanup(self, run: RunContext) -> None:
        """Deletes the kept parsed/malformed data. Must not raise."""
        if self.data_dir is not None:
            shutil.rmtree(self.data_dir, ignore_errors=True)

    # ------------------------------------------------------------------ helpers (done)
    def _run(self, body: str, label: str) -> int:
        """Saves the script as runs/<run>/pig_scripts/<label>.pig, runs it with `pig -x local`
        in the hadoop container, and returns how long it took in ms. Raises with Pig's error."""
        if self.scripts_dir is None:
            raise RuntimeError("PigEngine.prepare() has not run")
        script = self.scripts_dir / f"{label}.pig"
        script.write_text(_HEADER + body, encoding="utf-8")
        return hc.run_in_container(
            ["pig", _NEWLINE_ONLY, "-x", "local", "-logfile", hc.container_path(self.logs_dir / f"{label}.pig_errors.log"),
             "-f", hc.container_path(script)],
            label, self.logs_dir, what="Pig")
