"""MapReduce pipeline (Phase 2), Java jobs on Hadoop 3.3.6 in local mode.

Person A (Sahiti):  ingestion, parsing with the contract regexes, malformed records, Q1.
Person B (Tushaar): Q2, Q3, whole-run results (finalize), cleanup.

How the work is split between Python and MapReduce
--------------------------------------------------
Python only compiles the Java jobs and starts them with `hadoop jar` inside the hadoop container.
It never reads a log line. The jobs do everything else:

  batch_000001/input  --ParseJob (map only)-->  mr_data/parsed/batch_000001     (kept for finalize)
                                                mr_data/malformed/batch_000001  (kept for finalize)
                                                counts
  parsed  --Q1Job-->  q1.tsv           (key date+status, combiner sums 1 and bytes)
  parsed  --Q2 jobs-->  q2.tsv         (count per path -> top 20 -> distinct hosts of those 20)
  parsed  --Q3 jobs-->  q3.tsv         (per date+hour+host -> per date+hour)
  finalize: the same Q jobs over mr_data/parsed/* (every batch)

Status
  * Done (Sahiti): ParseJob, Q1Job, LogRecord, LongPair, process_batch(), _queries(), _q1().
    Checked against the Python reference at batch size 500,000: every batch's counts, q1.tsv and
    malformed.tsv, the whole-run Q1, and all fields of the 2,038-line smoke sample match.
  * To do (Tushaar): _q2(), _q3() and finalize(). Each is marked TODO below.

Shared setup
  * Java sources live in mapreduce/src/ (mounted at /mapreduce). prepare() compiles all of them
    into runs/<run>/mapreduce/etl-mr.jar, so a compile error fails the run straight away.
  * etl.mr.Contracts loads the contract regexes and month map from /contracts.
  * etl.mr.CopyText writes COPY-format output lines (escaping \\\\ \\t \\n \\r).
  * self._job('etl.mr.X', arg1, arg2, label=...) runs one job and returns its time in ms.

For Q2 and Q3 (Tushaar)
  * Input: parsed_input is one batch folder or the glob .../parsed/batch_* (whole run). Read it
    with SequenceFileInputFormat; each value is an etl.mr.LogRecord (NullWritable key). Q1Job is
    the pattern to copy: same input, combiner, one reducer, CopyText.row(...) output lines.
  * Q2: top 20 by request_count desc, then resource_path in byte order (compare UTF-8 bytes,
    e.g. with Text's raw comparator), then distinct hosts for those 20 only.
  * Q3: error = status 400-599 (LogRecord.isError()); keep hours with zero errors; error_rate =
    errors / total as a double.
  * Whole-run results must be computed over all parsed rows, never added up from batch outputs.
  * Use a single reducer (job.setNumReduceTasks(1)) where order or top-N matters.
  * Output folders must not exist yet: hc.clear_output(out_dir / 'q2.tsv') first, as _q1 does.
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Dict, Optional

from ..engine import BatchResult, BatchSpec, Engine, RunContext
from . import hadoop_common as hc

_SOURCES = "/mapreduce/src"


class MapReduceEngine(Engine):
    name = "mapreduce"

    def __init__(self) -> None:
        self.run_dir: Optional[Path] = None
        self.build_dir: Optional[Path] = None
        self.logs_dir: Optional[Path] = None
        self.data_dir: Optional[Path] = None
        self.jar = ""

    # ------------------------------------------------------------------ shared setup (done)
    def prepare(self, run: RunContext) -> None:
        self.run_dir = run.run_dir
        self.build_dir = run.run_dir / "mapreduce"           # classes + etl-mr.jar for this run
        self.logs_dir = run.run_dir / "mr_logs"
        self.data_dir = run.run_dir / "mr_data"              # parsed/ and malformed/, one folder per batch
        for d in (self.logs_dir, self.data_dir / "parsed", self.data_dir / "malformed"):
            d.mkdir(parents=True, exist_ok=True)
        self.compile()

    def compile(self) -> int:
        """Compiles every .java file under mapreduce/src into runs/<run>/mapreduce/etl-mr.jar."""
        hc.clear_output(self.build_dir)
        self.build_dir.mkdir(parents=True)
        out = hc.container_path(self.build_dir)
        self.jar = f"{out}/etl-mr.jar"
        script = (f'set -e; mkdir -p {out}/classes; '
                  f'javac -encoding UTF-8 -nowarn -cp "$(hadoop classpath)" -d {out}/classes '
                  f'$(find {_SOURCES} -name "*.java"); '
                  f'jar cf {self.jar} -C {out}/classes .')
        return hc.run_in_container(["sh", "-c", script], "00_compile", self.logs_dir, what="MapReduce")

    def parsed_path(self, batch_id: int) -> str:
        """Where batch <batch_id>'s parsed rows live, as the jobs see them."""
        return hc.container_path(self.data_dir / "parsed" / f"batch_{batch_id:06d}")

    def malformed_path(self, batch_id: int) -> str:
        return hc.container_path(self.data_dir / "malformed" / f"batch_{batch_id:06d}")

    # ------------------------------------------------------------------ Person A: parse one batch
    def process_batch(self, spec: BatchSpec) -> BatchResult:
        """Parses one batch with ParseJob, then runs Q1-Q3 over this batch's parsed rows."""
        b = spec.batch_id
        label = f"batch_{b:06d}"
        work = spec.output_dir / "_parse"                    # ParseJob's scratch folder, removed by the job
        hc.clear_output(work)
        for path in (spec.output_dir / "malformed.tsv", self.data_dir / "parsed" / f"batch_{b:06d}",
                     self.data_dir / "malformed" / f"batch_{b:06d}"):
            hc.clear_output(path)                            # a retried batch starts clean

        ms: Dict[str, int] = {}
        ms["parse"] = self._job(
            "etl.mr.ParseJob", hc.container_path(spec.input_dir), hc.container_path(work),
            self.parsed_path(b), self.malformed_path(b),
            hc.container_path(spec.output_dir / "malformed.tsv"), hc.container_path(spec.output_dir / "counts"),
            label=f"{label}_01_parse")
        read, valid, malformed = hc.read_counts(spec.output_dir / "counts", 3, "MapReduce")

        ms.update(self._queries(self.parsed_path(b), spec.output_dir, label))
        return BatchResult(batch_id=b, records_read=read, valid_records=valid,
                           malformed_records=malformed, engine_ms=ms)

    def _queries(self, parsed_input: str, out_dir: Path, label: str) -> Dict[str, int]:
        """Q1-Q3 over parsed_input (one batch folder, or the glob .../parsed/batch_* for the whole
        run), written as q1.tsv, q2.tsv, q3.tsv into out_dir. Used by process_batch and finalize."""
        ms = {"q1": self._q1(parsed_input, out_dir, label)}
        ms["q2"] = self._q2(parsed_input, out_dir, label)
        ms["q3"] = self._q3(parsed_input, out_dir, label)
        return ms

    def _q1(self, parsed_input: str, out_dir: Path, label: str) -> int:
        """Q1 Daily Traffic Summary (Sahiti): one Q1Job."""
        hc.clear_output(out_dir / "q1.tsv")
        return self._job("etl.mr.Q1Job", parsed_input, hc.container_path(out_dir / "q1.tsv"),
                         label=f"{label}_02_q1")

    # ------------------------------------------------------------------ Person B: Q2, Q3
    def _q2(self, parsed_input: str, out_dir: Path, label: str) -> int:
        """TODO (Tushaar): Q2 Top 20 Requested Resources into out_dir / 'q2.tsv'. Return time in ms.
        Read parsed_input with SequenceFileInputFormat; values are etl.mr.LogRecord (see Q1Job)."""
        raise NotImplementedError("MapReduce Q2: Tushaar's part")

    def _q3(self, parsed_input: str, out_dir: Path, label: str) -> int:
        """TODO (Tushaar): Q3 Hourly Error Analysis into out_dir / 'q3.tsv'. Return time in ms."""
        raise NotImplementedError("MapReduce Q3: Tushaar's part")

    # ------------------------------------------------------------------ Person B: whole run
    def finalize(self, run: RunContext) -> BatchResult:
        """TODO (Tushaar): self._queries(hc.container_path(self.data_dir / 'parsed') + '/batch_*',
        run.final_output_dir, 'final') runs Q1-Q3 over every batch. Then return the whole-run counts
        (batch_id=None), counted from mr_data/parsed and mr_data/malformed by a job, not by Python."""
        raise NotImplementedError("MapReduce finalize: Tushaar's part (Q2, Q3, whole-run results)")

    def cleanup(self, run: RunContext) -> None:
        """Deletes the kept parsed/malformed data. Must not raise."""
        if self.data_dir is not None:
            shutil.rmtree(self.data_dir, ignore_errors=True)

    # ------------------------------------------------------------------ helpers (done)
    def _job(self, main_class: str, *args: str, label: str) -> int:
        """Runs `hadoop jar etl-mr.jar <main_class> <args...>` and returns its time in ms.
        Output folders must not exist yet: call hc.clear_output(host_path) first."""
        if not self.jar:
            raise RuntimeError("MapReduceEngine.prepare() has not run")
        return hc.run_in_container(["hadoop", "jar", self.jar, main_class, *args],
                                   label, self.logs_dir, what="MapReduce")
