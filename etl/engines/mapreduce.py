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

Already done for you (shared setup)
  * Java sources live in mapreduce/src/ (mounted at /mapreduce). prepare() compiles all of them
    into runs/<run>/mapreduce/etl-mr.jar, so a compile error fails the run straight away.
  * etl.mr.Contracts loads the contract regexes and month map from /contracts.
  * etl.mr.CopyText writes COPY-format output lines (escaping \\\\ \\t \\n \\r).
  * etl.mr.LineCount is a tiny working job to copy from (setup check only).
  * self._job('etl.mr.ParseJob', arg1, arg2, label=...) runs one job and returns its time in ms.

Things to get right (from contracts/README.md)
  * conf.set("textinputformat.record.delimiter", "\\n") and strip a trailing \\r, so lines end
    at \\n only. Each batch folder holds 1 or 2 pieces; add the folder as the input path.
  * Use Contracts.envelope.matcher(line).matches(). Month not in Contracts.months -> malformed.
  * Missing parts '' (never null); bytes '-' -> 0; all counts long.
  * Q2: top 20 by request_count desc, then resource_path in byte order (compare UTF-8 bytes or
    use Text's raw byte comparator). Q3 error = status 400-599; keep hours with zero errors.
  * Keep parsed rows as a SequenceFile (or tab text written with CopyText) so 0x01 bytes and
    tabs inside request lines survive.
  * Use a single reducer (job.setNumReduceTasks(1)) for outputs whose order or top-N matters.
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional

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
        """TODO (Sahiti):
        1. Write mapreduce/src/etl/mr/ParseJob.java: map-only job over hc.container_path(spec.input_dir)
           that writes parsed rows to self.parsed_path(b), malformed lines to self.malformed_path(b)
           (MultipleOutputs) and records read / valid / malformed as job counters or a counts file.
        2. Write malformed.tsv (raw line, CopyText) and Q1 (Q1Job) into spec.output_dir.
        3. Q2 and Q3 (q2.tsv, q3.tsv) come from Tushaar's jobs: agree on one helper, e.g.
           self._queries(input_glob, out_dir, label), used by both process_batch and finalize.
        Run each job with: ms['parse'] = self._job('etl.mr.ParseJob', in_path, out_path, label=...)
        """
        raise NotImplementedError("MapReduce process_batch: Sahiti's part (ingestion, parsing, Q1)")

    # ------------------------------------------------------------------ Person B: whole run
    def finalize(self, run: RunContext) -> BatchResult:
        """TODO (Tushaar): run the Q1-Q3 jobs over every batch folder under mr_data/parsed
        (input path .../parsed/batch_*), write q1/q2/q3.tsv into run.final_output_dir and return
        the whole-run counts (batch_id=None)."""
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
