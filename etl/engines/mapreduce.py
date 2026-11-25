"""MapReduce pipeline: Java jobs on Hadoop 3.3.6 in local mode.

Sahiti (Person A):  ingestion, parsing with the contract regexes, malformed records, Q1.
Tushaar (Person B): Q2, Q3, whole-run results (finalize), cleanup.

How the work is split between Python and MapReduce
--------------------------------------------------
Python only compiles the Java jobs (once per run, in prepare()) and starts them with
`hadoop jar` inside the hadoop container. It never reads a log line. The jobs do the rest:

  batch_000001/input  --ParseJob (map only)-->  mr_data/parsed/batch_000001     LogRecord SequenceFiles
                                                mr_data/malformed/batch_000001  COPY-escaped raw lines
                                                malformed.tsv, counts
  parsed  --Q1Job-->  q1.tsv   key (date, status); combiner sums (1, bytes)
  parsed  --Q2Job-->  q2.tsv   job 1: (count, bytes) per path -> top 20; job 2: distinct hosts of the 20
  parsed  --Q3Job-->  q3.tsv   key (date, hour); combiner merges (total, errors, error-host set)
  finalize(): Q1Job, Q2Job, Q3Job over mr_data/parsed/batch_*, CountJob over parsed + malformed

Design choices
  * One `hadoop jar` call per batch: etl.mr.RunJobs runs parse, Q1, Q2 and Q3 in one JVM and
    times each job inside it, so every query still gets its own time in etl.query_execution.
  * Every job starts from etl.mr.Jobs.conf(): completion is polled every 100 ms instead of 5 s.
  * Parsed rows are LogRecord SequenceFiles (binary): 7 real request lines contain byte 0x01 and
    a path may contain a tab, so no text delimiter is safe.
  * Matcher.find() with the contract regexes, as Hive's RLIKE and regexp_extract do.
  * Q2 ranks paths as Hadoop Text (raw UTF-8 byte order), as PostgreSQL's C collation does.
  * Output lines are written with etl.mr.CopyText (PostgreSQL COPY text, Contract 2).
  * Whole-run results are recomputed over all parsed rows, never added up from batch outputs.
See docs/mapreduce_engine.md.
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Dict, List, Optional

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
        """Parses one batch with ParseJob, then runs Q1-Q3 over this batch's parsed rows.
        All four jobs run from one `hadoop jar` call (RunJobs), each with its own time."""
        b = spec.batch_id
        label = f"batch_{b:06d}"
        work = spec.output_dir / "_parse"                    # ParseJob's scratch folder, removed by the job
        for path in (work, spec.output_dir / "malformed.tsv", spec.output_dir / "counts",
                     self.data_dir / "parsed" / label, self.data_dir / "malformed" / label):
            hc.clear_output(path)                            # a retried batch starts clean

        steps = [("parse", "ParseJob", hc.container_path(spec.input_dir), hc.container_path(work),
                  self.parsed_path(b), self.malformed_path(b),
                  hc.container_path(spec.output_dir / "malformed.tsv"),
                  hc.container_path(spec.output_dir / "counts"))]
        steps += self._query_steps(self.parsed_path(b), spec.output_dir)
        ms = self._run_steps(steps, label)
        read, valid, malformed = hc.read_counts(spec.output_dir / "counts", 3, "MapReduce")
        return BatchResult(batch_id=b, records_read=read, valid_records=valid,
                           malformed_records=malformed, engine_ms=ms)

    def _query_steps(self, parsed_input: str, out_dir: Path) -> List[tuple]:
        """Q1-Q3 over parsed_input (one batch folder, or the glob .../parsed/batch_* for the whole
        run), written as q1.tsv, q2.tsv, q3.tsv into out_dir. Used by process_batch and finalize.
          q1  Q1Job (Sahiti): key (date, status), combiner sums (1, bytes).
          q2  Q2Job (Tushaar): job 1 counts per path and keeps the top 20 (request_count desc,
              then path in byte order); job 2 counts distinct hosts for those 20 paths only.
          q3  Q3Job (Tushaar): key (date, hour), every hour with traffic keeps its row.
        """
        for name in ("q1.tsv", "q2.tsv", "q3.tsv", "_q2_top_q2.tsv"):   # last: Q2 scratch after a crash
            hc.clear_output(out_dir / name)
        return [(q, f"{q.upper()}Job", parsed_input, hc.container_path(out_dir / f"{q}.tsv"))
                for q in ("q1", "q2", "q3")]

    # ------------------------------------------------------------------ Person B: whole run
    def finalize(self, run: RunContext) -> BatchResult:
        """Q1-Q3 over every batch's parsed rows (the glob mr_data/parsed/batch_*), never added up
        from batch outputs. The whole-run counts are counted by CountJob over the kept parsed and
        malformed folders, not by Python."""
        out = run.final_output_dir
        out.mkdir(parents=True, exist_ok=True)
        parsed_all = hc.container_path(self.data_dir / "parsed") + "/batch_*"
        malformed_all = hc.container_path(self.data_dir / "malformed") + "/batch_*"
        hc.clear_output(out / "counts")

        steps = self._query_steps(parsed_all, out)
        steps.append(("counts", "CountJob", parsed_all, malformed_all, hc.container_path(out / "counts")))
        ms = self._run_steps(steps, "final")
        ms.pop("counts", None)                               # not a query: not stored as a query time
        valid, malformed = hc.read_counts(out / "counts", 2, "MapReduce")
        return BatchResult(batch_id=None, records_read=valid + malformed, valid_records=valid,
                           malformed_records=malformed, engine_ms=ms)

    def cleanup(self, run: RunContext) -> None:
        """Deletes the kept parsed/malformed data. Must not raise."""
        if self.data_dir is not None:
            shutil.rmtree(self.data_dir, ignore_errors=True)

    # ------------------------------------------------------------------ helpers (done)
    def _run_steps(self, steps: List[tuple], label: str) -> Dict[str, int]:
        """Runs the steps (name, job class, args...) one after another in ONE `hadoop jar` call
        (etl.mr.RunJobs) and returns {name: ms} as timed inside the JVM. Starting a JVM per job
        cost about a second each; this way a batch pays for one."""
        if not self.jar:
            raise RuntimeError("MapReduceEngine.prepare() has not run")
        plan = self.logs_dir / f"{label}.plan.tsv"
        timings = self.logs_dir / f"{label}.timings.tsv"
        plan.write_text("".join("\t".join(step) + "\n" for step in steps), encoding="utf-8")
        hc.run_in_container(["hadoop", "jar", self.jar, "etl.mr.RunJobs",
                             hc.container_path(plan), hc.container_path(timings)],
                            label, self.logs_dir, what="MapReduce")
        done = dict(line.split("\t") for line in timings.read_text(encoding="utf-8").splitlines() if line)
        return {name: int(ms) for name, ms in done.items()}

    def _job(self, main_class: str, *args: str, label: str) -> int:
        """Runs one job on its own (`hadoop jar etl-mr.jar <main_class> <args...>`) and returns
        its time in ms. Used by the setup check; the pipeline itself uses _run_steps()."""
        if not self.jar:
            raise RuntimeError("MapReduceEngine.prepare() has not run")
        return hc.run_in_container(["hadoop", "jar", self.jar, main_class, *args],
                                   label, self.logs_dir, what="MapReduce")
