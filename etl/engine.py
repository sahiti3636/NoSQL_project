"""The one interface every pipeline implements (Contract 4).

The controller calls, in order:

    engine.prepare(run)                     once, before the first batch
    engine.process_batch(spec)  -> result   once per batch, batch_id = 1, 2, 3, ...
    engine.finalize(run)        -> result   once, after the last batch: exact whole-run Q1-Q3
    engine.cleanup(run)                     always, even after a failure

All parsing, cleaning and aggregation must happen INSIDE the engine (Hive queries, MongoDB
aggregations, Pig scripts, MapReduce jobs). The Python class only starts that work and reports
counts back. See etl/engines/reference.py for a complete, test-only example.
"""
from __future__ import annotations

import abc
import importlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

PIPELINES = ("hive", "mongodb", "pig", "mapreduce")


@dataclass
class RunContext:
    run_id: str
    pipeline: str
    batch_size: int
    run_dir: Path                         # runs/<run_id>
    batch_ids: List[int] = field(default_factory=list)   # batches processed so far

    @property
    def final_output_dir(self) -> Path:   # where finalize() writes q1/q2/q3 for the whole run
        return self.run_dir / "final" / "output"


@dataclass
class BatchSpec:
    """Controller -> engine. Mirrors contracts/batch_spec.schema.json."""
    run_id: str
    pipeline: str
    batch_id: int
    batch_size: int
    first_line_no: int
    last_line_no: int
    record_count: int
    input_dir: Path                       # folder with 1 or 2 unchanged pieces of the raw files
    output_dir: Path                      # write q1.tsv, q2.tsv, q3.tsv, malformed.tsv here
    pieces: List[dict] = field(default_factory=list)

    def as_json(self) -> dict:
        return {"run_id": self.run_id, "pipeline": self.pipeline, "batch_id": self.batch_id,
                "batch_size": self.batch_size, "first_line_no": self.first_line_no,
                "last_line_no": self.last_line_no, "record_count": self.record_count,
                "input_uri": str(self.input_dir), "output_dir": str(self.output_dir)}


@dataclass
class BatchResult:
    """Engine -> controller. Mirrors contracts/batch_result.schema.json.

    For process_batch: counts for that batch only.
    For finalize: batch_id=None and counts for the whole run.
    """
    batch_id: Optional[int]
    records_read: int
    valid_records: int
    malformed_records: int
    engine_ms: Dict[str, int] = field(default_factory=dict)   # e.g. {"parse": 812, "q1": 230}
    status: str = "ok"
    error: Optional[str] = None


class Engine(abc.ABC):
    """Subclass this once per pipeline and register it in ENGINES below."""

    name: str = ""
    test_only: bool = False               # True only for the Python reference engine

    @abc.abstractmethod
    def prepare(self, run: RunContext) -> None:
        """Connect, create tables/collections for this run_id, check the engine is reachable."""

    @abc.abstractmethod
    def process_batch(self, spec: BatchSpec) -> BatchResult:
        """Parse every line in spec.input_dir, keep the parsed rows for finalize(), and write
        this batch's own q1.tsv, q2.tsv, q3.tsv and malformed.tsv into spec.output_dir.
        Every line is counted: records_read = valid_records + malformed_records = spec.record_count.
        An output may be a single file (q1.tsv) or a folder of part files named q1.tsv/."""

    @abc.abstractmethod
    def finalize(self, run: RunContext) -> BatchResult:
        """Compute exact Q1-Q3 over ALL parsed rows of the run (never by adding up batch results)
        and write q1.tsv, q2.tsv, q3.tsv into run.final_output_dir."""

    def cleanup(self, run: RunContext) -> None:
        """Drop temporary tables/collections for this run. Must not raise."""


# name -> "module:Class", imported only when chosen so one broken engine never blocks the others.
ENGINES = {
    "hive": "etl.engines.hive:HiveEngine",
    "mongodb": "etl.engines.mongodb:MongoEngine",
    "reference": "etl.engines.reference:ReferenceEngine",   # TEST ONLY - not one of the 4 pipelines
}


def load_engine(name: str) -> Engine:
    if name not in ENGINES:
        raise SystemExit(f"unknown pipeline '{name}'. Available: {', '.join(sorted(ENGINES))}")
    module, cls = ENGINES[name].split(":")
    return getattr(importlib.import_module(module), cls)()
