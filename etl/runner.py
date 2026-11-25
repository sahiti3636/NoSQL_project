"""Run manager: drives one complete run with the chosen engine (Contract 4, "The run, end to end").

Runtime = from the moment the input starts being read until the final results are committed in
PostgreSQL. Engine/database connections are opened before the clock starts; the report is not timed.
"""
from __future__ import annotations

import platform
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from . import config
from .batcher import Batcher
from .engine import BatchResult, BatchSpec, RunContext, load_engine
from .loader import Loader


class RunIntegrityError(RuntimeError):
    """An engine lost or invented records. The run is failed on purpose."""


@dataclass
class RunSummary:
    run_id: str
    pipeline: str
    batch_size: int
    num_batches: int
    total_records: int
    valid_records: int
    malformed_records: int
    runtime_ms: int

    @property
    def avg_batch_size(self) -> float:
        return self.total_records / self.num_batches if self.num_batches else 0.0


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _environment(pipeline: str) -> dict:
    try:
        commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=config.PROJECT_ROOT,
                                capture_output=True, text=True, timeout=5).stdout.strip()
    except Exception:
        commit = ""
    return {"pipeline": pipeline, "python": sys.version.split()[0], "platform": platform.platform(),
            "machine": platform.machine(), "git_commit": commit or None}


def _check(result: BatchResult, expected_records: int, label: str) -> None:
    if result.status != "ok":
        raise RunIntegrityError(f"{label}: engine reported failure: {result.error}")
    if result.records_read != expected_records:
        raise RunIntegrityError(f"{label}: engine read {result.records_read} records, "
                                f"controller sent {expected_records}")
    if result.valid_records + result.malformed_records != result.records_read:
        raise RunIntegrityError(f"{label}: valid ({result.valid_records}) + malformed "
                                f"({result.malformed_records}) != read ({result.records_read})")


# Batch size used while a run without batching is in progress: the largest value the etl_run
# column holds, far above any input, so the batcher puts every line into batch 1.
NO_BATCHING = 2**31 - 1


def run(pipeline: str, batch_size: Optional[int], input_files: Optional[List[Path]] = None,
        keep_files: bool = False, log=print) -> RunSummary:
    """Runs the whole ETL with one pipeline. batch_size=None means no batching: the whole input
    is one batch, and the run's batch size is recorded as the number of input lines."""
    input_files = [Path(p) for p in (input_files or config.DEFAULT_INPUT_FILES)]
    engine = load_engine(pipeline)
    run_id = str(uuid.uuid4())
    run_dir = config.RUNS_DIR / run_id
    no_batching = batch_size is None
    if no_batching:
        batch_size = NO_BATCHING          # larger than any input: every line lands in batch 1
    ctx = RunContext(run_id=run_id, pipeline=pipeline, batch_size=batch_size, run_dir=run_dir)
    batcher = Batcher(input_files, batch_size, run_dir)          # checks the files exist

    loader = Loader(config.PG_DSN)
    environment = _environment(pipeline)
    environment["batching"] = "none (whole input as one batch)" if no_batching else f"{batch_size} lines"
    loader.start_run(run_id, pipeline, batch_size, _now(), environment)
    log(f"run {run_id}  pipeline={pipeline}  "
        + ("no batching (the whole input is one batch)" if no_batching else f"batch_size={batch_size:,}")
        + ("   [TEST-ONLY engine]" if engine.test_only else ""))
    valid = malformed = 0
    try:
        engine.prepare(ctx)

        started_at, t0 = _now(), time.monotonic()                # <- runtime clock starts
        for batch in batcher.batches():
            spec = BatchSpec(run_id=run_id, pipeline=pipeline, batch_id=batch.batch_id,
                             batch_size=batch_size, first_line_no=batch.first_line_no,
                             last_line_no=batch.last_line_no, record_count=batch.record_count,
                             input_dir=batch.input_dir, output_dir=batch.output_dir,
                             pieces=[p.as_json() for p in batch.pieces])
            b_start = _now()
            result = engine.process_batch(spec)
            _check(result, spec.record_count, f"batch {spec.batch_id}")
            b_end = _now()
            loader.load_batch(run_id, pipeline, spec, result, b_start, b_end)
            ctx.batch_ids.append(spec.batch_id)
            valid += result.valid_records
            malformed += result.malformed_records
            log(f"  batch {spec.batch_id:>4}  lines {spec.first_line_no:>9,}-{spec.last_line_no:<9,}"
                f"  malformed {result.malformed_records:<3} {(b_end - b_start).total_seconds():6.2f}s")
            if not keep_files:
                shutil.rmtree(batch.input_dir, ignore_errors=True)   # copies only; originals untouched

        if not ctx.batch_ids:
            raise RunIntegrityError("input is empty: no batches")
        ctx.final_output_dir.mkdir(parents=True, exist_ok=True)
        final = engine.finalize(ctx)
        _check(final, batcher.total_records, "finalize")
        if (final.valid_records, final.malformed_records) != (valid, malformed):
            raise RunIntegrityError(f"finalize counts ({final.valid_records} valid, "
                                    f"{final.malformed_records} malformed) differ from the batches "
                                    f"({valid} valid, {malformed} malformed)")
        loader.load_final(run_id, pipeline, ctx.final_output_dir, final, _now())
        runtime_ms = int((time.monotonic() - t0) * 1000)          # <- stops after the COMMIT
        finished_at = _now()

        if no_batching:
            batch_size = batcher.total_records                   # one batch holding every line
        loader.mark_succeeded(run_id, started_at, finished_at, runtime_ms, batcher.total_records,
                              valid, malformed, len(ctx.batch_ids), batcher.manifest,
                              batcher.input_sha256, batch_size if no_batching else None)
        return RunSummary(run_id, pipeline, batch_size, len(ctx.batch_ids), batcher.total_records,
                          valid, malformed, runtime_ms)
    except BaseException as exc:
        loader.mark_failed(run_id, f"{type(exc).__name__}: {exc}")
        log(f"run {run_id} FAILED: {type(exc).__name__}: {exc}")
        raise
    finally:
        try:
            engine.cleanup(ctx)
        except Exception as exc:                                   # cleanup must never hide the result
            log(f"warning: cleanup failed: {exc}")
        loader.close()
