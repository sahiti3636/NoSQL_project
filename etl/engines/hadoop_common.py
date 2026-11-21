"""Shared plumbing for the Phase 2 pipelines (Pig and MapReduce). Nothing here parses a log line.

Both engines run inside the `hadoop` container (docker/hadoop/Dockerfile) in Hadoop local mode.
The container sees the project folders at the same places as the Hive container:

    laptop ./runs       ->  /runs        batch input pieces in, result files out
    laptop ./contracts  ->  /contracts   shared regexes and month map (read-only)
    laptop ./data       ->  /input       raw NASA logs (read-only, never needed directly)
    laptop ./mapreduce  ->  /mapreduce   MapReduce Java sources (read-only)

Use container_path() to turn any path under ./runs into the path the engine sees.
"""
from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from .. import config, outputs

SERVICE = "hadoop"
CONTRACTS = "/contracts"


def container_path(host_path: Path) -> str:
    """runs/<run>/... on the laptop -> /runs/<run>/... inside the hadoop container."""
    return config.to_container_path(host_path)


def quote_string(value: str) -> str:
    """A single-quoted Pig (or Java-style) string literal: backslashes doubled, quotes escaped.
    Use it to put a contract regex into a generated Pig script."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def run_in_container(argv: Sequence[str], label: str, log_dir: Optional[Path] = None,
                     what: str = "Hadoop") -> int:
    """Runs argv inside the hadoop container and returns how long it took in ms.

    stdout + stderr are saved to <log_dir>/<label>.log (Pig and Hadoop are chatty), and the end of
    that log is put in the error when the command fails, so a failed run says why.
    """
    t = time.monotonic()
    proc = subprocess.run(["docker", "compose", "exec", "-T", SERVICE, *argv],
                          cwd=config.PROJECT_ROOT, capture_output=True, text=True)
    ms = int((time.monotonic() - t) * 1000)
    log = (proc.stdout or "") + (proc.stderr or "")
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / f"{label}.log").write_text(log, encoding="utf-8")
    if proc.returncode != 0:
        raise RuntimeError(f"{what} step {label} failed (exit {proc.returncode}):\n{log.strip()[-3000:]}")
    return ms


def read_counts(path: Path, n: int, what: str = "engine") -> Tuple[int, ...]:
    """Reads a one-row counts output (a file, or a folder of part files with one non-empty part),
    then deletes it: counts are not a result file for PostgreSQL."""
    rows: List[List[str]] = list(outputs.read_rows(path))
    if Path(path).is_dir():
        shutil.rmtree(path, ignore_errors=True)
    else:
        Path(path).unlink(missing_ok=True)
    if len(rows) != 1 or len(rows[0]) != n:
        raise RuntimeError(f"{what}: expected one row of {n} counts in {path}, got {rows!r}")
    return tuple(int(v) if v not in ("", "\\N") else 0 for v in rows[0])


def clear_output(path: Path) -> None:
    """Pig and Hadoop refuse to write into an existing output folder; remove it first."""
    p = Path(path)
    if p.is_dir():
        shutil.rmtree(p)
    elif p.exists():
        p.unlink()
