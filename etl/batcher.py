"""Cuts the raw input into batches of N lines (Contract 4).

The batcher is deliberately content-agnostic: it copies bytes and counts newlines, nothing else.
It never decodes, trims, parses or filters a line, so all real processing stays in the engine.

Each batch becomes a folder holding 1 or 2 byte-identical pieces of the original files:

    runs/<run_id>/batch_000003/input/NASA_access_log_Jul95   <- end of July (no bytes changed)
    runs/<run_id>/batch_000003/input/NASA_access_log_Aug95   <- start of August

Rules (all from the spec):
  * each file is split into lines on b'\\n' on its own; a last line without a newline still counts
    (the July file ends with the truncated line b'alyssa.p');
  * the files are never concatenated byte for byte;
  * line numbers run as one sequence across files: 1 .. total;
  * batch i holds lines (i-1)*N+1 .. i*N; only the final batch may be shorter.
The input is read exactly once; per-file SHA-256 and line counts are computed in the same pass.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, List, Optional


@dataclass
class Piece:
    source_file: str          # original file name, e.g. NASA_access_log_Jul95
    path: Path                # the byte-identical copy inside the batch folder
    first_line_no: int        # global line numbers (1-based, across both files)
    last_line_no: int
    byte_start: int           # byte range inside the original file
    byte_end: int

    def as_json(self) -> dict:
        return {"file": self.source_file, "first_line_no": self.first_line_no,
                "last_line_no": self.last_line_no, "byte_start": self.byte_start,
                "byte_end": self.byte_end}


@dataclass
class Batch:
    batch_id: int
    folder: Path              # runs/<run_id>/batch_000001
    first_line_no: int
    last_line_no: int = 0
    pieces: List[Piece] = field(default_factory=list)

    @property
    def record_count(self) -> int:
        return self.last_line_no - self.first_line_no + 1

    @property
    def input_dir(self) -> Path:
        return self.folder / "input"

    @property
    def output_dir(self) -> Path:
        return self.folder / "output"


class Batcher:
    def __init__(self, input_files: List[Path], batch_size: int, run_dir: Path):
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        self.input_files = [Path(p) for p in input_files]
        for p in self.input_files:
            if not p.is_file():
                raise FileNotFoundError(f"input file not found: {p}")
        self.batch_size = batch_size
        self.run_dir = Path(run_dir)
        self.manifest: List[dict] = []    # complete once batches() is exhausted
        self.total_records = 0

    @property
    def input_sha256(self) -> str:
        """One hash for the whole input: SHA-256 over the per-file hashes, in order."""
        h = hashlib.sha256()
        for f in self.manifest:
            h.update(f["sha256"].encode())
        return h.hexdigest()

    def _new_batch(self, batch_id: int, first_line_no: int) -> Batch:
        folder = self.run_dir / f"batch_{batch_id:06d}"
        (folder / "input").mkdir(parents=True, exist_ok=False)
        (folder / "output").mkdir(parents=True, exist_ok=False)
        return Batch(batch_id=batch_id, folder=folder, first_line_no=first_line_no)

    def batches(self) -> Iterator[Batch]:
        """Yields each batch as soon as its folder is complete (engine work overlaps reading)."""
        line_no = 0
        batch_id = 0
        batch: Optional[Batch] = None
        piece: Optional[Piece] = None
        out = None
        try:
            for src in self.input_files:
                digest = hashlib.sha256()
                size = 0
                lines = 0
                with open(src, "rb") as f:
                    for line in f:                       # splits on b'\n' only; keeps the b'\n'
                        if batch is None:
                            batch_id += 1
                            batch = self._new_batch(batch_id, line_no + 1)
                        if out is None:                  # start a new piece of this file
                            piece = Piece(src.name, batch.input_dir / src.name,
                                          line_no + 1, line_no + 1, size, size)
                            batch.pieces.append(piece)
                            out = open(piece.path, "wb")
                        out.write(line)
                        digest.update(line)
                        size += len(line)
                        lines += 1
                        line_no += 1
                        piece.last_line_no = line_no
                        piece.byte_end = size
                        batch.last_line_no = line_no
                        if batch.record_count == self.batch_size:
                            out.close()
                            out = None
                            yield batch
                            batch = None
                if out is not None:                      # file ended mid-batch: close this piece,
                    out.close()                          # the batch continues in the next file
                    out = None
                self.manifest.append({"file": src.name, "bytes": size, "lines": lines,
                                      "sha256": digest.hexdigest()})
            self.total_records = line_no
            if batch is not None:                        # the final, shorter batch
                yield batch
        finally:
            if out is not None:
                out.close()
