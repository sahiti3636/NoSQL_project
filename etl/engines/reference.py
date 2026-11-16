"""TEST-ONLY reference engine (pure Python). NOT one of the four pipelines.

It implements the contracts directly in Python so the controller, batcher, loader and database
can be tested end to end before Hive and MongoDB exist, and so every real engine has a known-good
answer to match. It must never be presented as a pipeline in the demo or the report.
"""
from __future__ import annotations

import re
import time
from collections import Counter, defaultdict
from pathlib import Path

from .. import contracts
from ..engine import BatchResult, BatchSpec, Engine, RunContext
from ..outputs import write_tsv


class _Aggregates:
    """Exact aggregates. Distinct hosts are kept as sets, so merging batches stays exact."""

    def __init__(self):
        self.q1_count, self.q1_bytes = Counter(), Counter()
        self.q2_count, self.q2_bytes, self.q2_hosts = Counter(), Counter(), defaultdict(set)
        self.q3_total, self.q3_err, self.q3_hosts = Counter(), Counter(), defaultdict(set)
        self.valid = self.malformed = 0

    def add(self, host, date, hour, path, status, nbytes):
        self.valid += 1
        self.q1_count[(date, status)] += 1
        self.q1_bytes[(date, status)] += nbytes
        self.q2_count[path] += 1
        self.q2_bytes[path] += nbytes
        self.q2_hosts[path].add(host)
        self.q3_total[(date, hour)] += 1
        if 400 <= status <= 599:
            self.q3_err[(date, hour)] += 1
            self.q3_hosts[(date, hour)].add(host)

    def merge(self, other: "_Aggregates"):
        self.valid += other.valid
        self.malformed += other.malformed
        for mine, theirs in ((self.q1_count, other.q1_count), (self.q1_bytes, other.q1_bytes),
                             (self.q2_count, other.q2_count), (self.q2_bytes, other.q2_bytes),
                             (self.q3_total, other.q3_total), (self.q3_err, other.q3_err)):
            mine.update(theirs)
        for mine, theirs in ((self.q2_hosts, other.q2_hosts), (self.q3_hosts, other.q3_hosts)):
            for k, v in theirs.items():
                mine[k] |= v

    def write(self, out_dir: Path) -> dict:
        ms = {}
        t = time.monotonic()
        write_tsv(out_dir / "q1.tsv", ((d, s, self.q1_count[(d, s)], self.q1_bytes[(d, s)])
                                       for d, s in sorted(self.q1_count)))
        ms["q1"] = int((time.monotonic() - t) * 1000)
        t = time.monotonic()
        top = sorted(self.q2_count.items(), key=lambda kv: (-kv[1], kv[0]))[:20]   # tie: path, byte order
        write_tsv(out_dir / "q2.tsv", ((p, c, self.q2_bytes[p], len(self.q2_hosts[p])) for p, c in top))
        ms["q2"] = int((time.monotonic() - t) * 1000)
        t = time.monotonic()
        write_tsv(out_dir / "q3.tsv", ((d, h, self.q3_err[(d, h)], n, repr(self.q3_err[(d, h)] / n),
                                        len(self.q3_hosts.get((d, h), ())))
                                       for (d, h), n in sorted(self.q3_total.items())))
        ms["q3"] = int((time.monotonic() - t) * 1000)
        return ms


class ReferenceEngine(Engine):
    name = "reference"
    test_only = True

    def prepare(self, run: RunContext) -> None:
        self.env = re.compile(contracts.envelope_regex(), re.S | re.A)
        self.req = re.compile(contracts.request_regex(), re.S | re.A)
        self.fallback = re.compile(contracts.request_fallback_regex(), re.S | re.A)
        self.months = contracts.months()
        self.run_total = _Aggregates()

    def _parse_piece(self, path: Path, agg: _Aggregates, malformed: list) -> int:
        lines = path.read_bytes().split(b"\n")
        if lines and lines[-1] == b"":
            lines.pop()                                   # a final newline makes no record
        for raw in lines:
            s = raw[:-1] if raw.endswith(b"\r") else raw
            s = s.decode("utf-8", "replace")
            m = self.env.match(s)
            if not m or m.group(5) not in self.months:
                agg.malformed += 1
                malformed.append((s,))
                continue
            host, _, _, dd, mon, yy, hh, _, _, _, request, status, nbytes = m.groups()
            r = self.req.match(request)
            path_ = r.group(2) if r else self.fallback.match(request).group(1)
            agg.add(host, f"{yy}-{self.months[mon]}-{dd}", int(hh), path_, int(status),
                    0 if nbytes == "-" else int(nbytes))
        return len(lines)

    def process_batch(self, spec: BatchSpec) -> BatchResult:
        t = time.monotonic()
        agg, malformed, read = _Aggregates(), [], 0
        for piece in spec.pieces:
            read += self._parse_piece(spec.input_dir / piece["file"], agg, malformed)
        ms = {"parse": int((time.monotonic() - t) * 1000)}
        ms.update(agg.write(spec.output_dir))
        write_tsv(spec.output_dir / "malformed.tsv", malformed)
        self.run_total.merge(agg)
        return BatchResult(batch_id=spec.batch_id, records_read=read, valid_records=agg.valid,
                           malformed_records=agg.malformed, engine_ms=ms)

    def finalize(self, run: RunContext) -> BatchResult:
        ms = self.run_total.write(run.final_output_dir)
        tot = self.run_total
        return BatchResult(batch_id=None, records_read=tot.valid + tot.malformed,
                           valid_records=tot.valid, malformed_records=tot.malformed, engine_ms=ms)
