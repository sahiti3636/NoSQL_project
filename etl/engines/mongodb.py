"""MongoDB pipeline.

Part A (done): raw-line ingestion, parsing inside MongoDB, malformed records, Q1.
Part B (to do): Q2 and Q3 - fill in _q2_pipeline() and _q3_pipeline() at the bottom.

How the work is split between Python and MongoDB
------------------------------------------------
Python only MOVES bytes: it reads each batch piece in binary, splits on b"\\n", strips one
trailing b"\\r" and decodes as UTF-8 (invalid bytes -> U+FFFD, because MongoDB only stores valid
UTF-8; Hadoop does the same for the other engines). Each line is inserted UNPARSED as
{b: batch_id, n: line_no, raw: "..."}.

Everything else runs inside MongoDB as aggregation pipelines:
  raw_<run>    --parse ($regexFind with the contract regexes)-->  parsed_<run>   ($merge)
  parsed_<run> --Q1/Q2/Q3 ($group ...)-->                         result rows

parsed_<run> keeps every batch, so finalize() can compute exact whole-run results over all of
them. A batch's raw documents are deleted once parsed (malformed lines keep their raw text in
parsed_<run>), so MongoDB never holds two copies of the data.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, List

from pymongo import ASCENDING, MongoClient

from .. import config, contracts
from ..engine import BatchResult, BatchSpec, Engine, RunContext
from ..outputs import write_tsv

INSERT_CHUNK = 20_000          # raw documents per insert_many call


def _cap(array: str, i: int) -> dict:
    return {"$arrayElemAt": [array, i]}


class MongoEngine(Engine):
    name = "mongodb"

    # ------------------------------------------------------------------ lifecycle
    def prepare(self, run: RunContext) -> None:
        self.client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
        self.client.admin.command("ping")                     # fail fast if MongoDB is down
        self.db = self.client["nasa_etl"]
        tag = run.run_id.replace("-", "")
        self.raw = self.db[f"raw_{tag}"]
        self.parsed = self.db[f"parsed_{tag}"]
        self.raw.drop()
        self.parsed.drop()
        self.raw.create_index([("b", ASCENDING)])
        self.parsed.create_index([("b", ASCENDING), ("ok", ASCENDING)])

        self.envelope = contracts.envelope_regex()
        self.request = contracts.request_regex()
        self.fallback = contracts.request_fallback_regex()
        months = contracts.months()
        self.month_names = list(months.keys())                # ['Jan', ..., 'Dec']
        self.month_numbers = list(months.values())            # ['01', ..., '12']

    def process_batch(self, spec: BatchSpec) -> BatchResult:
        ms: Dict[str, int] = {}

        t = time.monotonic()
        read = self._ingest(spec)
        ms["ingest"] = _ms(t)

        t = time.monotonic()
        self.raw.aggregate(self._parse_pipeline(spec.batch_id))
        self.raw.delete_many({"b": spec.batch_id})
        valid, malformed = self._counts({"b": spec.batch_id})
        ms["parse"] = _ms(t)

        write_tsv(spec.output_dir / "malformed.tsv",
                  ((d["raw"],) for d in self.parsed.find({"b": spec.batch_id, "ok": False},
                                                         {"_id": 0, "raw": 1}).sort("n", ASCENDING)))
        ms.update(self._queries({"b": spec.batch_id, "ok": True}, spec.output_dir))
        return BatchResult(batch_id=spec.batch_id, records_read=read, valid_records=valid,
                           malformed_records=malformed, engine_ms=ms)

    def finalize(self, run: RunContext) -> BatchResult:
        valid, malformed = self._counts({})
        ms = self._queries({"ok": True}, run.final_output_dir)
        return BatchResult(batch_id=None, records_read=valid + malformed, valid_records=valid,
                           malformed_records=malformed, engine_ms=ms)

    def cleanup(self, run: RunContext) -> None:
        try:
            self.raw.drop()
            self.parsed.drop()
            self.client.close()
        except Exception:
            pass                                               # cleanup must never raise

    # ------------------------------------------------------------------ ingestion (Python moves bytes)
    def _ingest(self, spec: BatchSpec) -> int:
        """Insert every line of the batch unparsed. Returns how many lines MongoDB now holds."""
        docs: List[dict] = []
        for piece in spec.pieces:
            line_no = piece["first_line_no"]
            lines = (spec.input_dir / piece["file"]).read_bytes().split(b"\n")
            if lines and lines[-1] == b"":
                lines.pop()                                    # a final newline makes no record
            for raw in lines:
                if raw.endswith(b"\r"):
                    raw = raw[:-1]
                docs.append({"b": spec.batch_id, "n": line_no, "raw": raw.decode("utf-8", "replace")})
                line_no += 1
                if len(docs) == INSERT_CHUNK:
                    self.raw.insert_many(docs, ordered=False)
                    docs = []
        if docs:
            self.raw.insert_many(docs, ordered=False)
        return self.raw.count_documents({"b": spec.batch_id})  # counted by MongoDB, not Python

    # ------------------------------------------------------------------ parsing (inside MongoDB)
    def _parse_pipeline(self, batch_id: int) -> list:
        """Contract 1, as one aggregation. Same logic as tests/smoke/regex_test_mongo.js."""
        ok = "$ok"
        field = lambda expr: {"$cond": [ok, expr, "$$REMOVE"]}     # valid lines only
        return [
            {"$match": {"b": batch_id}},
            {"$set": {"e": {"$regexFind": {"input": "$raw", "regex": self.envelope}}}},
            {"$set": {"c": "$e.captures"}},
            # month index, or -1 when the envelope did not match / month name is unknown
            {"$set": {"mi": {"$cond": [{"$eq": ["$e", None]}, -1,
                                       {"$indexOfArray": [self.month_names, _cap("$c", 4)]}]}}},
            {"$set": {"ok": {"$gte": ["$mi", 0]}}},
            {"$set": {
                "r": {"$cond": [ok, {"$regexFind": {"input": _cap("$c", 10), "regex": self.request}}, None]},
                "f": {"$cond": [ok, {"$regexFind": {"input": _cap("$c", 10), "regex": self.fallback}}, None]},
            }},
            {"$project": {
                "_id": 1, "b": 1, "n": 1, "ok": 1,
                "raw": {"$cond": [ok, "$$REMOVE", "$raw"]},           # kept only for malformed lines
                "host": field(_cap("$c", 0)),
                "log_date": field({"$concat": [_cap("$c", 5), "-",
                                               {"$arrayElemAt": [self.month_numbers, "$mi"]}, "-",
                                               _cap("$c", 3)]}),
                "log_hour": field({"$toInt": _cap("$c", 6)}),
                "http_method": field({"$cond": ["$r", _cap("$r.captures", 0), ""]}),
                "resource_path": field({"$cond": ["$r", _cap("$r.captures", 1), _cap("$f.captures", 0)]}),
                "protocol_version": field({"$cond": ["$r", {"$ifNull": [_cap("$r.captures", 2), ""]}, ""]}),
                "status_code": field({"$toInt": _cap("$c", 11)}),
                "bytes_transferred": field({"$cond": [{"$eq": [_cap("$c", 12), "-"]},
                                                      {"$toLong": 0}, {"$toLong": _cap("$c", 12)}]}),
            }},
            {"$merge": {"into": self.parsed.name, "whenMatched": "fail", "whenNotMatched": "insert"}},
        ]

    def _counts(self, match: dict) -> tuple:
        """(valid, malformed) counted inside MongoDB."""
        got = {d["_id"]: d["n"] for d in self.parsed.aggregate(
            [{"$match": match}, {"$group": {"_id": "$ok", "n": {"$sum": 1}}}])}
        return got.get(True, 0), got.get(False, 0)

    # ------------------------------------------------------------------ queries
    def _queries(self, match: dict, out_dir: Path) -> Dict[str, int]:
        """Runs Q1-Q3 over the parsed rows selected by `match` and writes q1/q2/q3.tsv."""
        ms = {}
        for qid, build, columns in (("q1", self._q1_pipeline, contracts.output_columns("q1.tsv")),
                                    ("q2", self._q2_pipeline, contracts.output_columns("q2.tsv")),
                                    ("q3", self._q3_pipeline, contracts.output_columns("q3.tsv"))):
            t = time.monotonic()
            rows = self.parsed.aggregate([{"$match": match}] + build(), allowDiskUse=True)
            write_tsv(out_dir / f"{qid}.tsv", ([d[c] for c in columns] for d in rows))
            ms[qid] = _ms(t)
        return ms

    def _q1_pipeline(self) -> list:
        """Q1 Daily Traffic Summary: per (log_date, status_code) -> request_count, total_bytes."""
        return [
            {"$group": {"_id": {"d": "$log_date", "s": "$status_code"},
                        "request_count": {"$sum": 1},
                        "total_bytes": {"$sum": "$bytes_transferred"}}},
            {"$project": {"_id": 0, "log_date": "$_id.d", "status_code": "$_id.s",
                          "request_count": 1, "total_bytes": 1}},
            {"$sort": {"log_date": 1, "status_code": 1}},
        ]

    # ------------------------------------------------------------------ PERSON B: fill these in
    def _q2_pipeline(self) -> list:
        """Q2 Top Requested Resources. Must return stages that output documents with fields
        resource_path, request_count, total_bytes, distinct_host_count (exactly 20 rows,
        sorted by request_count descending, then resource_path ascending).
        Parsed fields available: host, log_date, log_hour, http_method, resource_path,
        protocol_version, status_code, bytes_transferred. The $match is added for you."""
        raise NotImplementedError("MongoDB Q2 (Person B) not written yet")

    def _q3_pipeline(self) -> list:
        """Q3 Hourly Error Analysis. Must return stages that output documents with fields
        log_date, log_hour, error_request_count, total_request_count, error_rate,
        distinct_error_hosts - one per (log_date, log_hour) with any request, INCLUDING hours
        with zero errors. Error = 400 <= status_code <= 599. The $match is added for you."""
        raise NotImplementedError("MongoDB Q3 (Person B) not written yet")


def _ms(start: float) -> int:
    return int((time.monotonic() - start) * 1000)
