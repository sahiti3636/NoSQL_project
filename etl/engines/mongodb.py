"""MongoDB pipeline - TO BE IMPLEMENTED BY THE MONGODB TEAM.

Everything below is a template. Replace each `raise NotImplementedError` with real work.
Python may only MOVE raw lines into MongoDB; all parsing and aggregation must run as
MongoDB aggregation pipelines ($regexFind, $group, $addToSet, ...).

Useful facts:
  * connect with pymongo:  MongoClient(etl.config.MONGO_URI)
  * read each piece in spec.input_dir in BINARY mode, split on b"\\n" (a last line without a
    newline still counts), strip one trailing b"\\r", decode with .decode("utf-8", "replace"),
    and insert {run_id, batch_id, line_no, raw} unparsed. That decode is the one thing Python
    does, because MongoDB only stores valid UTF-8 (Hadoop does the same for the other engines).
  * build the regexes from etl.contracts; tests/smoke/regex_test_mongo.js has a working,
    tested aggregation that parses exactly like the reference
  * never use $dateFromString: it converts to UTC and shifts every hour by 4
  * keep parsed rows per run (e.g. collection parsed_<run>) so finalize() can compute exact
    whole-run results; distinct hosts: $addToSet then $size, or a two-stage $group
  * write each result with etl.outputs.write_tsv(spec.output_dir / "q1.tsv", rows)
  * count lines: records_read must equal spec.record_count, or the run fails on purpose.
"""
from ..engine import BatchResult, BatchSpec, Engine, RunContext


class MongoEngine(Engine):
    name = "mongodb"

    def prepare(self, run: RunContext) -> None:
        # e.g. self.db = MongoClient(MONGO_URI)["etl"]; create collections raw_<run>, parsed_<run>
        raise NotImplementedError("MongoDB engine: prepare() not written yet")

    def process_batch(self, spec: BatchSpec) -> BatchResult:
        # 1. insert this batch's raw lines (unparsed)
        # 2. aggregation: parse with $regexFind -> parsed_<run>; count valid and malformed
        # 3. Q1, Q2, Q3 for this batch -> spec.output_dir; malformed.tsv with the raw lines
        # 4. return BatchResult(batch_id=spec.batch_id, records_read=..., valid_records=...,
        #                       malformed_records=..., engine_ms={...})
        raise NotImplementedError("MongoDB engine: process_batch() not written yet")

    def finalize(self, run: RunContext) -> BatchResult:
        # Q1, Q2, Q3 over parsed_<run> -> run.final_output_dir; batch_id=None
        raise NotImplementedError("MongoDB engine: finalize() not written yet")

    def cleanup(self, run: RunContext) -> None:
        # drop raw_<run> and parsed_<run>; never raise from here
        pass
