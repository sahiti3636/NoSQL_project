"""Hive pipeline - TO BE IMPLEMENTED BY THE HIVE TEAM.

Everything below is a template. Replace each `raise NotImplementedError` with real work.
All parsing and aggregation must run as HiveQL inside the Hive container.

Useful facts (see SETUP.md "Things that will bite you"):
  * run HiveQL with:  docker compose exec -T hive beeline -u jdbc:hive2://localhost:10000/ -f /runs/...hql
    (or connect over JDBC on localhost:10000)
  * paths: a batch folder runs/<run>/batch_000001/input on the laptop is
    /runs/<run>/batch_000001/input in the container -> use etl.config.to_container_path()
  * read raw lines with a one-column RegexSerDe table, input.regex = '([\\\\s\\\\S]*)'
  * build every regex from etl.contracts (backslashes doubled inside HiveQL strings);
    tests/smoke/make_hive_sql.py shows a working, tested version of the parsing SELECT
  * start every script with  SET hive.query.results.cache.enabled=false;
  * write results with INSERT OVERWRITE LOCAL DIRECTORY '<output_dir>/q1.tsv'
      ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\'
    The loader accepts q1.tsv as a folder of part files and skips the .crc files.
  * keep each batch's parsed rows (e.g. a table partitioned by batch_id) so finalize() can
    compute exact whole-run results over all of them.
  * count lines: records_read must equal spec.record_count, or the run fails on purpose.
"""
from ..engine import BatchResult, BatchSpec, Engine, RunContext


class HiveEngine(Engine):
    name = "hive"

    def prepare(self, run: RunContext) -> None:
        # e.g. CREATE DATABASE IF NOT EXISTS etl_<run_id without dashes>; create the parsed table
        raise NotImplementedError("Hive engine: prepare() not written yet")

    def process_batch(self, spec: BatchSpec) -> BatchResult:
        # 1. external RegexSerDe table over to_container_path(spec.input_dir)
        # 2. INSERT the parsed rows (+ batch_id) into the run's parsed table; count malformed
        # 3. Q1, Q2, Q3 for this batch -> spec.output_dir / q1.tsv, q2.tsv, q3.tsv; malformed.tsv
        # 4. return BatchResult(batch_id=spec.batch_id, records_read=..., valid_records=...,
        #                       malformed_records=..., engine_ms={"parse": ..., "q1": ...})
        raise NotImplementedError("Hive engine: process_batch() not written yet")

    def finalize(self, run: RunContext) -> BatchResult:
        # Q1, Q2, Q3 over the whole parsed table -> run.final_output_dir; batch_id=None
        raise NotImplementedError("Hive engine: finalize() not written yet")

    def cleanup(self, run: RunContext) -> None:
        # DROP DATABASE etl_<run> CASCADE; never raise from here
        pass
