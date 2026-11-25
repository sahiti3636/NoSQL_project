"""Hive pipeline.

Person C: run database and tables (prepare), raw-line ingestion with RegexSerDe, parsing inside
          HiveQL with the contract regexes, malformed records, Q1.
Person D: Q2, Q3, whole-run results (finalize), cleanup.

How the work is split between Python and Hive
---------------------------------------------
Python only writes HiveQL scripts and runs them with beeline inside the Hive container. It never
reads a log line. Hive does everything else:

  batch_000001/input  --RegexSerDe-->  raw_batch (1 column: line)
  raw_batch  --one scan, multi-insert-->  parsed_records     (valid lines, partition batch_id)
                                          malformed_records  (raw text,    partition batch_id)
                                          counts             (read / valid / malformed)
  parsed_records  --GROUP BY-->  q1.tsv, q2.tsv, q3.tsv   (per batch, and over all batches)

Why these choices
  * raw_batch uses RegexSerDe with '([\\s\\S]*)': the default text format splits fields on byte
    0x01, which occurs inside 7 real request lines and would cut them.
  * parsed_records and malformed_records are ORC: a TEXTFILE table would split those same 0x01
    bytes when reading the rows back and shift every later column.
  * One FROM ... INSERT ... INSERT ... reads each batch once instead of three times.
  * Both tables keep every batch (partitioned by batch_id), so finalize() computes exact
    whole-run results, counted inside Hive.
"""
from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

from .. import config, contracts, outputs
from ..engine import BatchResult, BatchSpec, Engine, RunContext

# Every script starts with this: Hive's query-results cache otherwise swallows the output of
# INSERT OVERWRITE LOCAL DIRECTORY.
_HEADER = "SET hive.query.results.cache.enabled=false;\nUSE {db};\n"
# Contract 2: PostgreSQL COPY text format (tab-separated, backslash escapes).
_TSV = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\'"
_ERROR = "status_code BETWEEN 400 AND 599"


def _hql_string(value: str) -> str:
    """A HiveQL single-quoted string literal: backslashes doubled, quotes escaped."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


class HiveEngine(Engine):
    name = "hive"

    def __init__(self) -> None:
        self.db = ""
        self.scripts_dir: Optional[Path] = None

    # ------------------------------------------------------------------ Person C: setup
    def prepare(self, run: RunContext) -> None:
        self.db = "etl_" + run.run_id.replace("-", "")          # one database per run
        self.scripts_dir = run.run_dir / "hql_scripts"           # every script is kept for review
        self.scripts_dir.mkdir(parents=True, exist_ok=True)

        self.env = _hql_string(contracts.envelope_regex())
        self.req = _hql_string(contracts.request_regex())
        self.fb = _hql_string(contracts.request_fallback_regex())
        months = ",".join(f"{name}:{number}" for name, number in contracts.months().items())
        self.month = f"str_to_map('{months}', ',', ':')[regexp_extract(line, {self.env}, 5)]"

        self._run(f"""
CREATE DATABASE IF NOT EXISTS {self.db};
USE {self.db};
CREATE TABLE IF NOT EXISTS parsed_records (
    host STRING,
    log_timestamp STRING,
    log_date STRING,
    log_hour INT,
    http_method STRING,
    resource_path STRING,
    protocol_version STRING,
    status_code INT,
    bytes_transferred BIGINT
) PARTITIONED BY (batch_id INT) STORED AS ORC;
CREATE TABLE IF NOT EXISTS malformed_records (line STRING)
  PARTITIONED BY (batch_id INT) STORED AS ORC;
""", "00_init", use_db=False)

    # ------------------------------------------------------------------ Person C: parse one batch
    def process_batch(self, spec: BatchSpec) -> BatchResult:
        ms: Dict[str, int] = {}
        b = spec.batch_id
        out = config.to_container_path(spec.output_dir)
        env = self.env
        ms["parse"] = self._run(f"""
DROP TABLE IF EXISTS raw_batch;
CREATE EXTERNAL TABLE raw_batch (line STRING)
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
  WITH SERDEPROPERTIES ('input.regex' = '([\\\\s\\\\S]*)')
  STORED AS TEXTFILE LOCATION 'file://{config.to_container_path(spec.input_dir)}';

ALTER TABLE parsed_records DROP IF EXISTS PARTITION (batch_id = {b});
ALTER TABLE malformed_records DROP IF EXISTS PARTITION (batch_id = {b});

-- one scan of the batch feeds all three outputs
FROM (
  SELECT line,
         line RLIKE {env} AND {self.month} IS NOT NULL AS ok,
         regexp_extract(line, {env}, 11) AS req
  FROM raw_batch
) t
INSERT INTO TABLE parsed_records PARTITION (batch_id = {b})
SELECT
  regexp_extract(line, {env}, 1),
  concat(regexp_extract(line, {env}, 6), '-', {self.month}, '-', regexp_extract(line, {env}, 4), ' ',
         regexp_extract(line, {env}, 7), ':', regexp_extract(line, {env}, 8), ':',
         regexp_extract(line, {env}, 9), ' ', regexp_extract(line, {env}, 10)),
  concat(regexp_extract(line, {env}, 6), '-', {self.month}, '-', regexp_extract(line, {env}, 4)),
  cast(regexp_extract(line, {env}, 7) AS INT),
  if(req RLIKE {self.req}, regexp_extract(req, {self.req}, 1), ''),
  if(req RLIKE {self.req}, regexp_extract(req, {self.req}, 2), regexp_extract(req, {self.fb}, 1)),
  if(req RLIKE {self.req}, nvl(regexp_extract(req, {self.req}, 3), ''), ''),
  cast(regexp_extract(line, {env}, 12) AS INT),
  cast(if(regexp_extract(line, {env}, 13) = '-', '0', regexp_extract(line, {env}, 13)) AS BIGINT)
WHERE ok
INSERT INTO TABLE malformed_records PARTITION (batch_id = {b})
SELECT line
WHERE NOT ok
INSERT OVERWRITE LOCAL DIRECTORY '{out}/counts'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
SELECT count(*), sum(if(ok, 1, 0)), sum(if(ok, 0, 1));

INSERT OVERWRITE LOCAL DIRECTORY '{out}/malformed.tsv' {_TSV}
SELECT line FROM malformed_records WHERE batch_id = {b};
""", f"batch_{b:06d}_01_parse")
        read, valid, malformed = self._read_counts(spec.output_dir / "counts", 3)

        ms.update(self._queries(f"WHERE batch_id = {b}", out, f"batch_{b:06d}"))
        return BatchResult(batch_id=b, records_read=read, valid_records=valid,
                           malformed_records=malformed, engine_ms=ms)

    # ------------------------------------------------------------------ Person D: whole run
    def finalize(self, run: RunContext) -> BatchResult:
        out = config.to_container_path(run.final_output_dir)
        ms = self._queries("", out, "final")
        self._run(f"""
INSERT OVERWRITE LOCAL DIRECTORY '{out}/counts'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
SELECT v.n, m.n
FROM (SELECT count(*) AS n FROM parsed_records) v
CROSS JOIN (SELECT count(*) AS n FROM malformed_records) m;
""", "final_04_counts")
        valid, malformed = self._read_counts(run.final_output_dir / "counts", 2)
        return BatchResult(batch_id=None, records_read=valid + malformed, valid_records=valid,
                           malformed_records=malformed, engine_ms=ms)

    def cleanup(self, run: RunContext) -> None:
        if not self.db:
            return
        try:
            self._run(f"DROP DATABASE IF EXISTS {self.db} CASCADE;", "99_cleanup", use_db=False)
        except Exception:
            pass                                               # cleanup must never raise

    # ------------------------------------------------------------------ queries (C: Q1, D: Q2, Q3)
    def _queries(self, where: str, out: str, label: str) -> Dict[str, int]:
        """Runs Q1-Q3 over the parsed rows selected by `where` ('' = every batch)."""
        p_where = where.replace("batch_id", "p.batch_id")
        sql = {
            # Q1 Daily Traffic Summary (Person C)
            "q1": f"""
INSERT OVERWRITE LOCAL DIRECTORY '{out}/q1.tsv' {_TSV}
SELECT log_date, status_code, count(*), sum(bytes_transferred)
FROM parsed_records {where}
GROUP BY log_date, status_code;
""",
            # Q2 Top 20 Requested Resources (Person D). Ties: path in byte order (Contract 2).
            # Distinct hosts are counted only for the 20 winners, joined back to the rows.
            "q2": f"""
WITH top_resources AS (
  SELECT resource_path FROM parsed_records {where}
  GROUP BY resource_path
  ORDER BY count(*) DESC, resource_path ASC
  LIMIT 20
)
INSERT OVERWRITE LOCAL DIRECTORY '{out}/q2.tsv' {_TSV}
SELECT p.resource_path, count(*) AS request_count, sum(p.bytes_transferred),
       count(DISTINCT p.host)
FROM parsed_records p JOIN top_resources t ON p.resource_path = t.resource_path
{p_where}
GROUP BY p.resource_path
ORDER BY request_count DESC, p.resource_path ASC;
""",
            # Q3 Hourly Error Analysis (Person D). Every hour with traffic is kept, so hours
            # with zero errors appear (no WHERE on status before grouping).
            "q3": f"""
INSERT OVERWRITE LOCAL DIRECTORY '{out}/q3.tsv' {_TSV}
SELECT log_date, log_hour,
       sum(if({_ERROR}, 1, 0)),
       count(*),
       sum(if({_ERROR}, 1, 0)) / count(*),
       count(DISTINCT if({_ERROR}, host, NULL))
FROM parsed_records {where}
GROUP BY log_date, log_hour;
""",
        }
        return {qid: self._run(q, f"{label}_0{i}_{qid}") for i, (qid, q) in enumerate(sql.items(), 1)}

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _read_counts(path: Path, n: int) -> Tuple[int, ...]:
        """Reads the one-row counts file Hive wrote, then deletes it (it is not a result file)."""
        rows = list(outputs.read_rows(path))
        shutil.rmtree(path, ignore_errors=True)
        if len(rows) != 1 or len(rows[0]) != n:
            raise RuntimeError(f"Hive: unexpected counts output in {path}: {rows!r}")
        return tuple(int(v) if v not in ("", "\\N") else 0 for v in rows[0])

    def _run(self, body: str, label: str, use_db: bool = True) -> int:
        """Writes the script to runs/<run>/hql_scripts/<label>.hql, runs it with beeline inside the
        Hive container and returns how long it took in ms. Raises with Hive's error on failure."""
        if self.scripts_dir is None:
            raise RuntimeError("HiveEngine.prepare() has not run")
        header = _HEADER.format(db=self.db) if use_db else _HEADER.split("\n")[0] + "\n"
        script = self.scripts_dir / f"{label}.hql"
        script.write_text(header + body, encoding="utf-8")

        t = time.monotonic()
        proc = subprocess.run(
            ["docker", "compose", "exec", "-T", "hive", "beeline", "-u", config.HIVE_JDBC,
             "--silent=true", "-f", config.to_container_path(script)],
            cwd=config.PROJECT_ROOT, capture_output=True, text=True)
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()[-3000:]
            raise RuntimeError(f"Hive script {label} failed (exit {proc.returncode}):\n{detail}")
        return int((time.monotonic() - t) * 1000)
