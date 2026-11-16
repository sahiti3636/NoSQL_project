"""Loads engine output into PostgreSQL (Contract 3). One transaction per batch, one for the
final result, then a tiny one that flips the run to 'succeeded' (so a half-loaded run is never
visible in the report views)."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg

from . import contracts, outputs

_SQL_TYPES = {"date": "date", "int": "int", "bigint": "bigint", "double": "double precision",
              "text": "text"}
_RESULT_TABLES = {"q1": "etl.q1_daily_traffic", "q2": "etl.q2_top_resources",
                  "q3": "etl.q3_hourly_errors"}


def _columns(file_name: str):
    return [(c["name"], _SQL_TYPES[c["type"]]) for c in contracts.outputs()["files"][file_name]["columns"]]


class Loader:
    def __init__(self, dsn: str):
        self.conn = psycopg.connect(dsn)
        with self.conn.cursor() as cur:   # staging tables shaped exactly like the output files
            for name in ("q1.tsv", "q2.tsv", "q3.tsv", "malformed.tsv"):
                cols = ", ".join(f"{n} {t}" for n, t in _columns(name))
                cur.execute(f"CREATE TEMP TABLE stage_{name.split('.')[0]} ({cols}) ON COMMIT DELETE ROWS")
        self.conn.commit()

    def close(self):
        self.conn.close()

    # ---- run lifecycle -------------------------------------------------------------------
    def start_run(self, run_id: str, pipeline: str, batch_size: int, started_at: datetime,
                  environment: dict) -> None:
        with self.conn.cursor() as cur:
            cur.execute("""INSERT INTO etl.etl_run (run_id, pipeline, batch_size, status, started_at,
                                                    input_manifest, environment)
                           VALUES (%s, %s, %s, 'running', %s, '[]', %s)""",
                        (run_id, pipeline, batch_size, started_at, json.dumps(environment)))
        self.conn.commit()

    def load_batch(self, run_id: str, pipeline: str, spec, result, started_at: datetime,
                   finished_at: datetime) -> None:
        with self.conn.cursor() as cur:
            cur.execute("""INSERT INTO etl.etl_batch (run_id, batch_id, record_count, valid_records,
                               malformed_records, first_line_no, last_line_no, source_pieces,
                               started_at, finished_at, duration_ms)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (run_id, spec.batch_id, spec.record_count, result.valid_records,
                         result.malformed_records, spec.first_line_no, spec.last_line_no,
                         json.dumps(spec.pieces), started_at, finished_at,
                         int((finished_at - started_at).total_seconds() * 1000)))
            self._load_queries(cur, run_id, pipeline, spec.batch_id, spec.output_dir, result, finished_at)
            data = outputs.read_bytes(spec.output_dir / "malformed.tsv")
            if data:
                self._copy(cur, "stage_malformed", data)
                cur.execute("""INSERT INTO etl.malformed_record (run_id, batch_id, raw_line)
                               SELECT %s, %s, raw_line FROM stage_malformed""", (run_id, spec.batch_id))
        self.conn.commit()

    def load_final(self, run_id: str, pipeline: str, output_dir: Path, result,
                   executed_at: datetime) -> None:
        with self.conn.cursor() as cur:
            self._load_queries(cur, run_id, pipeline, None, output_dir, result, executed_at)
        self.conn.commit()

    def mark_succeeded(self, run_id: str, started_at: datetime, finished_at: datetime,
                       runtime_ms: int, total: int, valid: int, malformed: int, num_batches: int,
                       manifest: list, input_sha256: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute("""UPDATE etl.etl_run SET status='succeeded', started_at=%s, finished_at=%s,
                               runtime_ms=%s, total_records=%s, valid_records=%s, malformed_records=%s,
                               num_batches=%s, input_manifest=%s, input_sha256=%s
                           WHERE run_id=%s""",
                        (started_at, finished_at, runtime_ms, total, valid, malformed, num_batches,
                         json.dumps(manifest), input_sha256, run_id))
        self.conn.commit()

    def mark_failed(self, run_id: str, message: str) -> None:
        self.conn.rollback()
        with self.conn.cursor() as cur:
            cur.execute("UPDATE etl.etl_run SET status='failed', error_message=%s WHERE run_id=%s",
                        (message[:4000], run_id))
        self.conn.commit()

    # ---- helpers -------------------------------------------------------------------------
    def _copy(self, cur, table: str, data: bytes) -> None:
        with cur.copy(f"COPY {table} FROM STDIN") as cp:
            cp.write(data)

    def _load_queries(self, cur, run_id, pipeline, batch_id: Optional[int], output_dir: Path,
                      result, executed_at: datetime) -> None:
        for qid, file_name in contracts.QUERY_FILES.items():
            path = Path(output_dir) / file_name
            if not outputs.data_files(path):
                raise RuntimeError(f"{pipeline}: missing {file_name} in {output_dir}")
            data = outputs.read_bytes(path)
            rows = list(outputs.read_rows(path))
            stage = f"stage_{qid}"
            self._copy(cur, stage, data)
            cols = ", ".join(n for n, _ in _columns(file_name))
            cur.execute(f"""INSERT INTO {_RESULT_TABLES[qid]} (run_id, pipeline, batch_id, executed_at, {cols})
                            SELECT %s, %s, %s, %s, {cols} FROM {stage}""",
                        (run_id, pipeline, batch_id, executed_at))
            cur.execute("""INSERT INTO etl.query_execution (run_id, query_id, batch_id, executed_at,
                               duration_ms, row_count, fingerprint)
                           VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                        (run_id, qid, batch_id, executed_at, int(result.engine_ms.get(qid, 0)),
                         len(rows), outputs.fingerprint(qid, rows)))
