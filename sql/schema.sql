-- =====================================================================
-- NASA Log ETL - reporting database (PostgreSQL 15+)
-- Contract 3 of the shared spec. Safe to re-run: drops and recreates.
--   psql -d nasa_etl -v ON_ERROR_STOP=1 -f sql/schema.sql
-- Scopes: batch_id = 1..n  -> that batch's own result
--         batch_id IS NULL -> exact whole-run (final) result
-- =====================================================================

BEGIN;

DROP SCHEMA IF EXISTS etl CASCADE;
CREATE SCHEMA etl;
SET search_path TO etl;

-- ---------------------------------------------------------------------
-- 1. Runs: one row per execution of the tool
-- ---------------------------------------------------------------------
CREATE TABLE etl_run (
    run_id            uuid        PRIMARY KEY,
    pipeline          text        NOT NULL
                      CHECK (pipeline IN ('hive', 'mongodb', 'pig', 'mapreduce')),
    batch_size        int         NOT NULL CHECK (batch_size > 0),
    status            text        NOT NULL DEFAULT 'running'
                      CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at        timestamptz NOT NULL,          -- first byte of input read
    finished_at       timestamptz,                   -- final COMMIT returned
    runtime_ms        bigint      CHECK (runtime_ms >= 0),
    total_records     bigint      CHECK (total_records >= 0),
    valid_records     bigint      CHECK (valid_records >= 0),
    malformed_records bigint      CHECK (malformed_records >= 0),
    num_batches       int         CHECK (num_batches >= 0),
    avg_batch_size    numeric(14,2) GENERATED ALWAYS AS
                      (total_records::numeric / NULLIF(num_batches, 0)) STORED,
    input_sha256      char(64),                      -- hash of all input files, for comparing runs
    input_manifest    jsonb       NOT NULL,          -- per file: name, size, sha256, line count
    environment       jsonb,                         -- engine versions, machine, git commit
    error_message     text,
    UNIQUE (run_id, pipeline),                       -- target of the result tables' composite FK
    CHECK (valid_records + malformed_records = total_records),
    CHECK (status <> 'succeeded'
           OR (finished_at IS NOT NULL AND runtime_ms IS NOT NULL
               AND total_records IS NOT NULL AND num_batches IS NOT NULL))
);

-- ---------------------------------------------------------------------
-- 2. Batches: one row per batch
-- ---------------------------------------------------------------------
CREATE TABLE etl_batch (
    run_id            uuid        NOT NULL REFERENCES etl_run ON DELETE CASCADE,
    batch_id          int         NOT NULL CHECK (batch_id >= 1),
    record_count      int         NOT NULL CHECK (record_count > 0),
    valid_records     int         NOT NULL CHECK (valid_records >= 0),
    malformed_records int         NOT NULL CHECK (malformed_records >= 0),
    first_line_no     bigint      NOT NULL CHECK (first_line_no >= 1),
    last_line_no      bigint      NOT NULL,
    source_pieces     jsonb       NOT NULL,          -- [{file, first_line_no, last_line_no}], 1 or 2 pieces
    started_at        timestamptz NOT NULL,
    finished_at       timestamptz NOT NULL,
    duration_ms       bigint      NOT NULL CHECK (duration_ms >= 0),
    PRIMARY KEY (run_id, batch_id),
    CHECK (valid_records + malformed_records = record_count),
    CHECK (last_line_no - first_line_no + 1 = record_count),
    CHECK (finished_at >= started_at)
);

-- ---------------------------------------------------------------------
-- 3. Query catalogue and one row per query execution
-- ---------------------------------------------------------------------
CREATE TABLE query_def (
    query_id   text PRIMARY KEY,
    query_name text NOT NULL UNIQUE,
    description text NOT NULL
);

INSERT INTO query_def VALUES
  ('q1', 'Daily Traffic Summary',
   'Per log_date and status_code: request count and total bytes'),
  ('q2', 'Top Requested Resources',
   'Top 20 resource paths by request count, with total bytes and distinct hosts'),
  ('q3', 'Hourly Error Analysis',
   'Per log_date and log_hour: error requests (400-599), total requests, error rate, distinct error hosts');

CREATE TABLE query_execution (
    run_id       uuid        NOT NULL REFERENCES etl_run ON DELETE CASCADE,
    query_id     text        NOT NULL REFERENCES query_def,
    batch_id     int,                                -- NULL = final, whole-run result
    executed_at  timestamptz NOT NULL,
    duration_ms  bigint      NOT NULL CHECK (duration_ms >= 0),
    row_count    int         NOT NULL CHECK (row_count >= 0),
    fingerprint  char(64)    NOT NULL,               -- SHA-256 of canonical sorted rows
    FOREIGN KEY (run_id, batch_id) REFERENCES etl_batch (run_id, batch_id) ON DELETE CASCADE,
    UNIQUE NULLS NOT DISTINCT (run_id, query_id, batch_id)
);

-- ---------------------------------------------------------------------
-- 4. Result tables
--   pipeline and executed_at are stored on every row because the PDF
--   requires them; the composite FK stops pipeline drifting from etl_run.
--   The batch FK only applies when batch_id is not NULL (MATCH SIMPLE).
-- ---------------------------------------------------------------------
CREATE TABLE q1_daily_traffic (
    run_id        uuid        NOT NULL,
    pipeline      text        NOT NULL,
    batch_id      int,
    executed_at   timestamptz NOT NULL,
    log_date      date        NOT NULL,
    status_code   int         NOT NULL CHECK (status_code BETWEEN 100 AND 999),
    request_count bigint      NOT NULL CHECK (request_count > 0),
    total_bytes   bigint      NOT NULL CHECK (total_bytes >= 0),
    FOREIGN KEY (run_id, pipeline) REFERENCES etl_run (run_id, pipeline) ON DELETE CASCADE,
    FOREIGN KEY (run_id, batch_id) REFERENCES etl_batch (run_id, batch_id) ON DELETE CASCADE,
    UNIQUE NULLS NOT DISTINCT (run_id, batch_id, log_date, status_code)
);

CREATE TABLE q2_top_resources (
    run_id              uuid        NOT NULL,
    pipeline            text        NOT NULL,
    batch_id            int,
    executed_at         timestamptz NOT NULL,
    resource_path       text        NOT NULL,
    request_count       bigint      NOT NULL CHECK (request_count > 0),
    total_bytes         bigint      NOT NULL CHECK (total_bytes >= 0),
    distinct_host_count bigint      NOT NULL CHECK (distinct_host_count BETWEEN 1 AND request_count),
    FOREIGN KEY (run_id, pipeline) REFERENCES etl_run (run_id, pipeline) ON DELETE CASCADE,
    FOREIGN KEY (run_id, batch_id) REFERENCES etl_batch (run_id, batch_id) ON DELETE CASCADE,
    UNIQUE NULLS NOT DISTINCT (run_id, batch_id, resource_path)
);

CREATE TABLE q3_hourly_errors (
    run_id               uuid             NOT NULL,
    pipeline             text             NOT NULL,
    batch_id             int,
    executed_at          timestamptz      NOT NULL,
    log_date             date             NOT NULL,
    log_hour             int              NOT NULL CHECK (log_hour BETWEEN 0 AND 23),
    error_request_count  bigint           NOT NULL CHECK (error_request_count >= 0),
    total_request_count  bigint           NOT NULL CHECK (total_request_count > 0),
    error_rate           double precision NOT NULL CHECK (error_rate BETWEEN 0 AND 1),
    distinct_error_hosts bigint           NOT NULL CHECK (distinct_error_hosts >= 0),
    FOREIGN KEY (run_id, pipeline) REFERENCES etl_run (run_id, pipeline) ON DELETE CASCADE,
    FOREIGN KEY (run_id, batch_id) REFERENCES etl_batch (run_id, batch_id) ON DELETE CASCADE,
    UNIQUE NULLS NOT DISTINCT (run_id, batch_id, log_date, log_hour),
    CHECK (error_request_count <= total_request_count),
    CHECK (distinct_error_hosts <= error_request_count),
    CHECK (abs(error_rate - error_request_count::double precision / total_request_count) < 1e-9)
);

-- Malformed lines are loaded per batch only (finalize does not reload them).
CREATE TABLE malformed_record (
    run_id   uuid NOT NULL,
    batch_id int  NOT NULL,
    raw_line text NOT NULL,
    FOREIGN KEY (run_id, batch_id) REFERENCES etl_batch (run_id, batch_id) ON DELETE CASCADE
);
CREATE INDEX ON malformed_record (run_id, batch_id);

-- ---------------------------------------------------------------------
-- 5. Views read by the report (successful runs only)
-- ---------------------------------------------------------------------

-- Run metadata + per-query runtime: what the PDF says the report must show.
CREATE VIEW v_run_report AS
SELECT r.run_id, r.pipeline, d.query_id, d.query_name,
       e.duration_ms       AS query_duration_ms,
       r.runtime_ms, r.batch_size, r.num_batches, r.avg_batch_size,
       r.total_records, r.valid_records, r.malformed_records,
       r.started_at, r.finished_at
FROM etl_run r
JOIN query_execution e ON e.run_id = r.run_id AND e.batch_id IS NULL
JOIN query_def d       ON d.query_id = e.query_id
WHERE r.status = 'succeeded';

-- Per-batch metadata for a run (batch ID, size, timing).
CREATE VIEW v_batch_report AS
SELECT b.run_id, r.pipeline, b.batch_id, b.record_count AS batch_size,
       b.valid_records, b.malformed_records, b.duration_ms, b.started_at
FROM etl_batch b JOIN etl_run r USING (run_id)
WHERE r.status = 'succeeded';

-- Final (whole-run) results, in the PDF's column order.
CREATE VIEW v_q1_final AS
SELECT q.run_id, q.pipeline, q.executed_at,
       q.log_date, q.status_code, q.request_count, q.total_bytes
FROM q1_daily_traffic q JOIN etl_run r USING (run_id)
WHERE q.batch_id IS NULL AND r.status = 'succeeded';

CREATE VIEW v_q2_final AS
SELECT q.run_id, q.pipeline, q.executed_at,
       row_number() OVER (PARTITION BY q.run_id
                          ORDER BY q.request_count DESC, q.resource_path COLLATE "C") AS rank,
       q.resource_path, q.request_count, q.total_bytes, q.distinct_host_count
FROM q2_top_resources q JOIN etl_run r USING (run_id)
WHERE q.batch_id IS NULL AND r.status = 'succeeded';

CREATE VIEW v_q3_final AS
SELECT q.run_id, q.pipeline, q.executed_at,
       q.log_date, q.log_hour, q.error_request_count, q.total_request_count,
       q.error_rate, q.distinct_error_hosts
FROM q3_hourly_errors q JOIN etl_run r USING (run_id)
WHERE q.batch_id IS NULL AND r.status = 'succeeded';

-- Batch totals must add up to the run totals.
CREATE VIEW v_run_integrity AS
SELECT r.run_id, r.pipeline, r.total_records, r.num_batches,
       coalesce(sum(b.record_count), 0)      AS batch_records,
       count(b.batch_id)                     AS batch_rows,
       coalesce(sum(b.malformed_records), 0) AS batch_malformed,
       (r.total_records     = coalesce(sum(b.record_count), 0)
        AND r.num_batches   = count(b.batch_id)
        AND r.malformed_records = coalesce(sum(b.malformed_records), 0)) AS consistent
FROM etl_run r LEFT JOIN etl_batch b USING (run_id)
GROUP BY r.run_id;

-- Same input + same batch size, different pipelines: do the answers match?
CREATE VIEW v_equivalence AS
SELECT r.input_sha256, r.batch_size, e.query_id,
       count(DISTINCT r.pipeline)    AS pipelines_compared,
       count(DISTINCT e.fingerprint) AS distinct_answers,
       count(DISTINCT e.fingerprint) = 1 AS all_agree,
       string_agg(DISTINCT r.pipeline, ', ') AS pipelines
FROM etl_run r
JOIN query_execution e ON e.run_id = r.run_id AND e.batch_id IS NULL
WHERE r.status = 'succeeded'
GROUP BY r.input_sha256, r.batch_size, e.query_id;

-- Runtime comparison across pipelines and batch sizes.
CREATE VIEW v_pipeline_comparison AS
SELECT pipeline, batch_size,
       count(*)                         AS runs,
       round(avg(runtime_ms))           AS avg_runtime_ms,
       min(runtime_ms)                  AS best_runtime_ms,
       round(avg(avg_batch_size), 2)    AS avg_batch_size,
       max(num_batches)                 AS num_batches
FROM etl_run
WHERE status = 'succeeded'
GROUP BY pipeline, batch_size;

COMMIT;
