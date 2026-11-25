# Hive engine (Aman and Prajwal)

Code: [`etl/engines/hive.py`](../etl/engines/hive.py). Shared rules: [`contracts/`](../contracts/README.md).

| Person | Part |
| --- | --- |
| **Aman** (C) | `prepare()`: one Hive database per run, the `parsed_records` and `malformed_records` tables. Raw-line ingestion with `RegexSerDe`. Parsing inside HiveQL with the contract regexes. Malformed records. **Q1** daily traffic. |
| **Prajwal** (D) | **Q2** top 20 resources, **Q3** hourly errors. `finalize()` for whole-run results. `cleanup()`. |

## How a batch flows through Hive

Python only writes HiveQL scripts and runs them with `beeline` inside the Hive container. It never reads a log line.

```
runs/<run>/batch_000001/input   (byte-identical pieces of the logs, cut by the controller)
        │  RegexSerDe, input.regex = '([\s\S]*)'
        ▼
raw_batch (line STRING)                         external table, one row per line
        │  ONE scan:  FROM (...) INSERT ... INSERT ... INSERT ...
        ├──► parsed_records    PARTITION (batch_id)   valid lines, 9 parsed columns, ORC
        ├──► malformed_records PARTITION (batch_id)   raw text of bad lines, ORC
        └──► counts                                   read / valid / malformed, counted by Hive
parsed_records ── GROUP BY ──► q1.tsv  q2.tsv  q3.tsv   (this batch: WHERE batch_id = N)
malformed_records ───────────► malformed.tsv
finalize(): the same Q1–Q3 with no WHERE, over every batch → runs/<run>/final/output/
```

Every script the engine runs is saved in `runs/<run>/hql_scripts/`. This is useful for the demo and for debugging.

## Design choices and why

| Choice | Why |
| --- | --- |
| Raw lines are read with `RegexSerDe` and `([\s\S]*)` | The default text format splits columns on byte `0x01`. This byte occurs inside **7 real request lines**, for example July line 691,503 (`klothos.crl.research.digital.com … "\x05\x01" 400 -`), and the split would cut them. |
| `parsed_records` and `malformed_records` are stored as **ORC** | When the first version used a `TEXTFILE` table, Hive split those same `0x01` bytes again while reading rows back. That shifted `status_code` into the wrong column and PostgreSQL rejected the load. ORC is a binary columnar format with no field delimiter, so control bytes stay inside their column. |
| One multi-insert per batch | A single scan of `raw_batch` fills the parsed table, the malformed table and the counts. The first version scanned each batch 3 times. |
| Tables partitioned by `batch_id`, kept for the whole run | `finalize()` computes exact whole-run Q1–Q3. Distinct hosts can't be added up across batches. Re-running a batch drops and replaces only its partition. |
| Malformed lines are kept in a Hive table | Both the per-batch and the whole-run malformed counts are counted inside Hive, not in Python. |
| Regexes are loaded from `contracts/` at run time and escaped for HiveQL by `_hql_string()` | Every engine uses the exact same regex text. Inside a HiveQL string every `\` must be doubled. |
| Month comes from `str_to_map` built from `months.tsv` | An unknown month makes the line malformed, as in every other engine. No date functions are used. |
| `SET hive.query.results.cache.enabled=false;` at the top of every script | Without it, `INSERT OVERWRITE LOCAL DIRECTORY` writes nothing. |
| Output uses `ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ESCAPED BY '\\'` | This is PostgreSQL `COPY` text format (Contract 2). A tab inside a path is written as backslash + tab, which `COPY` reads back correctly. |
| The stale `hiveserver2.pid` is removed before the image's entrypoint (`docker-compose.yml`) | The image keeps the pid file inside the container. After Docker Desktop restarts, HiveServer2 otherwise refuses to start ("running as process 7. Stop it first"). |
| One Hive database per run (`etl_<run_id>`), dropped in `cleanup()` | Runs never see each other's tables. `raw_batch` is an external table, so dropping it never deletes the batch input files. |

## Parsing (Aman)

Envelope regex groups: 1 host, 4 day, 5 month, 6 year, 7 hour, 8 minute, 9 second, 10 zone, 11 request, 12 status, 13 bytes.

| Column | HiveQL |
| --- | --- |
| valid line? | `line RLIKE <envelope> AND <month map>[group 5] IS NOT NULL` |
| `host` | group 1 |
| `log_timestamp` | `concat(log date, ' ', hour, ':', minute, ':', second, ' ', zone)`, e.g. `1995-07-01 00:00:01 -0400` |
| `log_date` | `concat(year, '-', month number, '-', day)` |
| `log_hour` | `cast(group 7 AS INT)` |
| `http_method`, `resource_path`, `protocol_version` | request regex groups 1–3. If the request regex doesn't match, `resource_path` = fallback group 1 and the other two are `''`. |
| `status_code` | `cast(group 12 AS INT)` |
| `bytes_transferred` | `'-'` → `0`, then `cast(... AS BIGINT)` (64-bit: the byte total is 65.5 GB) |

## Queries

- **Q1 (Aman):** `GROUP BY log_date, status_code` → `count(*)`, `sum(bytes_transferred)`.
- **Q2 (Prajwal):** a CTE picks the top 20 paths (`ORDER BY count(*) DESC, resource_path ASC LIMIT 20`). It is then joined back to compute `count(DISTINCT host)` only for those 20.
- **Q3 (Prajwal):** `GROUP BY log_date, log_hour` over **all** rows, so hours with zero errors are kept. Errors are `status_code BETWEEN 400 AND 599`, counted with `sum(if(...))` and `count(DISTINCT if(..., host, NULL))`.

## Verification

- **Smoke test** (`./tests/smoke/run.sh`): 2,038 tricky real lines, 0 field mismatches against the Python reference. A tab inside a path survives escaping.
- **Parsed table:** on the same sample, `parsed_records` (all nine columns, `log_timestamp` included) equals Python's parse, row for row.
- **Full runs** (25 Nov 2026), each 3,461,613 records = 3,461,612 valid + 1 malformed (`alyssa.p`):

| Batch size | Batches | Runtime | Answer key | vs MongoDB |
| --- | ---: | ---: | --- | --- |
| 100,000 | 35 | 425.1 s | passed | EQUIVALENT |
| 500,000 | 7 | 207.2 s | passed | EQUIVALENT |
| 1,000,000 | 4 | 171.4 s | passed | EQUIVALENT |
| no batching | 1 | 149.3 s | passed | EQUIVALENT |

Hive pays about 8 s per batch (four beeline scripts, each compiled into Tez work), so bigger batches are much faster. The first version read each batch three times; the multi-insert cut a 500k run from 276 s to 165 s. See [BENCHMARKS.md](BENCHMARKS.md) and [VALIDATION.md](VALIDATION.md).

## Run it

```bash
docker compose -f docker-compose.yml -f compose.lock.yml up -d --wait postgres hive
.venv/bin/python -m etl run --pipeline hive --batch-size 500000
.venv/bin/python -m etl run --pipeline hive --no-batch
.venv/bin/python -m etl report --pipeline hive
```

Hive runs with a 2 GB heap (`HADOOP_HEAPSIZE` in `docker-compose.yml`), which is enough for 500k-line batches. Keep it at 2 GB so it still fits on 8 GB laptops.
