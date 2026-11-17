# Hive engine (Persons C and D)

Code: [`etl/engines/hive.py`](../etl/engines/hive.py). Shared rules: [`contracts/`](../contracts/README.md).

| Person | Part |
| --- | --- |
| **C** | `prepare()`: one Hive database per run, the `parsed_records` and `malformed_records` tables. Raw-line ingestion with `RegexSerDe`. Parsing inside HiveQL with the contract regexes. Malformed records. **Q1** daily traffic. |
| **D** | **Q2** top 20 resources, **Q3** hourly errors. `finalize()` for whole-run results. `cleanup()`. |

## How a batch flows through Hive

Python only writes HiveQL scripts and runs them with `beeline` inside the Hive container. It never reads a log line.

```
runs/<run>/batch_000001/input   (byte-identical pieces of the logs, cut by the controller)
        │  RegexSerDe, input.regex = '([\s\S]*)'
        ▼
raw_batch (line STRING)                         external table, one row per line
        │  ONE scan:  FROM (...) INSERT ... INSERT ... INSERT ...
        ├──► parsed_records    PARTITION (batch_id)   valid lines, 8 parsed columns, ORC
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
| One Hive database per run (`etl_<run_id>`), dropped in `cleanup()` | Runs never see each other's tables. `raw_batch` is an external table, so dropping it never deletes the batch input files. |

## Parsing (Person C)

Envelope regex groups: 1 host, 4 day, 5 month, 6 year, 7 hour, 11 request, 12 status, 13 bytes.

| Column | HiveQL |
| --- | --- |
| valid line? | `line RLIKE <envelope> AND <month map>[group 5] IS NOT NULL` |
| `host` | group 1 |
| `log_date` | `concat(year, '-', month number, '-', day)` |
| `log_hour` | `cast(group 7 AS INT)` |
| `http_method`, `resource_path`, `protocol_version` | request regex groups 1–3. If the request regex doesn't match, `resource_path` = fallback group 1 and the other two are `''`. |
| `status_code` | `cast(group 12 AS INT)` |
| `bytes_transferred` | `'-'` → `0`, then `cast(... AS BIGINT)` (64-bit: the byte total is 65.5 GB) |

## Queries

- **Q1 (C):** `GROUP BY log_date, status_code` → `count(*)`, `sum(bytes_transferred)`.
- **Q2 (D):** a CTE picks the top 20 paths (`ORDER BY count(*) DESC, resource_path ASC LIMIT 20`). It is then joined back to compute `count(DISTINCT host)` only for those 20.
- **Q3 (D):** `GROUP BY log_date, log_hour` over **all** rows, so hours with zero errors are kept. Errors are `status_code BETWEEN 400 AND 599`, counted with `sum(if(...))` and `count(DISTINCT if(..., host, NULL))`.

## Verification

- **Smoke test** (`./tests/smoke/run.sh`): 2,038 tricky real lines, 0 field mismatches against the Python reference. A tab inside a path survives escaping.
- **Full run**, `python -m etl run --pipeline hive --batch-size 500000`:

| | |
| --- | --- |
| Records | 3,461,613 read = 3,461,612 valid + 1 malformed (`alyssa.p`, batch 4) |
| Batches | 7 × 500,000 (average 494,516.14) |
| Runtime | 165 s, about 21 s per batch. The first version took 276 s; reading each batch once with a multi-insert saved 40%. |
| Equivalence | `etl.v_equivalence`: Q1, Q2, Q3 `all_agree = true` with the reference run. Every batch fingerprint matches too. |

## Run it

```bash
docker compose up -d --wait
.venv/bin/python -m etl run --pipeline hive --batch-size 500000
```

Hive runs with a 2 GB heap (`HADOOP_HEAPSIZE` in `docker-compose.yml`), which is enough for 500k-line batches. Keep it at 2 GB so it still fits on 8 GB laptops.
