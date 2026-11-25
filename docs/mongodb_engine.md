# MongoDB engine (Sahiti and Tushaar)

Code: [`etl/engines/mongodb.py`](../etl/engines/mongodb.py). Shared rules: [`contracts/`](../contracts/README.md).

| Person | Part |
| --- | --- |
| **Sahiti** (A) | Raw-line ingestion, parsing inside MongoDB with the contract regexes, malformed records, counts, **Q1**, `prepare()` and `cleanup()` |
| **Tushaar** (B) | **Q2** top 20 resources, **Q3** hourly errors, whole-run results |

## How a batch flows through MongoDB

```
runs/<run>/batch_000001/input   (byte-identical pieces of the logs, cut by the controller)
        │  Python reads the bytes, splits on \n, strips one trailing \r, decodes UTF-8
        ▼  (invalid bytes → U+FFFD, as Hadoop does), inserts each line UNPARSED
raw_<run>       {b: batch_id, n: line_no, raw: "..."}
        │  ONE aggregation: $regexFind (envelope) → month index → $regexFind (request,
        ▼  fallback only when needed) → typed fields → $merge
parsed_<run>    valid lines: 9 parsed fields;  malformed lines: ok=false + raw text
        ├── $group ──► q1.tsv  q2.tsv  q3.tsv   (this batch: {b: N, ok: true})
        └── find ok=false ──► malformed.tsv
finalize(): the same queries over {ok: true}, i.e. every batch → runs/<run>/final/output/
```

A batch's raw documents are deleted as soon as they are parsed, so MongoDB never holds two copies of the data. `parsed_<run>` keeps every batch for `finalize()`. Both collections belong to one run and are dropped by `cleanup()`.

## Parsing (Sahiti)

Python only moves bytes: MongoDB stores only valid UTF-8, so the bytes are decoded before insertion, exactly as Hadoop's `Text` does for Hive, Pig and MapReduce. Every field is then computed by the aggregation pipeline `_parse_pipeline()`:

| Field | Aggregation |
| --- | --- |
| valid line? | `$regexFind` with the envelope regex matches **and** the month name is in the month list from `months.tsv` (`$indexOfArray` ≥ 0) |
| `host` | capture 1 |
| `log_timestamp` | `$concat` of year, month number, day, hour, minute, second, zone: `1995-07-01 00:00:01 -0400` |
| `log_date` | `$concat(year, '-', month number, '-', day)` |
| `log_hour` | `$toInt` of capture 7 |
| `http_method`, `resource_path`, `protocol_version` | request regex captures 1–3; if it does not match, the fallback regex gives the path and the other two are `''` |
| `status_code` | `$toInt` of capture 12 |
| `bytes_transferred` | `'-'` → 0, otherwise `$toLong` (64-bit) |

`$dateFromString` is never used: it converts to UTC and would move every hour by 4.

## Queries

All query aggregations use `allowDiskUse` and the **simple collation** (binary string comparison), so ties sort by raw bytes like PostgreSQL's `COLLATE "C"`.

- **Q1 (Sahiti):** `$group` by `(log_date, status_code)` → request count and byte total.
- **Q2 (Tushaar):** two aggregations, both inside MongoDB:
  1. `$group` by path (request count, bytes) → `$sort` by count descending, then path → `$limit 20`;
  2. `$match` the rows of those 20 paths only → `$group` by `(path, host)` → `$group` by path to count distinct hosts. Each winner's count and bytes are passed in as literal arrays and attached in the same aggregation.

  Counting distinct hosts only for the 20 winners avoids building a group for every (path, host) pair in the data; the earlier version did that and then discarded all but 20 paths.
- **Q3 (Tushaar):** `$group` by `(date, hour, host)` counting all requests and errors (status 400–599), then `$group` by `(date, hour)`: totals, errors, and hosts with at least one error. Successful requests stay in, so hours with zero errors keep their row; `error_rate` = errors / total.
- **Whole run:** `finalize()` runs the same queries over every parsed row (`{ok: true}`), never adding up batch outputs; the counts come from a `$group` on `ok`.

## Design choices and why

| Choice | Why |
| --- | --- |
| Lines inserted unparsed, parsed by an aggregation | All parsing happens inside MongoDB; Python never reads a field. |
| One parse aggregation with `$merge` | The batch is scanned once; results stay on the server. |
| Index `(b, ok)` on the parsed collection | Each batch's queries read only that batch's valid rows. |
| Two-step Q2 | Top 20 first, distinct hosts for 20 paths only: the same plan as Hive, MapReduce and Pig. |
| Two-stage `$group` in Q3 | Exact distinct error hosts without building large host arrays. |

## Verification

See [VALIDATION.md](VALIDATION.md) for the full-run results. `tests/integration/mongodb_part_a.py` parses the whole dataset and compares all 3,461,613 parsed documents, field by field (timestamp included), with the Python reference: 0 mismatches. `tests/integration/test_person_b.py` holds 16 database tests for Q2, Q3 and the equivalence checker.

## Run it

```bash
docker compose -f docker-compose.yml -f compose.lock.yml up -d --wait postgres mongodb
.venv/bin/python -m etl run --pipeline mongodb --batch-size 500000
.venv/bin/python -m etl run --pipeline mongodb --no-batch
.venv/bin/python -m etl report --pipeline mongodb
```
