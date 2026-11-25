# Pig engine (Aman and Prajwal)

Code: [`etl/engines/pig.py`](../etl/engines/pig.py), plus the Java UDF [`pig/src/etl/pig/Utf8Key.java`](../pig/src/etl/pig/Utf8Key.java) (byte-order key for Q2). Shared rules: [`contracts/`](../contracts/README.md).

| Person | Part |
| --- | --- |
| **Aman** (C) | Raw-line ingestion with `TextLoader`, parsing in Pig Latin with the contract regexes, malformed records, counts, **Q1**, `process_batch()` |
| **Prajwal** (D) | **Q2** top 20 resources, **Q3** hourly errors, `finalize()` for whole-run results and counts, `cleanup()` |

## How a batch flows through Pig

Python only writes Pig Latin scripts and starts them inside the `hadoop` container (Pig 0.18 on Hadoop 3.3.6, local mode). It never reads a log line.

```
runs/<run>/batch_000001/input   (byte-identical pieces of the logs, cut by the controller)
        │  TextLoader; a line ends at \n only (-Dtextinputformat.record.delimiter)
        ▼
raw (line:chararray) → strip one trailing \r → RegexFind(envelope)
        │  LEFT OUTER JOIN months.tsv (replicated) → month number or null
        │  SPLIT good / bad      ONE script; Pig's multi-query: one scan feeds all four STOREs
        ├──► pig_data/parsed/batch_000001      BinStorage, 9 typed columns (kept for finalize)
        ├──► pig_data/malformed/batch_000001   BinStorage, raw line          (kept for finalize)
        ├──► malformed.tsv                     COPY-escaped raw lines
        └──► counts                            read / valid / malformed, counted by Pig
pig_data/parsed/batch_000001 ── Q1, Q2, Q3 scripts ──► q1.tsv  q2.tsv  q3.tsv
finalize(): the same Q1–Q3 scripts over pig_data/parsed/batch_*, plus a counts script
```

The parse script and the three query scripts of a batch run from **one** `pig` call: a small driver script `exec`s each one in turn and prints a clock reading (`sh date`) between them, so each step is timed. Every script is kept in `runs/<run>/pig_scripts/`, every log in `runs/<run>/pig_logs/`.

## Parsing (Aman)

`DEFINE RegexFind REGEX_EXTRACT_ALL('false');` makes Pig use `Matcher.find()` instead of `matches()`, exactly as Hive (`RLIKE`, `regexp_extract`) and the MapReduce `ParseJob` do. The regexes are read from `contracts/` at run time and quoted for Pig by `hadoop_common.quote_string()`.

| Column | Pig Latin |
| --- | --- |
| valid line? | the envelope matches **and** the month joins to a row of `/contracts/months.tsv` |
| `host` | envelope group 1 |
| `log_timestamp` | `CONCAT(log_date, ' ', hour, ':', minute, ':', second, ' ', zone)`: `1995-07-01 00:00:01 -0400` |
| `log_date` | `CONCAT(year, '-', month_number, '-', day)`; slicing, no date functions |
| `log_hour` | `(int)` group 7 |
| `http_method`, `resource_path`, `protocol_version` | request regex groups 1–3. Only when it does not match is the fallback regex run: it gives `resource_path`, and the other two are `''` |
| `status_code` | `(int)` group 12 |
| `bytes` | `'-'` → `0L`, otherwise `(long)` group 13 |

## Queries

- **Q1 (Aman):** `GROUP parsed BY (log_date, status_code)` → `COUNT_STAR`, `SUM(bytes)`.
- **Q2 (Prajwal):** count requests per path; put all path counts in one group and, inside a nested `FOREACH`, `ORDER` by count descending then by the path's UTF-8 byte key and `LIMIT 20`. A replicated (in-memory) join takes those 20 paths back to the parsed rows, so `DISTINCT host` and the byte total are computed only for them. No final `ORDER`: the report ranks Q2 in SQL with the same rule.
- **Q3 (Prajwal):** `GROUP BY (log_date, log_hour)` over **all** rows, so hours with zero errors keep their row. A nested `FILTER` keeps the errors (status 400–599) and `DISTINCT` counts their hosts; `error_rate` = `(double) errors / (double) total`.
- **Whole run (Prajwal):** `finalize()` loads the glob `pig_data/parsed/batch_*`, so results are computed over every row and never added up from batch outputs. The counts script `UNION`s the parsed and malformed folders and sums two flags, so it still gives a row when there are no malformed lines.

## Design choices and why

| Choice | Why |
| --- | --- |
| One `pig` call per batch (driver script with `exec`) | Starting `pig` costs several seconds (JVM, Hadoop, Pig) before any work. One session runs parse, Q1, Q2 and Q3 and still times each script, so every query keeps its own time in `etl.query_execution`. A first version used Pig's embedded Jython as the driver; Jython's own start-up (about 6 s) cancelled the gain, so the driver is plain Pig Latin. |
| `-Dpig.jobcontrol.sleep=100` | Pig checks whether its Hadoop jobs have finished every 5 s by default, so each job sat idle after it ended. 100 ms removes that wait; results are unchanged. |
| Parse script does parse + malformed + counts | Pig's multi-query execution reads the batch once for all four `STORE`s. |
| Top 20 picked in one reducer (`GROUP ALL` + nested `ORDER`/`LIMIT`) | A top-level `ORDER BY` costs Pig an extra sampling job and a sort job; ranking the per-path counts in one reducer needs neither. |
| Q2 ties by `Utf8Key(path)` (Java UDF, hex of the UTF-8 bytes; compiled once per run in `prepare()`) | The contract ranks ties by raw byte order. Pig compares `chararray` as Java Strings (UTF-16 units), which orders a character above U+FFFF before U+E000–U+FFFF. Ordering hex keys as strings is exactly byte order. |
| Fallback regex only when the request regex fails | It ran on every line before; it is needed for a handful of lines. |
| Parsed and malformed rows stored with `BinStorage` | 7 real request lines contain byte `0x01` and a path can contain a tab, so `PigStorage` would split them when read back. `BinStorage` also keeps the column types. |
| Month map joined from `months.tsv` | The contract file is read by Pig itself, not retyped. Its header row never matches a 3-letter month. |
| `_copy_escape()` on the path (Q2) and the raw line (malformed) | `PigStorage` writes fields as they are. `REPLACE` escapes backslash, tab, newline and carriage return into PostgreSQL `COPY` text (Contract 2). |
| Trailing `\r` removed with `REPLACE(line, '\r\z', '')` | `\z` is the true end of the string; `$` could also match before a final line terminator. |

## Verification

`scripts/check_phase2_sample.py pig` runs the engine on the 2,038-line smoke sample plus made-up lines: a path with a tab and a backslash that reaches the top 20, a malformed line with a tab and a backslash, a trailing `\r`, an unknown month, and 21 paths tied for the top 20 whose cut-off depends on byte order. Every parsed record (all nine fields), every count, and every Q1–Q3 and malformed row match the contract rules, for the batch and for `finalize()`. Full-run results: [VALIDATION.md](VALIDATION.md); runtimes: [BENCHMARKS.md](BENCHMARKS.md).

## Run it

```bash
docker compose -f docker-compose.yml -f compose.lock.yml up -d --build --wait postgres hadoop
.venv/bin/python scripts/check_phase2_sample.py pig
.venv/bin/python -m etl run --pipeline pig --batch-size 500000
.venv/bin/python -m etl run --pipeline pig --no-batch
.venv/bin/python -m etl report --pipeline pig
```
