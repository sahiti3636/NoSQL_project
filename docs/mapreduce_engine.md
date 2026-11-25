# MapReduce engine (Sahiti and Tushaar)

Code: [`etl/engines/mapreduce.py`](../etl/engines/mapreduce.py) and the Java jobs in [`mapreduce/src/etl/mr/`](../mapreduce/src/etl/mr/). Shared rules: [`contracts/`](../contracts/README.md).

| Person | Part |
| --- | --- |
| **Sahiti** (A) | `ParseJob` (ingestion, parsing with the contract regexes, malformed records, counts), `LogRecord`, `LongPair`, **Q1** (`Q1Job`), `process_batch()` |
| **Tushaar** (B) | **Q2** (`Q2Job`), **Q3** (`Q3Job`, `Q3Partial`), whole-run counts (`CountJob`), `Jobs` (shared job settings), `finalize()`, `cleanup()` |
| Shared | `RunJobs` (one JVM per batch), `Contracts`, `CopyText` |

## How a batch flows through MapReduce

Python compiles the Java sources once per run (`prepare()`, into `runs/<run>/mapreduce/etl-mr.jar`) and starts the jobs with `hadoop jar` inside the `hadoop` container (Hadoop 3.3.6, local mode). It never reads a log line.

```
runs/<run>/batch_000001/input   (byte-identical pieces of the logs, cut by the controller)
        │  ParseJob (map only, TextInputFormat, a line ends at \n only)
        ├──► mr_data/parsed/batch_000001      LogRecord SequenceFiles (kept for finalize)
        ├──► mr_data/malformed/batch_000001   COPY-escaped raw lines   (kept for finalize)
        ├──► malformed.tsv                    this batch's malformed lines
        └──► counts                           read / valid / malformed, from job counters
mr_data/parsed/batch_000001
        ├── Q1Job ──► q1.tsv   key (date, status)   combiner sums (1, bytes)
        ├── Q2Job ──► q2.tsv   job 1: (count, bytes) per path → top 20 · job 2: distinct hosts of the 20
        └── Q3Job ──► q3.tsv   key (date, hour)     combiner merges (total, errors, error-host set)
finalize(): Q1Job, Q2Job, Q3Job over mr_data/parsed/batch_*, CountJob over parsed + malformed
```

All four jobs of a batch run from **one** `hadoop jar` call: `etl.mr.RunJobs` reads a plan (one line per job), calls each job's `run()` in the same JVM, and writes each job's time to a timings file. Each batch's plan, timings and log are kept in `runs/<run>/mr_logs/`.

## Parsing (Sahiti)

`ParseJob` loads the regexes and month map from `/contracts` (`etl.mr.Contracts`) and uses `Matcher.find()`, as Hive's `RLIKE` and `regexp_extract` do.

| Field of `LogRecord` | Java |
| --- | --- |
| valid line? | the envelope matches **and** `months.get(group 5)` is not null |
| `host` | group 1 |
| `logTimestamp` | `logDate + " " + hour + ":" + minute + ":" + second + " " + zone`: `1995-07-01 00:00:01 -0400` |
| `logDate` | `year + "-" + month number + "-" + day` |
| `logHour` | `Integer.parseInt(group 7)` |
| `httpMethod`, `resourcePath`, `protocolVersion` | request regex groups 1–3; if it does not match, the fallback regex gives the path and the other two are `""` |
| `statusCode` | `Integer.parseInt(group 12)` |
| `bytes` | `"-"` → 0, otherwise `Long.parseLong(group 13)` |

## Q2 and Q3 (Tushaar)

`Q2Job` runs two jobs from one `run()`:

| Job | Map | Combine | Reduce (1 reducer) |
| --- | --- | --- | --- |
| top | `path → (1, bytes)` | sums (reuses `Q1Job.SumCombiner`) | sums per path, keeps a bounded heap of 20, writes the winners to a scratch SequenceFile |
| hosts | only rows whose path is a winner: `path → host` | drops repeated hosts within a map task | counts distinct hosts per winner, writes the 20 `q2.tsv` lines |

- **Rank order:** `request_count` descending, then `resource_path` in UTF-8 byte order. The heap compares paths as Hadoop `Text`, which compares raw bytes like PostgreSQL's C collation; Java's `String.compareTo` would order characters above U+FFFF differently.
- **Winners to job 2:** through the job `Configuration`, each path as Base64 of its UTF-8 bytes, so commas, tabs or any other byte are safe.

`Q3Job` is one job. Its map output value is a `Q3Partial` (total requests, error requests, set of error hosts). Partials merge by adding counts and taking the union of host sets, so the same class serves the map, the combiner and the reducer. Every valid row is mapped, not only errors, so hours with zero errors keep their row.

**Whole run:** `finalize()` runs the same three query jobs on the glob `mr_data/parsed/batch_*`, so results are computed over every parsed row and never added up from batch outputs. `CountJob` is a map-only job with two inputs (`MultipleInputs`): the parsed SequenceFiles and the malformed text files, counted with job counters.

## Design choices and why

| Choice | Why |
| --- | --- |
| One `hadoop jar` call per batch (`RunJobs`) | Starting a JVM and Hadoop costs about a second before a job does any work. One JVM runs parse, Q1, Q2 and Q3, and still times each job, so every query keeps its own time in `etl.query_execution`. |
| Every job starts from `Jobs.conf()`: `mapreduce.client.completion.pollinterval` = 100 ms | The default 5,000 ms made `waitForCompletion` sit idle after each job finished: every batch took about 29 s at any batch size. With 100 ms, the first 500k runs went from 275 s to 146 s with identical results. |
| Combiners on every query | Q1 and Q2 sum early; Q3 merges counts and host sets early. Far less data is shuffled. |
| One reducer for Q2 and Q3 | Q2 needs one global top 20. Q3 has at most 24 × 62 keys. |
| Parsed rows as `LogRecord` SequenceFiles | 7 real request lines contain byte `0x01` and a path can contain a tab, so no text delimiter is safe. |
| Output through `CopyText.row(...)` | PostgreSQL `COPY` text format (Contract 2): backslash, tab, newline and carriage return are escaped. |

## Verification

`scripts/check_phase2_sample.py mapreduce` runs the engine on the 2,038-line smoke sample plus made-up lines (a path with a tab and a backslash in the top 20, a malformed line with a tab and a backslash, a trailing `\r`, an unknown month, and 21 paths tied for the top 20 whose cut-off depends on byte order). Every parsed record (all nine fields), every count, and every Q1–Q3 and malformed row match the contract rules, for the batch and for `finalize()`. Full-run results: [VALIDATION.md](VALIDATION.md); runtimes: [BENCHMARKS.md](BENCHMARKS.md).

## Run it

```bash
docker compose -f docker-compose.yml -f compose.lock.yml up -d --build --wait postgres hadoop
.venv/bin/python scripts/check_phase2_sample.py mapreduce
.venv/bin/python -m etl run --pipeline mapreduce --batch-size 500000
.venv/bin/python -m etl run --pipeline mapreduce --no-batch
.venv/bin/python -m etl report --pipeline mapreduce
```
