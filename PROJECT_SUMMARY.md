# Project summary

DAS 839 NoSQL Systems, end-semester project. One ETL and analytics workload over the NASA Kennedy Space Center HTTP logs (July and August 1995, 3,461,613 lines) runs on the pipeline the user picks: **Pig, MapReduce, MongoDB or Hive**. Every pipeline writes the same three query results into PostgreSQL, and a reporting module shows them with the run's execution metadata. Status: **complete** (all four pipelines implemented, verified equivalent, benchmarked).

- **Repository:** https://github.com/sahiti3636/NoSQL_project
- **Final report:** [report/final_report.pdf](report/final_report.pdf) · **Setup:** [SETUP.md](SETUP.md) · **Architecture:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## Requirements and where they are met

| Requirement (project brief) | Where |
| --- | --- |
| User selects exactly one of Pig, MapReduce, MongoDB, Hive | `python -m etl run --pipeline <pig\|mapreduce\|mongodb\|hive>` ([etl/__main__.py](etl/__main__.py)) |
| Only the official raw logs; no preprocessing outside the pipeline | Batcher copies unchanged byte slices ([etl/batcher.py](etl/batcher.py)); every engine parses inside itself |
| Extract host, timestamp, log date, log hour, method, path, protocol, status, bytes; `-` = 0 | Every engine keeps all nine fields ([contracts/README.md](contracts/README.md)); checked field by field |
| Malformed records counted, never dropped | Kept with raw text in each engine and in `etl.malformed_record`; counts checked per batch |
| Same parsing, cleaning and query rules in every pipeline | Shared [contracts/](contracts/) loaded at run time; equivalence proven row by row |
| Q1 daily traffic, Q2 top 20 resources, Q3 hourly errors | One implementation per engine ([docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#how-each-engine-does-it)) |
| Results in PostgreSQL with pipeline, run ID, batch ID, execution time | [sql/schema.sql](sql/schema.sql), loader [etl/loader.py](etl/loader.py) |
| Reporting module: results plus pipeline, query name, runtime, batch ID, batch size, average batch size | `python -m etl report` (terminal and HTML) ([etl/report.py](etl/report.py)) |
| Batches of N records, IDs from 1, short last batch valid; average = records / non-empty batches | [etl/batcher.py](etl/batcher.py), `etl_run.avg_batch_size` generated column |
| Runtime from first input read to final database write | Controller clock ([etl/runner.py](etl/runner.py)) |
| Same batch size across pipelines for fair comparison | `run_benchmarks.sh`; `etl.compare` refuses different batch sizes |
| One coherent tool, backend switchable | One controller, one engine interface ([etl/engine.py](etl/engine.py)) |
| Database schema and load scripts | [sql/schema.sql](sql/schema.sql), [etl/loader.py](etl/loader.py), [sql/compare_runs.sql](sql/compare_runs.sql) |
| Compact report: architecture, parsing, workflow, queries, batching, schema, decisions, four-pipeline comparison | [report/final_report.pdf](report/final_report.pdf) |
| Sample outputs | [docs/sample_outputs/](docs/sample_outputs/) |

## Team

| Member | Phase 1 | Phase 2 |
| --- | --- | --- |
| Sahiti (BT2024163) | MongoDB ingestion, parsing, malformed records, Q1; controller, batcher, loader | MapReduce `ParseJob`, `LogRecord`, Q1; benchmark runs |
| Tushaar (BT2024022) | MongoDB Q2, Q3; equivalence checker, database tests | MapReduce Q2, Q3, whole-run counts, job settings |
| Aman (BT2024123) | Hive ingestion (RegexSerDe), HiveQL parsing, malformed records, Q1 | Pig ingestion, parsing, malformed records, Q1 |
| Prajwal (BT2024245) | Hive Q2, Q3, whole-run results; reporting module | Pig Q2, Q3, whole-run results |

## Key decisions

- **Parsing inside each engine; Python only moves bytes.** The controller cuts byte-exact batches and loads result files; it never reads a field. (MongoDB needs the bytes decoded before insertion, because it stores only valid UTF-8.)
- **Shared contracts, never retyped.** Three regex files, a month map and the output columns are loaded by every engine. Regex classes are spelled out (`[0-9]`, not `\d`) so Java, JavaScript and Python agree; all engines use find semantics.
- **Real-data edge cases drive the rules.** The files are never joined (July ends with a truncated line); byte `0x01` inside 7 request lines forces binary storage (ORC, `BinStorage`, SequenceFiles) and a whole-line reader in Hive; a lone `\r` must not end a line; dates and timestamps are sliced, never parsed with date functions (which shift time zones).
- **Exact whole-run results.** `finalize()` recomputes Q1–Q3 over every parsed row; per-batch distinct hosts and top-20 lists cannot be added up.
- **Same plan for Q2 everywhere:** rank paths first, count distinct hosts only for the 20 winners; ties in raw UTF-8 byte order.
- **Proof, not trust.** A Python answer key, SHA-256 fingerprints of sorted rows, and a row-by-row checker for any two runs.
- **Reproducible environment.** Docker with pinned images and a pinned Pig download; one transaction per batch; failed runs are recorded with their reason and excluded from reports.

## Results (25 Nov 2026, one laptop, one run each)

| Batch size | Batches | MongoDB | Hive | MapReduce | Pig |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 35 | **97.2 s** | 425.1 s | 111.7 s | 261.4 s |
| 500,000 | 7 | 94.8 s | 207.2 s | **65.6 s** | 137.1 s |
| 1,000,000 | 4 | 99.8 s | 171.4 s | **56.6 s** | 128.9 s |
| no batching | 1 | 97.1 s | 149.3 s | **51.2 s** | 114.3 s |

All 16 runs: 3,461,613 records, 1 malformed; every run matches the answer key; every engine is EQUIVALENT to MongoDB at every batch size. Fixed cost per batch: MongoDB ≈ 0 s, MapReduce 1.8 s, Pig 4.3 s, Hive 8.1 s. Details: [docs/BENCHMARKS.md](docs/BENCHMARKS.md), [docs/VALIDATION.md](docs/VALIDATION.md).

## Final-phase changes (after Phase 2 handover)

- **MapReduce:** `RunJobs` runs a batch's jobs in one JVM (500k: 72.9 s → 65.6 s); `log_timestamp` in `LogRecord`.
- **Pig:** one Pig session per batch and 100 ms job polling (500k: 187.8 s → 137.1 s); Q2 top 20 without a sampling job and with a byte-order tie-break (Java UDF); fallback regex only when needed; COPY escaping verified on paths that reach the top 20.
- **MongoDB:** Q2 ranks paths before counting hosts (whole-run Q2 11.2 s → 6.5 s); fallback regex only when needed; `log_timestamp`.
- **Hive:** `log_timestamp` column; the stale-pid start-up failure after a Docker restart is fixed in `docker-compose.yml`.
- **Controller:** `--no-batch` mode; report labels unbatched runs; benchmark script covers all four engines and no batching.
- **Checks:** sample check extended (top-20 escaping, tie at rank 20, every parsed field); `hadoop` health check no longer starts three JVMs every 15 s.
