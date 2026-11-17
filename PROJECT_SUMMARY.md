# NASA Log ETL: project summary (status as of 17 Nov 2026)

DAS 839 NoSQL end-semester project. One ETL over the NASA HTTP logs (July and August 1995) runs on four engines: Pig, MapReduce, MongoDB and Hive. Every engine writes the same three query results into one relational database, and a report shows the results and the runtimes.

- **Repository:** https://github.com/sahiti3636/NoSQL_project
- **Shared spec (all decisions in detail):** https://claude.ai/artifact/RXNjfcm1MjC4iS6Zg448EM
- **Setup guide:** [SETUP.md](SETUP.md)

---

## 1. What the project asks for

| Part | Requirement |
| --- | --- |
| Input | `NASA_access_log_Jul95` and `NASA_access_log_Aug95`, unchanged. The raw files may not be edited. |
| Parsing | Inside each engine. Count malformed lines. |
| Q1, daily traffic | Per `log_date` and `status_code`: request count and total bytes. |
| Q2, top resources | Top 20 paths by request count, with total bytes and distinct hosts. |
| Q3, hourly errors | Per date and hour: error count (status 400–599), total count, error rate and distinct error hosts. |
| Batching | The user picks a batch size. Report batch IDs, batch count and average batch size. |
| Runtime | Measured and reported per run. |
| Output | Results loaded into a relational database, with a reporting module. |
| Phases | **Phase 1:** MongoDB and Hive. **Phase 2:** Pig and MapReduce. |

---

## 2. Team split (Phase 1)

| Person | Engine work | Shared work | Status |
| --- | --- | --- | --- |
| **A** | MongoDB: raw ingestion, parsing inside MongoDB, malformed records, Q1 | Contracts, Docker, controller (batcher, run manager, loader, CLI), test-only reference engine | **Done, merged (PR #1)** |
| **B** | MongoDB: Q2 and Q3 | Architecture diagram, equivalence check script, database tests | **Done, not merged yet** |
| **C** | Hive: table over raw logs, parsing, malformed records, Q1 | PostgreSQL schema (done by A, so C reviews it and owns explaining it) | **Engine done, not merged yet** |
| **D** | Hive: Q2 and Q3, per-batch output | Report script | **Engine done, not merged yet** · report to do |

Each team has one person on setup, parsing and Q1, and one person on the two harder queries (distinct counts, top 20 and rates). Each person also owns one shared part.

**Phase 1 presentation**

| Topic | Speaker |
| --- | --- |
| Architecture | B |
| Parsing strategy | C (Hive) and A (MongoDB) |
| ETL workflow and batching | A |
| Relational schema | C |
| Equivalence check and results | B |
| Live demo: Hive | D |
| Live demo: MongoDB | A |

---

## 3. Key decisions and why

### Parsing (Contract 1)
- **Same regexes for every engine**, stored as files in `contracts/` and loaded by each engine. They are not copied by hand. Character classes are spelled out (no `\d`, `\s`) so every regex dialect reads them the same way.
- **Envelope regex** for the line, then a **request regex** for method / path / protocol, then a **fallback** that takes the whole request as the path when the request has no method.
- **Missing parts are `''`, never NULL.** Bytes `-` becomes `0`. All byte counts are 64-bit.
- **Months come from `months.tsv`.** An unknown month name makes the line malformed.
- **No date functions.** MongoDB's `$dateFromString` converts to UTC and shifts hours by 4. `log_date` and `log_hour` come straight from the text.
- **Bytes are decoded as UTF-8, and invalid bytes become U+FFFD.** MongoDB and Hadoop both need valid UTF-8.
- **Each file is split into lines separately and never concatenated.** The July file ends with the truncated line `alyssa.p` and has no final newline. Joining the files would merge that line into the first August line and hide the only malformed record.

### Outputs and equivalence (Contract 2)
- Each engine writes `q1.tsv`, `q2.tsv`, `q3.tsv` and `malformed.tsv` in PostgreSQL `COPY` text format.
- **Q2 ties break by path in byte order** (C collation), so every engine returns the same top 20.
- **Q3 includes hours with zero errors.**
- **Fingerprints:** a SHA-256 hash over the sorted rows of each result. Two engines agree when their hashes are equal. Q3 leaves `error_rate` out of the hash, because floating-point rounding can differ between engines.

### Database (Contract 3)
- **PostgreSQL 15, not MySQL.** It offers `UNIQUE NULLS NOT DISTINCT`, generated columns, `CHECK` constraints and fast `COPY` bulk loading.
- One schema `etl` with these tables: `etl_run`, `etl_batch`, `query_def`, `query_execution`, `q1_daily_traffic`, `q2_top_resources`, `q3_hourly_errors` and `malformed_record`.
- **Results are stored per batch and for the whole run.** `batch_id = NULL` means the whole-run result.
- `CHECK` constraints enforce integrity: valid + malformed = read. `avg_batch_size` is a generated column.
- Report views: `v_run_report`, `v_batch_report`, `v_q1_final`, `v_q2_final`, `v_q3_final`, `v_run_integrity`, `v_equivalence` and `v_pipeline_comparison`.

### Batching and runtime (Contract 4)
- A batch is exactly N lines, copied as byte-identical pieces into `runs/<run_id>/batch_000001/input/`. Batches cross the July/August boundary, so only the last batch is short.
- Every engine implements four methods: `prepare`, `process_batch`, `finalize` and `cleanup`.
- **The runtime runs from the first byte read until the final database commit.**
- The run fails if a batch's counts don't add up.
- **Batch sizes to compare:** 100k, 500k and 1M lines (about 35, 7 and 4 batches).

### Environment
- **Docker gives everyone identical software:** PostgreSQL 15 on port 5433, MongoDB 7 on port 27017, Hive 4.0.1 on ports 10000 and 10002. Installing Hive by hand needs Hadoop, the right Java version and a metastore all configured to match, and that is very hard on Windows.
- **Docker does not make runtimes equal across laptops.** All timed benchmark runs happen on **one machine**, with the same memory limit and batch sizes.
- PostgreSQL uses port 5433 on the laptop because a local PostgreSQL often holds 5432.

---

## 4. What the data check found

We checked the real files against the expected answers in `reference/`.

| Fact | Value |
| --- | --- |
| Total lines | 3,461,613 (July 1,891,715 + August 1,569,898) |
| Malformed lines | 1: the truncated last July line, `alyssa.p` |
| Lines with non-ASCII bytes | 22 |
| Lines with a tab | 1 |
| Lines with VT/FF control characters | 6 |
| Requests with a quote inside | 32 |
| Total bytes | 65,524,314,915. This needs 64-bit integers. |
| Bytes for the busiest path | 3,196,457,118. This is over the 32-bit limit. |
| Gaps in the data | 28 Jul 14:00 to 1 Aug, and 1 Aug 15:00 to 3 Aug 04:00 |
| Hours with zero errors | 19 |

---

## 5. Done

**Contracts and database** (`contracts/`, `sql/schema.sql`, `reference/`)
- Regexes, months, output column definitions and batch JSON schemas.
- Full PostgreSQL schema with views, and reference answers.

**Docker environment** (`docker-compose.yml`, `SETUP.md`)
- Three services, each with a health check, and shared mounts: `./data` → `/input`, `./contracts` → `/contracts`, `./runs` → `/runs`.
- Smoke test `tests/smoke/run.sh`: Hive and MongoDB parse 2,038 tricky real lines with **0 mismatches** against the reference.

**Controller** (`etl/`)
- CLI: `python -m etl run --pipeline X --batch-size N`.
- Batcher, run manager with integrity checks, and a PostgreSQL loader using COPY, with fingerprints and one transaction per batch.
- Test-only reference engine. It runs in about 16 s and matches the reference exactly. Batch sizes 500k and 1M give identical fingerprints. An engine that drops lines is rejected.
- Unit tests for the batcher: 6 tests, all passing.

**MongoDB Part A** (`etl/engines/mongodb.py`)
- Python only moves bytes. Raw lines are inserted unparsed, and parsing runs **inside MongoDB** as one aggregation with `$regexFind` and `$merge`.
- Malformed records and Q1 are done.
- Tested on all 3,461,613 lines: **0 field mismatches**, and the final Q1 equals the reference. A full run takes about 112 s.

---

## 6. Left for Phase 1

- [x] **B:** `_q2_pipeline()` and `_q3_pipeline()` in `etl/engines/mongodb.py`. Full MongoDB run: 106 s, all queries agree with the reference.
- [x] **B:** equivalence checker `python -m etl.compare` (row by row in PostgreSQL) and `scripts/verify_reference.py`. **Hive vs MongoDB: EQUIVALENT** on every batch and the final output.
- [x] **B:** edge cases covered by `tests/integration/test_person_b.py` (16 database tests) instead of a separate golden file.
- [x] **C and D:** Hive engine in `etl/engines/hive.py`: full run succeeds and Q1–Q3 agree with the reference (see [docs/hive_engine.md](docs/hive_engine.md)). Still to merge.
- [ ] **D:** report script that reads the database views.
- [x] **B:** architecture diagram, `docs/architecture.png` and `docs/ARCHITECTURE.md`.
- [ ] **All:** benchmark runs at 100k, 500k and 1M on one machine.
- [ ] **All:** prepare and rehearse the Phase 1 presentation and demo.

**Phase 2 (later):** Pig and MapReduce engines, using the same contracts, controller and database.

---

## 7. Notes for teammates

- **Low-RAM laptops (8 GB):** start only what you need, for example `docker compose up -d --wait postgres mongodb`. On Windows, set Docker's memory limit in `%UserProfile%\.wslconfig` (`[wsl2]` then `memory=4GB`), then run `wsl --shutdown`.
- **Windows:** use Git Bash, and use `.venv/Scripts/python` instead of `.venv/bin/python`.
- **MongoDB only:** skip `tests/smoke/run.sh` because it needs Hive. Check your setup with the reference run and `tests/integration/mongodb_part_a.py`.
- **Hive:** every script needs `SET hive.query.results.cache.enabled=false;`. Read raw lines with `RegexSerDe`, never the default text format. Skip `.crc` files in output folders.
- **Git:** create a branch for your part, open a pull request, and ask for a review before merging into `main`.
