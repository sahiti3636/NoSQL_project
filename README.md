# NASA Log ETL: one workload, many engines

A multi-pipeline ETL and reporting tool for the NASA Kennedy Space Center web server logs (July and August 1995, 3,461,613 lines). The user picks one engine and a batch size. The tool parses every log line **inside that engine**, runs three fixed queries, loads the results into PostgreSQL, and reports them together with the run's runtime and batch statistics.

DAS 839 NoSQL Systems, end-semester project.

| Phase | Engines | Status |
| --- | --- | --- |
| 1 | **MongoDB**, **Hive** | Done: both engines give identical results on every batch and the whole run |
| 2 | Pig, MapReduce | Planned, on the same contracts and controller |

![Phase 1 architecture](docs/architecture.png)

## The three queries

| Query | Output |
| --- | --- |
| **Q1** Daily traffic | Per date and status code: requests, total bytes |
| **Q2** Top resources | Top 20 paths by requests: requests, total bytes, distinct hosts |
| **Q3** Hourly errors | Per date and hour: error requests (400–599), total requests, error rate, distinct error hosts |

## How it works

- **The controller** (`etl/`) cuts the input into batches of exactly N lines. The batches are unchanged byte slices of the logs, so this is not preprocessing. The controller also times the run, from the first byte read to the final database commit, and checks that every batch's counts add up.
- **The engines** (`etl/engines/`) do all parsing and aggregation. Python never reads a log line:
  - **MongoDB:** `$regexFind` parsing and `$group` queries
  - **Hive:** `RegexSerDe` ingestion, HiveQL parsing, ORC tables
- **Contracts** (`contracts/`) are the shared rules every engine loads: the parsing regexes, the month map and the output columns. This is what keeps the engines equivalent.
- **PostgreSQL** (`sql/schema.sql`) stores each batch's results and the whole-run results, with run, batch and query metadata. Constraints reject inconsistent data.
- **The report** (`python -m etl report`) shows a run's results with its execution metadata.
- **The equivalence checker** (`python -m etl.compare`) compares two runs row by row.

## Results

All runs: 3,461,613 records, 1 malformed line (the truncated last line of the July file).

| Batch size | Batches | MongoDB | Hive |
| ---: | ---: | ---: | ---: |
| 100,000 | 35 | 104.7 s | 437.0 s |
| 500,000 | 7 | 106.3 s | 170.5 s |
| 1,000,000 | 4 | 105.5 s | 142.7 s |

Each figure is the median of 3 runs on an Apple M3 Pro with 8 GB for Docker. Both engines spend about the same time per line (~26.5 µs), but Hive pays a fixed cost of about 9 s per batch, so it gets much faster with bigger batches. Details: [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

**Equivalence:** Hive and MongoDB agree on all 614 Q1, 160 Q2 and 2,744 Q3 rows across every batch and the whole run, and all 18 benchmark runs give one fingerprint per query. Details: [docs/VALIDATION.md](docs/VALIDATION.md).

## Quick start

You need Docker Desktop (6 GB+ memory) and Python 3. Full instructions, including Windows, are in [SETUP.md](SETUP.md).

1. Get the code and the two log files, and unpack them. Never edit the logs.
   ```bash
   git clone https://github.com/sahiti3636/NoSQL_project.git
   cd NoSQL_project && mkdir -p data runs
   curl -o data/NASA_access_log_Jul95.gz https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
   curl -o data/NASA_access_log_Aug95.gz https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
   gunzip -k data/*.gz
   ```
2. Start PostgreSQL, MongoDB and Hive, with image versions pinned:
   ```bash
   docker compose -f docker-compose.yml -f compose.lock.yml up -d --wait --wait-timeout 360
   ```
3. Set up Python and run the checks:
   ```bash
   python3 -m venv .venv
   .venv/bin/python -m pip install -r requirements.txt
   .venv/bin/python -m unittest discover -s tests
   ./tests/smoke/run.sh
   ```
   The unit tests must pass, and the smoke test must print `SMOKE TEST PASSED`.
4. Run a pipeline and see the report:
   ```bash
   .venv/bin/python -m etl run --pipeline mongodb --batch-size 500000
   .venv/bin/python -m etl run --pipeline hive --batch-size 500000
   .venv/bin/python -m etl report --pipeline hive
   ```

## Commands

| Command | What it does |
| --- | --- |
| `python -m etl pipelines` | List the engines |
| `python -m etl run --pipeline <mongodb\|hive> --batch-size <N>` | Run the full ETL with one engine |
| `python -m etl report [--run-id ID \| --pipeline P]` | Show a run's results and metadata (latest successful run by default) |
| `python -m etl report --html FILE` | Write the full report, every row, as a web page |
| `python -m etl report --list` | List recent runs |
| `python -m etl report --compare` | Show runtimes and equivalence across engines |
| `python -m etl.compare --left-run A --right-run B` | Row-by-row check that two runs agree |
| `python scripts/verify_reference.py --run-id ID` | Check a run against the reference answers |
| `./scripts/run_benchmarks.sh` | Benchmark both engines at 100k, 500k and 1M (about 1 hour) |

## Repository layout

```
contracts/      shared parsing regexes, month map, output columns, batch schemas
etl/            controller, batcher, loader, report, equivalence checker
etl/engines/    mongodb.py, hive.py, reference.py (test-only Python engine)
sql/            PostgreSQL schema and report views; row-by-row comparison query
reference/      Python reference implementation and expected results
tests/          unit tests, smoke test (2,038 tricky lines), database tests
scripts/        benchmark runner, reference check, Windows smoke-test launcher
docs/           architecture, Hive engine, benchmarks, validation evidence
report/         Phase 1 report (IEEE format, LaTeX + PDF)
```

## Documentation

- [Phase 1 report (PDF)](report/phase1_report.pdf)
- [Architecture](docs/ARCHITECTURE.md) · [Hive engine](docs/hive_engine.md) · [Benchmarks](docs/BENCHMARKS.md) · [Validation](docs/VALIDATION.md)
- [Setup guide](SETUP.md) · [Windows guide](docs/PERSON_B.md) · [Project summary](PROJECT_SUMMARY.md)

## Team

| Member | Phase 1 work |
| --- | --- |
| Sahiti (BT2024163) | MongoDB ingestion, parsing, malformed records, Q1; controller, batcher, loader |
| Tushaar (BT2024022) | MongoDB Q2 and Q3; architecture, equivalence checker, database tests |
| Aman (BT2024123) | Hive ingestion, parsing, malformed records, Q1; PostgreSQL schema |
| Prajwal (BT2024245) | Hive Q2, Q3 and whole-run results; reporting module |
