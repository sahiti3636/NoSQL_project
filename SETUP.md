# Setup (Phase 1: PostgreSQL + MongoDB + Hive)

Everything runs in Docker, so every team member gets the same versions. About 15 minutes the first time.

## 1. Install

- **Docker Desktop**, then in *Settings → Resources* give it **at least 6 GB of memory** (8 GB is better).
- **Python 3** (any recent version; the smoke test uses only the standard library).
- **Git Bash or WSL** if you are on Windows, to run the `.sh` scripts.

## 2. Get the code and the data

```bash
git clone https://github.com/sahiti3636/NoSQL_project.git
cd NoSQL_project
mkdir -p data runs
curl -o data/NASA_access_log_Jul95.gz https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
curl -o data/NASA_access_log_Aug95.gz https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
gunzip -k data/*.gz
```

Check the line counts. They must be exactly these, or your download is incomplete:

```bash
wc -l data/NASA_access_log_Jul95 data/NASA_access_log_Aug95
#  1891714 data/NASA_access_log_Jul95   <- 1,891,715 records: the last line has no newline
#  1569898 data/NASA_access_log_Aug95
```

`data/` and `runs/` are in `.gitignore`. Never commit the logs, and never edit them: the PDF forbids any change to the raw files.

## 3. Start the services

```bash
docker compose up -d --wait
docker compose ps
```

All three must show **healthy**. The first start downloads about 4.6 GB of images.

| Service | Connect from your laptop | Login |
| --- | --- | --- |
| PostgreSQL 15 | `localhost:5433`, database `nasa_etl` | user `etl`, password `etl` |
| MongoDB 7 | `mongodb://localhost:27017` | none |
| Hive 4.0.1 | `jdbc:hive2://localhost:10000/` (web UI: http://localhost:10002) | none |

Inside the containers, the raw logs are at `/input`, the contract files at `/contracts`, and Hive reads and writes batch folders under `/runs` (shared with `./runs` on your laptop).

PostgreSQL creates the `etl` schema from `sql/schema.sql` automatically on the **first** start only. After changing the schema, either reload it by hand:

```bash
docker compose exec -T postgres psql -U etl -d nasa_etl -v ON_ERROR_STOP=1 < sql/schema.sql
```

or wipe everything and start fresh with `docker compose down -v` and then `docker compose up -d --wait`.

## 4. Check your setup

```bash
./tests/smoke/run.sh
```

It must end with `SMOKE TEST PASSED`. It builds a sample of about 2,000 real log lines, including every odd one (binary bytes, a tab in a path, quotes inside requests, the truncated last line). It then parses the sample inside Hive and inside MongoDB using only `contracts/`, and checks both against the Python reference field by field.

## 5. Python environment for the controller

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
```

## 6. Run the ETL

```bash
.venv/bin/python -m etl pipelines
.venv/bin/python -m etl run --pipeline reference --batch-size 500000
```

`reference` is a **test-only** pure-Python engine. It exists to test the controller, loader and database, and to give every real engine a known-good answer. It is hidden from the report views and must never be shown as a pipeline in the demo. A full run takes about 16 seconds and must report 3,461,613 records, 1 malformed, and final results equal to `reference/reference_results.json`.

`hive` is complete. A full run takes about 4–5 minutes, and how it works is described in [docs/hive_engine.md](docs/hive_engine.md). `mongodb` fails with `NotImplementedError` until Q2 and Q3 are filled in in `etl/engines/mongodb.py`.

## 7. Writing an engine (Hive and MongoDB teams)

Read `etl/engine.py` (the interface, about 100 lines) and `etl/engines/reference.py` (a complete working example). Then fill in your file in `etl/engines/`. The controller already does everything else:

| Already done by the controller | Your engine does |
| --- | --- |
| Cuts batches of exactly N lines into `runs/<run_id>/batch_000001/input/` (unchanged byte pieces) | Parse every line **inside Hive/MongoDB** |
| Starts and stops the runtime clock | Write `q1.tsv`, `q2.tsv`, `q3.tsv`, `malformed.tsv` to `spec.output_dir` |
| Checks `records_read = record_count` and `valid + malformed = read`; fails the run if not | Keep parsed rows so `finalize()` computes exact whole-run Q1-Q3 |
| Bulk-loads every file into PostgreSQL, with fingerprints, in one transaction per batch | Return a `BatchResult` with the counts and per-step times |
| Marks the run `succeeded` or `failed` with the reason | Clean up its own tables/collections in `cleanup()` |

Done means: `python -m etl run --pipeline <yours> --batch-size 500000` succeeds, and its final fingerprints in `etl.query_execution` equal the reference run's (`etl.v_equivalence` shows `all_agree = true` for the same batch size).

## Things that will bite you

- **Port already in use:** another PostgreSQL or MongoDB running on your laptop. Stop it, or pick other ports, for example `PG_PORT=5434 MONGO_PORT=27018 docker compose up -d --wait` (then export the same variables before running `python -m etl`). PostgreSQL already uses 5433 by default because a local PostgreSQL usually holds 5432.
- **Hive `INSERT OVERWRITE LOCAL DIRECTORY` writes nothing:** Hive's query-results cache swallows the output. Put `SET hive.query.results.cache.enabled=false;` at the top of every Hive script.
- **Hive output folders contain `.crc` files:** the loader must read only `000000_*`-style files and skip names starting with `.`.
- **Reading the raw logs in Hive:** use a one-column table with `RegexSerDe` and `input.regex = '([\\s\\S]*)'`, never the default text format. The default splits fields on byte `\x01`, which appears inside a real request line and would silently cut it.
- **Regex inside a HiveQL string:** every `\` must be doubled. Generate the SQL from `contracts/*.regex` (see `tests/smoke/make_hive_sql.py`) instead of pasting by hand.
- **Hive container is slow or killed:** Docker Desktop needs more memory (step 1).
