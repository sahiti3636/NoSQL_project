# Setup

Everything runs in Docker, so every machine gets the same versions: PostgreSQL 15, MongoDB 7, Hive 4.0.1, and Pig 0.18 with MapReduce on Hadoop 3.3.6. About 20 minutes the first time.

On **Windows**, see [docs/WINDOWS.md](docs/WINDOWS.md) for the PowerShell versions of these commands.

## 1. Install

- **Docker Desktop**, with **at least 6 GB of memory** in *Settings → Resources* (8 GB is better).
- **Python 3** (any recent version).
- **Git Bash or WSL** on Windows, to run the `.sh` scripts.

## 2. Get the code and the data

```bash
git clone https://github.com/sahiti3636/NoSQL_project.git
cd NoSQL_project
mkdir -p data runs
curl -o data/NASA_access_log_Jul95.gz https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
curl -o data/NASA_access_log_Aug95.gz https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
gunzip -k data/*.gz
```

Check the line counts; anything else means the download is incomplete:

```bash
wc -l data/NASA_access_log_Jul95 data/NASA_access_log_Aug95
#  1891714 data/NASA_access_log_Jul95   <- 1,891,715 records: the last line has no newline
#  1569898 data/NASA_access_log_Aug95
```

Decompression is the only change allowed to the logs. `data/` and `runs/` are git-ignored.

## 3. Start the services

```bash
docker compose -f docker-compose.yml -f compose.lock.yml up -d --build --wait --wait-timeout 600
docker compose ps
```

All four services must show **healthy**. The first start downloads about 4.6 GB of images and builds the `hadoop` container (Pig, MapReduce, JDK; about 490 MB more). To start only what one engine needs, name the services, for example `up -d --wait postgres mongodb` or `up -d --build --wait postgres hadoop`.

`compose.lock.yml` pins every image to an exact digest, and `docker/hadoop/Dockerfile` pins its base images and the Pig download, because tags like `mongo:7` move over time. The pins work on amd64 (Windows, Linux) and arm64 (Apple Silicon). Always pass both `-f` files when starting or recreating containers; `ps`, `exec`, `logs` and `stop` work with plain `docker compose`.

| Service | From your laptop | Login |
| --- | --- | --- |
| PostgreSQL 15 | `localhost:5433`, database `nasa_etl` | user `etl`, password `etl` |
| MongoDB 7 | `mongodb://localhost:27017` | none |
| Hive 4.0.1 | `jdbc:hive2://localhost:10000/` (web UI http://localhost:10002) | none |
| Pig 0.18, MapReduce | `docker compose exec hadoop bash` (local mode, no server) | none |

Inside the containers the raw logs are at `/input`, the contracts at `/contracts`, and batch folders under `/runs` (shared with `./runs` on the laptop). PostgreSQL creates the `etl` schema from `sql/schema.sql` on the **first** start only; to reload it after a change:

```bash
docker compose exec -T postgres psql -U etl -d nasa_etl -v ON_ERROR_STOP=1 < sql/schema.sql
```

## 4. Python and checks

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests                 # unit tests, no Docker needed
./tests/smoke/run.sh                                           # SMOKE TEST PASSED
.venv/bin/python scripts/check_phase2.py                       # PHASE 2 SETUP OK
.venv/bin/python scripts/check_phase2_sample.py pig            # SAMPLE CHECK PASSED
.venv/bin/python scripts/check_phase2_sample.py mapreduce      # SAMPLE CHECK PASSED
```

- The **smoke test** parses about 2,000 real log lines, including every odd one (binary bytes, a tab in a path, quotes inside requests, the truncated last line), inside Hive and MongoDB using only `contracts/`, and compares every field with Python.
- **check_phase2.py** runs a real Pig script and a real MapReduce job in the `hadoop` container.
- **check_phase2_sample.py** runs the Pig or MapReduce engine on the same sample plus made-up edge cases and compares every parsed record and every output row with the contract rules.

## 5. Run the ETL

```bash
.venv/bin/python -m etl pipelines                                       # the four engines
.venv/bin/python -m etl run --pipeline mongodb --batch-size 500000      # batched
.venv/bin/python -m etl run --pipeline mongodb --no-batch               # whole input as one batch
```

`--pipeline` is one of `mongodb`, `hive`, `mapreduce`, `pig`. Every run must report 3,461,613 records with 1 malformed. `reference` is a **test-only** Python engine that gives the expected answers; it is hidden from the report views and is not one of the four pipelines.

## 6. Report, compare, verify

```bash
.venv/bin/python -m etl report                                   # latest successful run
.venv/bin/python -m etl report --pipeline pig                    # latest Pig run
.venv/bin/python -m etl report --run-id RUN_ID --rows all        # every Q1/Q3 row
.venv/bin/python -m etl report --html runs/reports/pig.html      # full report as a web page
.venv/bin/python -m etl report --list                            # recent runs
.venv/bin/python -m etl report --compare                         # runtimes and equivalence of all engines
.venv/bin/python -m etl.compare --left-run RUN_A --right-run RUN_B   # row by row, any two engines
.venv/bin/python scripts/verify_reference.py --run-id RUN_ID     # final results vs the answer key
```

The report shows the pipeline, run ID, start and finish time, runtime, batch size (or "no batching"), number of batches, average batch size, per-batch counts and times, per-query times, and the Q1–Q3 results.

## 7. Benchmarks and database tests

```bash
./scripts/run_benchmarks.sh                                       # 3 rounds × {100k, 500k, 1M, none} × 4 engines
ROUNDS=1 SIZES="500000 none" ./scripts/run_benchmarks.sh          # a quicker subset
.venv/bin/python -m unittest discover -s tests/integration -p test_person_b.py -v
.venv/bin/python tests/integration/mongodb_part_a.py              # every MongoDB parsed field vs Python
```

For fair timings, run benchmarks on mains power with Low Power Mode off and nothing else busy: a throttled laptop made every engine about 50% slower.

## 8. Adding an engine

Implement `prepare`, `process_batch`, `finalize` and `cleanup` from [etl/engine.py](etl/engine.py) (`etl/engines/reference.py` is a complete small example) and register the class in `ENGINES`. The controller already cuts the batches, runs the clock, checks the counts, loads PostgreSQL and records success or failure. The engine must parse inside its own system, write `q1.tsv`, `q2.tsv`, `q3.tsv` and `malformed.tsv` in COPY text format, and compute whole-run results over all parsed rows.

## Troubleshooting

- **Port already in use:** another PostgreSQL or MongoDB on the laptop. Stop it, or pick other ports, for example `PG_PORT=5434 MONGO_PORT=27018 docker compose up -d --wait` (and export the same variables before `python -m etl`).
- **Hive is slow or killed:** give Docker more memory (step 1). Hive runs with a 2 GB heap.
- **Hive says "HiveServer2 running as process 7. Stop it first":** fixed in `docker-compose.yml`, which removes the stale pid file at start. With an older checkout, recreate the container: `docker compose -f docker-compose.yml -f compose.lock.yml up -d --wait --force-recreate hive`.
- **A Pig or MapReduce run fails:** the terminal shows the end of the error; the full log is in `runs/<run>/pig_logs/` or `runs/<run>/mr_logs/`, and the generated Pig scripts are in `runs/<run>/pig_scripts/`.
- **Everything is suddenly much slower:** check for Low Power Mode or another process using the CPU (`top -o cpu`).
