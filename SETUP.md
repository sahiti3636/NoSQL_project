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
| PostgreSQL 15 | `localhost:5432`, database `nasa_etl` | user `etl`, password `etl` |
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

## Things that will bite you

- **Port already in use:** another PostgreSQL or MongoDB running on your laptop. Stop it, or pick other ports, for example `PG_PORT=5433 MONGO_PORT=27018 docker compose up -d --wait`.
- **Hive `INSERT OVERWRITE LOCAL DIRECTORY` writes nothing:** Hive's query-results cache swallows the output. Put `SET hive.query.results.cache.enabled=false;` at the top of every Hive script.
- **Hive output folders contain `.crc` files:** the loader must read only `000000_*`-style files and skip names starting with `.`.
- **Reading the raw logs in Hive:** use a one-column table with `RegexSerDe` and `input.regex = '([\\s\\S]*)'`, never the default text format. The default splits fields on byte `\x01`, which appears inside a real request line and would silently cut it.
- **Regex inside a HiveQL string:** every `\` must be doubled. Generate the SQL from `contracts/*.regex` (see `tests/smoke/make_hive_sql.py`) instead of pasting by hand.
- **Hive container is slow or killed:** Docker Desktop needs more memory (step 1).
