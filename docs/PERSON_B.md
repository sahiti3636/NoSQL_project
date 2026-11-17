# MongoDB Person B handoff

Baseline: `sahiti3636/NoSQL_project`, commit `7619b07` (MongoDB Person A merged).

This contribution completes MongoDB Q2 and Q3, supplies a SQL equivalence checker and a
Phase 1 architecture diagram, and adds reproducible Docker image pins and regression tests.
Person A's parsing, ingestion, Q1, shared batcher, controller, and loader are retained.

## Files to merge into the team repository

| File | Purpose |
|---|---|
| `etl/engines/mongodb.py` | Completed Q2/Q3; explicit binary collation for query ordering |
| `etl/compare.py` | Read-only comparison CLI and run compatibility checks |
| `sql/compare_runs.sql` | Actual Q1-Q3 row comparisons in PostgreSQL |
| `tests/integration/test_person_b.py` | Real MongoDB/PostgreSQL regression tests |
| `scripts/verify_reference.py` | Check a full run against committed reference JSON |
| `scripts/smoke.ps1` | Windows launcher for the team's smoke test |
| `compose.lock.yml` | Immutable image digests for team reproducibility |
| `.gitattributes` | LF line endings for scripts, regexes, and TSV contracts |
| `SETUP.md` | Updated primary startup command and MongoDB implementation status |
| `docs/ARCHITECTURE.md`, `docs/architecture.svg`, `docs/architecture.png` | Architecture explanation and diagram |
| `docs/VALIDATION.md`, `docs/validation/` | Recorded checks, versions, run IDs, and sample final outputs |

The changes archive contains only these contributions. Overlay it on the stated baseline;
review the MongoDB file before replacing it if your team has made later edits. The complete
source archive is a standalone snapshot of that baseline plus this contribution. Neither archive
contains raw NASA data, virtual environments, credentials, Docker volumes, or `.git` history.

## Team Docker setup

Install Docker Desktop, Python 3, and Git for Windows. Start Docker Desktop in Linux-container
mode. Use the same `compose.lock.yml` on every laptop:

```powershell
docker compose -f docker-compose.yml -f compose.lock.yml up -d --wait --wait-timeout 360
docker compose ps
docker compose exec -T mongodb mongosh --quiet --eval 'db.version()'
```

Expected MongoDB version in this lock: **7.0.48**. PostgreSQL: **15.20**. Hive image: **4.0.1**.
The original tags (`mongo:7`, `postgres:15`) can move; the lock file selects the exact downloaded
image digests. Always include both `-f` options when starting/recreating containers. `exec`,
`ps`, and `stop` may use the ordinary project Compose command.

| Service | Laptop connection |
|---|---|
| PostgreSQL | `localhost:5433`, database `nasa_etl`, user `etl`, password `etl` |
| MongoDB | `mongodb://localhost:27017` (local development, no authentication) |
| Hive JDBC | `jdbc:hive2://localhost:10000/` |
| Hive web UI | `http://localhost:10002` |

The upstream guide recommends 6-8 GB available to Docker. This laptop has 8 GB total RAM and
a 4 GB WSL cap; the recorded validation used that existing cap. Keep hardware, resource limits,
image digests, source data, and batch sizes in your experiment notes when comparing runtimes.
Correctness equivalence is independent of runtime; different laptops are not controlled benchmarks.

## Data and Python on Windows

Run from the repository root in PowerShell. The data commands below are for a fresh checkout;
the working copy prepared on this laptop already contains both verified datasets.

```powershell
New-Item -ItemType Directory -Force data,runs | Out-Null
curl.exe --fail --location --retry 3 -o data/NASA_access_log_Jul95.gz https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
curl.exe --fail --location --retry 3 -o data/NASA_access_log_Aug95.gz https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -c "import gzip,pathlib,shutil; [(shutil.copyfileobj(gzip.open(p,'rb'),open(p.with_suffix(''),'wb'))) for p in pathlib.Path('data').glob('NASA_access_log_*.gz')]"
.\.venv\Scripts\python.exe -c "from pathlib import Path; expected={'Jul95':1891714,'Aug95':1569898}; counts={m:Path('data/NASA_access_log_'+m).read_bytes().count(b'\n') for m in expected}; print(counts); assert counts==expected"
```

July has 1,891,714 newline bytes but **1,891,715 records** because its last line has no newline.
August has 1,569,898 records. Keep these raw files unchanged and uncommitted.

For an older Windows clone with CRLF files, after adding `.gitattributes` restore **only clean,
unmodified** contract regex/TSV and shell-script files from Git to apply LF. Do not discard team
edits. The packaged snapshot already supplies LF for these files.

## Validate and run

PowerShell (no activation or execution-policy change is needed for Python):

```powershell
# Smoke launcher; if PowerShell blocks .ps1 execution, use the Git Bash alternative below.
& .\scripts\smoke.ps1
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m unittest discover -s tests/integration -p test_person_b.py -v
.\.venv\Scripts\python.exe -m etl pipelines
.\.venv\Scripts\python.exe -m etl run --pipeline mongodb --batch-size 500000
```

Git Bash on Windows, from the repository root:

```bash
python3() { .venv/Scripts/python.exe "$@"; }
docker() { MSYS_NO_PATHCONV=1 docker.exe "$@"; }
export -f python3 docker
bash tests/smoke/run.sh
```

On Linux/macOS use `.venv/bin/python` and run `bash tests/smoke/run.sh` directly with Python 3
on PATH. The smoke test must finish with `SMOKE TEST PASSED`.

The full MongoDB run must report **3,461,613 total, 3,461,612 valid, 1 malformed**, with seven
batches at size 500,000. Final query row counts are **Q1: 295, Q2: 20, Q3: 1,369**.
Use the run UUID printed by the command:

```powershell
.\.venv\Scripts\python.exe scripts/verify_reference.py --run-id YOUR_MONGODB_RUN_UUID
```

That command checks actual final PostgreSQL rows against `reference/reference_results.json`,
and verifies their recorded fingerprints. It exits 0 only on a match.

## Compare with the Hive teammate

Both runs must be stored in the same PostgreSQL database. Have the Hive teammate's completed
engine run through the shared controller on this environment with the same official input files
in July/August order and batch size 500,000. Alternatively, agree on one shared PostgreSQL target
via `ETL_PG_DSN` before executing runs; do not compare two unrelated local UUIDs.

```powershell
.\.venv\Scripts\python.exe -m etl.compare --left-run HIVE_RUN_UUID --right-run MONGODB_RUN_UUID
# Optional machine-readable output:
.\.venv\Scripts\python.exe -m etl.compare --left-run HIVE_RUN_UUID --right-run MONGODB_RUN_UUID --json
```

Exit codes: **0 equivalent**, **1 different results**, **2 invalid/incomplete comparison**.
The checker compares every batch and final Q1-Q3 rows, including Q3 error rates within `1e-12`,
plus malformed records. It refuses failed runs, different input identities or batch sizes,
self-comparisons, missing results, and missing execution metadata.

For development only, a reference run can be compared with MongoDB:

```powershell
.\.venv\Scripts\python.exe -m etl run --pipeline reference --batch-size 500000
.\.venv\Scripts\python.exe -m etl.compare --left-run REFERENCE_RUN_UUID --right-run MONGODB_RUN_UUID --allow-reference
```

The reference engine must remain excluded from the demonstration's real pipeline choices and
report views. The Hive engine is now implemented, and the checker reports Hive and MongoDB as
**EQUIVALENT** on the official dataset (see [VALIDATION.md](VALIDATION.md)). The reporting module is
still pending team work.

## Everyday commands

```powershell
docker compose ps                         # inspect health
docker compose logs --tail 50 mongodb     # troubleshoot
docker compose stop                      # stop services and retain databases
docker compose -f docker-compose.yml -f compose.lock.yml up -d --wait
```

Avoid `docker compose down -v` unless deliberately resetting all project database volumes.
