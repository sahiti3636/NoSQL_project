# Running on Windows

Everything runs in Docker, so Windows needs only Docker Desktop (Linux containers, WSL 2), Python 3 and Git for Windows. Give Docker at least 6 GB of memory (8 GB is better) in *Settings → Resources* or in `%UserProfile%\.wslconfig`. Run every command from the repository root in PowerShell.

## 1. Start the services

```powershell
docker compose -f docker-compose.yml -f compose.lock.yml up -d --build --wait --wait-timeout 600
docker compose ps
```

All four services (postgres, mongodb, hive, hadoop) must show **healthy**. The first start downloads the images and builds the `hadoop` container (Pig and MapReduce); later starts take seconds. Low on memory? Start only what one engine needs, for example `... up -d --wait postgres mongodb` or `... up -d --build --wait postgres hadoop`.

| Service | From the laptop |
| --- | --- |
| PostgreSQL | `localhost:5433`, database `nasa_etl`, user `etl`, password `etl` |
| MongoDB | `mongodb://localhost:27017` |
| Hive | `jdbc:hive2://localhost:10000/` (web UI `http://localhost:10002`) |
| Pig, MapReduce | `docker compose exec hadoop bash` |

## 2. Data and Python

```powershell
New-Item -ItemType Directory -Force data,runs | Out-Null
curl.exe --fail --location --retry 3 -o data/NASA_access_log_Jul95.gz https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
curl.exe --fail --location --retry 3 -o data/NASA_access_log_Aug95.gz https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -c "import gzip,pathlib,shutil; [(shutil.copyfileobj(gzip.open(p,'rb'),open(p.with_suffix(''),'wb'))) for p in pathlib.Path('data').glob('NASA_access_log_*.gz')]"
```

Decompressing is the only change allowed to the logs. July has 1,891,715 records (its last line has no newline) and August 1,569,898.

## 3. Check and run

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests
.\.venv\Scripts\python.exe scripts\check_phase2.py
.\.venv\Scripts\python.exe -m etl run --pipeline mongodb --batch-size 500000
.\.venv\Scripts\python.exe -m etl run --pipeline mongodb --no-batch
.\.venv\Scripts\python.exe -m etl report --pipeline mongodb
```

The smoke test is a bash script. From Git Bash:

```bash
python3() { .venv/Scripts/python.exe "$@"; }
docker() { MSYS_NO_PATHCONV=1 docker.exe "$@"; }
export -f python3 docker
bash tests/smoke/run.sh
```

(`scripts\smoke.ps1` starts the same test from PowerShell.)

## Line endings

`.gitattributes` keeps the contract files, scripts and TSV files on LF line endings, so the regexes read the same on every laptop. If a clone made before that file existed shows CRLF contract files, check them out again from Git (do not edit them by hand).

## Everyday commands

```powershell
docker compose ps                          # health of the four services
docker compose logs --tail 50 hive         # troubleshoot one service
docker compose stop                        # stop, keeping all databases
```

`docker compose down -v` deletes every saved run in every database; use it only to reset deliberately.
