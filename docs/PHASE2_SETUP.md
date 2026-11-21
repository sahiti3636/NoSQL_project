# Phase 2 setup: Pig and MapReduce

The shared setup for Phase 2 is done. Each person only writes their own part of an engine; the
controller, batcher, loader, PostgreSQL schema, report and equivalence checker already handle
`pig` and `mapreduce`.

| Engine | Person | Part | File(s) to write |
| --- | --- | --- | --- |
| MapReduce | Sahiti (A) | ingestion, parsing, malformed records, Q1 | `mapreduce/src/etl/mr/ParseJob.java`, `Q1Job.java`; `process_batch` in [etl/engines/mapreduce.py](../etl/engines/mapreduce.py) |
| MapReduce | Tushaar (B) | Q2, Q3, whole-run results, cleanup | `Q2*.java`, `Q3*.java`; `finalize` in [etl/engines/mapreduce.py](../etl/engines/mapreduce.py) |
| Pig | Aman (C) | ingestion, parsing, malformed records, Q1 | `process_batch` in [etl/engines/pig.py](../etl/engines/pig.py) |
| Pig | Prajwal (D) | Q2, Q3, whole-run results, cleanup | `finalize` in [etl/engines/pig.py](../etl/engines/pig.py) |

Each file's docstring lists the data flow, what is already done, and the contract rules that are
easy to get wrong. The Hive engine ([etl/engines/hive.py](../etl/engines/hive.py)) is the closest
working example: same idea of Python writing scripts that the engine runs.

## What is already set up

- **`hadoop` container** ([docker/hadoop/Dockerfile](../docker/hadoop/Dockerfile)): Hadoop 3.3.6 (from the pinned Hive image), a full JDK 8 for `javac`, and Pig 0.18.0 (the first Pig release built for Hadoop 3). Both engines run in Hadoop **local mode**. Every base image and download is pinned, so every laptop builds the same thing.
- **Mounts**, the same as Hive's: `./runs` → `/runs`, `./contracts` → `/contracts`, `./data` → `/input`, plus `./mapreduce` → `/mapreduce` for the Java sources.
- **Engines registered**: `python -m etl run --pipeline pig|mapreduce` works end to end. Until the parts are written, a run stops at the first batch with "Aman's part …", is marked failed, and is cleaned up.
- **Shared plumbing** in [etl/engines/hadoop_common.py](../etl/engines/hadoop_common.py): run a command in the container with timing and a saved log, Pig string quoting for the contract regexes, a counts reader, and output-folder cleanup.
- **Pig**: `prepare()` creates the folders and the contract regexes as Pig literals (`self.env`, `self.req`, `self.fb`); `self._run(script, label)` saves and runs a script. Scripts are kept in `runs/<run>/pig_scripts`, logs in `runs/<run>/pig_logs`.
- **MapReduce**: `prepare()` compiles everything in `mapreduce/src` into `runs/<run>/mapreduce/etl-mr.jar`; `self._job('etl.mr.X', args..., label=...)` runs one job. Java helpers: `Contracts` (loads the regexes and month map from `/contracts`), `CopyText` (writes PostgreSQL COPY lines), and `LineCount` (a small working job to copy from).
- **Equivalence checker**: `python -m etl.compare` accepts any two of the four pipelines.
- **Checks**: `scripts/check_phase2.py` (real Pig + MapReduce run in the container) and `tests/test_phase2_setup.py`.

## Start it

```bash
git pull
docker compose -f docker-compose.yml -f compose.lock.yml up -d --build --wait --wait-timeout 600
```

The first build downloads Pig (about 490 MB) and takes several minutes; after that it is cached.
Low on memory? Start only what you need, for example `up -d --build --wait postgres hadoop`.

Then check the setup:

```bash
.venv/bin/python -m unittest discover -s tests
.venv/bin/python scripts/check_phase2.py
```

`check_phase2.py` must end with `PHASE 2 SETUP OK`.

## While you build your part

```bash
# open a shell in the container (pig, hadoop, javac are on the PATH)
docker compose exec hadoop bash

# try Pig Latin interactively on a batch folder
docker compose exec hadoop pig -x local

# a full run with your engine (start with the smallest number of batches)
.venv/bin/python -m etl run --pipeline pig --batch-size 1000000
.venv/bin/python -m etl report --pipeline pig
```

- Read the error in the terminal first; the full Pig or Hadoop log is in `runs/<run>/pig_logs` or `runs/<run>/mr_logs`, and the scripts Pig ran are in `runs/<run>/pig_scripts`.
- Use `--keep-files` on `etl run` to keep the batch and output folders for inspection.
- Pig and MapReduce refuse to write into an existing output folder: call `hc.clear_output(path)` first.

## Done means

1. `python -m etl run --pipeline <engine> --batch-size 500000` succeeds: 3,461,613 records, 1 malformed.
2. `python -m etl.compare --left-run <your run> --right-run <a MongoDB or Hive run at 500k>` prints **EQUIVALENT**.
3. `python scripts/verify_reference.py --run-id <your run>` passes.
4. `python -m etl report --compare` shows one distinct answer per query, with all four pipelines.

Benchmarks for all four engines: `PIPELINES="mongodb hive pig mapreduce" ./scripts/run_benchmarks.sh`.
