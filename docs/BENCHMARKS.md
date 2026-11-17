# Phase 1 benchmarks: MongoDB vs Hive

![Full-run runtime by batch size](benchmark_runtime.png)

## Setup

| | |
| --- | --- |
| Machine | Apple M3 Pro (11 cores), 18 GB RAM, macOS 15.7 |
| Docker | 29.5, 11 CPUs and 8 GB memory for all containers |
| Images | MongoDB 7.0.48, PostgreSQL 15.20, Hive 4.0.1 (pinned by `compose.lock.yml`), Hive heap 2 GB |
| Input | the official July + August 1995 logs, unchanged: 3,461,613 lines |
| Runs | 3 rounds × 3 batch sizes × 2 engines = 18 runs, 17–18 Nov 2026 |
| Order | interleaved (round → batch size → engine), so slow drift on the laptop hits every configuration equally |
| Runtime | measured by the controller, from the first byte read until the last PostgreSQL commit |

To reproduce: `./scripts/run_benchmarks.sh`. It takes about an hour, and logs go to `runs/benchmarks/<timestamp>/`.

## Results

| Batch size | Batches | Avg batch size | MongoDB median (min–max) | Hive median (min–max) | Hive ÷ MongoDB |
| --- | ---: | ---: | --- | --- | ---: |
| 100,000 | 35 | 98,903.23 | **104.7 s** (103.0–106.1) | **437.0 s** (400.4–438.2) | 4.2× |
| 500,000 | 7 | 494,516.14 | **106.3 s** (102.4–106.7) | **170.5 s** (163.7–212.3) | 1.6× |
| 1,000,000 | 4 | 865,403.25 | **105.5 s** (103.2–126.8) | **142.7 s** (133.7–145.3) | 1.35× |

**Correctness:** all 18 runs succeeded, each with 3,461,613 records and 1 malformed line. Every run, at every batch size and on both engines, produced the same final Q1, Q2 and Q3 (one SHA-256 fingerprint per query across all 18 runs). `etl.v_equivalence` shows `all_agree = true` for every batch size.

## What the numbers say

**1. MongoDB doesn't care about batch size, but Hive does.** MongoDB stays at about 105 s whether it gets 35 batches or 4. Hive drops from 437 s to 143 s as the batches get bigger.

**2. The difference is a fixed cost per batch.** Fitting the average batch time against batch size:

| Engine | Fixed cost per batch | Cost per line |
| --- | ---: | ---: |
| MongoDB | ≈ 0 s | 26.5 µs |
| Hive | **≈ 9.1 s** | 26.6 µs |

Both engines do the actual parsing and aggregation work at almost the same speed per line. Hive pays about 9 s extra for every batch, because each batch runs 4 HiveQL scripts (parse, Q1, Q2, Q3), and each one starts a beeline client, compiles the query and launches a Tez job. With 35 batches that adds up to about 5 minutes, and with 4 batches to about 36 s. MongoDB's aggregation pipelines run inside a server that is already up, so starting one costs almost nothing.

**3. Whole-run results also cost Hive more when there are many batches.** Time outside the batches (mostly `finalize()`, the whole-run Q1–Q3) was 37 s for Hive at 100k but 15–16 s at 500k and 1M. Hive's parsed table then has 35 partitions of small ORC files instead of 4–7 larger ones. MongoDB's finalize stays at about 21 s.

**4. Choosing a batch size.** For Hive, larger batches are much better, up to the memory limit. For MongoDB the choice hardly matters for speed, so smaller batches are fine if you want earlier per-batch results.

## Incidents (reported, not hidden)

- **Hive stalled once.** In round 3, the Hive 1M run (`3340af0e-…`) hung on one Q2 step for over 6 minutes. That step normally takes about 3 s. HiveServer2 was at 550% CPU with memory at its 2 GB heap limit, after about 2.5 hours of non-stop queries. The run was stopped and marked `failed` in `etl.etl_run` with the reason, Hive was restarted, and the configuration was re-run (133.7 s). **Lesson:** a long-running HiveServer2 with a small heap degrades over many queries. For long benchmark sessions, restart Hive between configurations or give it more heap.
- **Outliers.** Hive at 500k in round 3 (212.3 s) and MongoDB at 1M in round 1 (126.8 s) were slower than the other two rounds. The likely cause is the same memory pressure, or other activity on the laptop. The medians are not affected.

## Caveats

- This is one laptop. Docker on macOS runs Linux in a VM, and Hive runs in local mode inside a single container, not on a cluster. Absolute times would differ on other machines, but the **shape** (MongoDB flat, Hive's fixed cost per batch) is what the comparison shows.
- 3 rounds per configuration is enough to see the trend and the spread, not to give confidence intervals.

## Run IDs (all stored in PostgreSQL `etl.etl_run`)

| Round | Size | MongoDB | Hive |
| --- | --- | --- | --- |
| 1 | 100k | `e9179a6b-4cfb-47e7-9f61-bef0b3ef6c49` | `69b3b3e9-7916-4325-bbb3-52e79d3e55e0` |
| 1 | 500k | `9796c714-e108-4021-8370-fa8f3cedb24c` | `b689ae47-a5f1-4d1b-97b4-e2c7803b7160` |
| 1 | 1M | `c594495e-937b-4dfb-b069-9de94823848d` | `b1a7f59f-e060-4f5b-ab5a-5ab9654ad2e3` |
| 2 | 100k | `382532c3-1d4c-43e8-8a2f-5e5a93daac14` | `56e74830-44d6-4540-96a1-955461060ebc` |
| 2 | 500k | `90ede3d8-459e-401f-8bc8-4bd837e051a6` | `5232f7c1-aa3d-4703-b9ab-b5ab0c2d4721` |
| 2 | 1M | `0ac0524d-f4dc-449b-a52d-f9fc17b4cdc1` | `10057d3a-f16d-4a7f-99a1-8b9621c5ef33` |
| 3 | 100k | `19ea3fbf-b18c-47da-9fdb-47b5e3b78fde` | `3e29e8e3-a7c0-4bc7-bb43-9583ba41e0b0` |
| 3 | 500k | `b7dd18d2-ab92-46e5-8996-4b11c41b6572` | `114b0df9-90ee-4abc-bf2a-a95c54b80d18` |
| 3 | 1M | `54a20b34-b2b0-447d-b63f-b1fa2ea8c437` | `174f2782-9bba-4d29-bca3-635f7414b170` (re-run; stalled run `3340af0e-8a77-4488-9e2e-c8a12d165beb`) |

Chart source: `docs/benchmark_runtime.svg`.
