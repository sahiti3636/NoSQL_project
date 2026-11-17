# Person B validation results

Validated on 17 November 2026 (Asia/Calcutta). Source baseline: `7619b07`.

## Completed checks

- PostgreSQL, MongoDB, and Hive containers healthy, including after starting with `compose.lock.yml`.
- Original smoke test: **SMOKE TEST PASSED**; 2,038 sample records, 1 malformed;
  zero parsing mismatches for both MongoDB and Hive.
- Existing batcher suite: **6 tests passed**.
- New real-database Person B suite: **16 tests passed** (log included).
- Python compilation and Git whitespace check passed.
- Full MongoDB ETL: **3,461,613 records, 3,461,612 valid, 1 malformed**.
- Seven batches at size 500,000; last batch 461,613; average 494,516.14.
- MongoDB final Q1-Q3: actual PostgreSQL rows and recorded fingerprints match the committed oracle.
- SQL comparison against the test-only reference: every batch and final result equivalent.

## Final results versus committed reference JSON

| Query | Final rows | Differences | Fingerprint |
|---|---:|---:|---|
| Q1 | 295 | 0 | Matches |
| Q2 | 20 | 0 | Matches |
| Q3 | 1,369 | 0 | Matches |

## SQL comparison: reference versus MongoDB

| Query | Reference rows, all scopes | MongoDB rows, all scopes | Differences |
|---|---:|---:|---:|
| Q1 | 614 | 614 | 0 |
| Q2 | 160 | 160 | 0 |
| Q3 | 2,744 | 2,744 | 0 |

Malformed-record differences: **0**.
Q3 absolute error-rate comparison tolerance: **1e-12**.

## Runs retained in this laptop's PostgreSQL database

- MongoDB: `c817ae40-b8c4-4cb7-9f50-d67b612a7896`; measured runtime **588.36 s**.
- Test-only reference: `fe304457-dc62-4489-ad87-3deed8786f21`; measured runtime **41.14 s**.

The runtimes describe this validation session, not a controlled performance comparison. Some
setup/testing work ran concurrently, and the host uses a 4 GB Docker/WSL memory cap on an 8 GB PC.
Reference is a development oracle, never a real pipeline in the demonstration/report.

## Docker reproducibility

MongoDB **7.0.48**, PostgreSQL **15.20**, Hive image **4.0.1**; Python **3.13.0**.
Exact image references, architecture, image IDs, driver versions and memory are in
`validation/environment.json`. Use both Compose files when starting containers:

```text
docker compose -f docker-compose.yml -f compose.lock.yml up -d --wait --wait-timeout 360
```

Share the lock file to reproduce these versions. It does not prove that a teammate already
uses them; each teammate should start with that file and confirm their environment.

## Hive vs MongoDB (after the Hive engine was merged)

Run on the team's Mac on 17 November 2026, same input files, batch size 500,000, both runs in
one PostgreSQL database.

| Run | Pipeline | Records | Malformed | Runtime |
|---|---|---:|---:|---:|
| `36eb3560-c931-44ec-8811-1b9d13e2a4c9` | hive | 3,461,613 | 1 | 165.06 s |
| `b9e02d53-a31e-4c70-b040-ede2cacad9dc` | mongodb | 3,461,613 | 1 | 106.08 s |

`python -m etl.compare --left-run 36eb3560-… --right-run b9e02d53-…`:

| Query | Hive rows, all scopes | MongoDB rows, all scopes | Differences |
|---|---:|---:|---:|
| Q1 | 614 | 614 | 0 |
| Q2 | 160 | 160 | 0 |
| Q3 | 2,744 | 2,744 | 0 |

Malformed-record differences: **0**. Result: **EQUIVALENT** (exit code 0).
`scripts/verify_reference.py` also passes for the MongoDB run, and `etl.v_equivalence` shows
`all_agree = true` for Q1-Q3 across hive, mongodb and reference.

The reporting module is still team work.

## Included evidence

- `validation/mongodb-oracle.json` and `reference-oracle.json`: row and fingerprint comparisons.
- `validation/sql-comparison.json`: all-scope SQL comparison report.
- `validation/runs.json`, `run-integrity.json`, `fingerprint-equivalence.json`: SQL metadata.
- `validation/mongodb-run.log`, `reference-run.log`, `smoke-test.log`, `person-b-tests.log`.
- `validation/mongodb-final/q1.tsv`, `q2.tsv`, `q3.tsv`: final MongoDB output samples.
- `validation/environment.json`: exact Docker builds and runtime versions.
