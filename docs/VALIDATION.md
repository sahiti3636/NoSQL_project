# Validation

How we know the four engines are correct and give the same answers. Final checks: 25 Nov 2026, on the code in this repository. Logs of every check are in [validation/](validation/).

## Checks

| Check | What it proves | Result | Log |
| --- | --- | --- | --- |
| Unit tests (`python -m unittest discover -s tests`) | batcher (incl. no-batch mode), report formatting, engine registry, regex quoting, Pig COPY escaping | **21 passed** | [unit-tests.log](validation/unit-tests.log) |
| Smoke test (`tests/smoke/run.sh`) | Hive and MongoDB parse 2,038 tricky real lines exactly like Python, field by field | **SMOKE TEST PASSED**, 0 mismatches | [smoke-test.log](validation/smoke-test.log) |
| `scripts/check_phase2.py` | Pig and MapReduce run real jobs in the `hadoop` container with the contract regex | **PHASE 2 SETUP OK** | [phase2-setup-check.log](validation/phase2-setup-check.log) |
| `scripts/check_phase2_sample.py pig` / `mapreduce` | on the 2,038-line sample plus made-up edge cases (tab and backslash in a top-20 path, malformed line with tab and backslash, trailing `\r`, unknown month, 21 paths tied for the top 20 where byte order decides): every parsed record (all nine fields), every count, every Q1–Q3 and malformed row, batch and whole run | **SAMPLE CHECK PASSED** (both) | [pig](validation/sample-check-pig.log), [mapreduce](validation/sample-check-mapreduce.log) |
| Hive parsed table on the sample | Hive's `parsed_records`, all nine fields incl. `log_timestamp`, vs Python | 2,037 rows, **0 mismatches** | (run during development) |
| `tests/integration/mongodb_part_a.py` | all 3,461,613 parsed MongoDB documents vs Python, field by field (incl. `log_timestamp`); final Q1 vs answer key | **PART A PASSED**, 0 mismatches | (about 15 minutes; run on 25 Nov) |
| Database tests (`tests/integration/test_person_b.py`) | Q2/Q3 edge cases in MongoDB (binary tie-break, 64-bit bytes, zero-error hours, empty input) and the equivalence checker's refusals | **16 passed** | [database-tests.log](validation/database-tests.log) |
| Full runs (below) | every engine at four batch sizes vs the answer key and vs each other | **all passed, all equivalent** | [final-runs.json](validation/final-runs.json), [benchmark-summary.log](validation/benchmark-summary.log) |

## Full runs: four engines × four batch sizes

16 runs on 25 Nov 2026, the same input (one SHA-256 for both files). Each run read **3,461,613 records: 3,461,612 valid, 1 malformed** (`alyssa.p`, the truncated last line of July).

- `scripts/verify_reference.py --run-id …`: final Q1 (295 rows), Q2 (20) and Q3 (1,369) equal `reference/reference_results.json`, with matching fingerprints, **for all 16 runs**.
- `python -m etl.compare --left-run <engine> --right-run <mongodb>` at the same batch size: **EQUIVALENT for all 12 pairs**, on every batch and the whole run (for example at 500k: 614 Q1, 160 Q2 and 2,744 Q3 rows, 0 differences, malformed lines identical).
- `python -m etl report --compare` (`etl.v_equivalence`): **one distinct answer per query** at 100k, 500k, 1M and 3,461,613 (no batching) across hive, mapreduce, mongodb and pig.

| Batch size | MongoDB | Hive | MapReduce | Pig |
| --- | --- | --- | --- | --- |
| 100,000 | `7068b74d` ✓ | `ce9d9ce5` ✓ | `9640ffb3` ✓ | `4501fc44` ✓ |
| 500,000 | `08e034fd` ✓ | `97571b10` ✓ | `0e1e6acd` ✓ | `18ab2f24` ✓ |
| 1,000,000 | `c0cd3a04` ✓ | `506f88da` ✓ | `982aad14` ✓ | `df5db37a` ✓ |
| no batching | `3c36925b` ✓ | `c4c8a04a` ✓ | `54ca64c2` ✓ | `53580065` ✓ |

✓ = answer key passed and (for the three non-MongoDB engines) EQUIVALENT to MongoDB at that batch size. Full run IDs and runtimes: [BENCHMARKS.md](BENCHMARKS.md).

## What "equivalent" means

`python -m etl.compare` opens a read-only transaction and refuses the comparison unless both runs succeeded, used the same input files (hash and manifest), the same batch size and the same batch boundaries, and have complete execution metadata. It then full-outer-joins every Q1, Q2 and Q3 row of every batch and of the whole run: counts and bytes must be equal, Q3's `error_rate` within `1e-12`, and a missing row on either side is a difference. Malformed lines are compared per batch. Stored fingerprints alone are never trusted: actual table values are compared.

## Phase 1 evidence (17 Nov 2026)

The files `mongodb-oracle.json`, `reference-oracle.json`, `sql-comparison.json`, `runs.json`, `run-integrity.json`, `fingerprint-equivalence.json`, `environment.json`, `mongodb-run.log`, `reference-run.log`, `person-b-tests.log` and `mongodb-final/` record the Phase 1 validation: MongoDB vs the answer key and vs the reference engine, and Hive (`36eb3560-…`) vs MongoDB (`b9e02d53-…`) at 500k, **EQUIVALENT** on all 614 Q1, 160 Q2 and 2,744 Q3 rows.
