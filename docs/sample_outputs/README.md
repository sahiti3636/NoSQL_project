# Sample outputs

Exported from the final runs of 25 Nov 2026 (see [../BENCHMARKS.md](../BENCHMARKS.md)) with the tool's own reporting module. Open an `.html` file in a browser for the full report with every row.

| Engine | Batched (500,000 lines per batch, 7 batches) | No batching (1 batch of 3,461,613 lines) |
| --- | --- | --- |
| MongoDB | [html](mongodb_batch500k.html) · [text](mongodb_batch500k.txt) | [html](mongodb_nobatch.html) · [text](mongodb_nobatch.txt) |
| Hive | [html](hive_batch500k.html) · [text](hive_batch500k.txt) | [html](hive_nobatch.html) · [text](hive_nobatch.txt) |
| MapReduce | [html](mapreduce_batch500k.html) · [text](mapreduce_batch500k.txt) | [html](mapreduce_nobatch.html) · [text](mapreduce_nobatch.txt) |
| Pig | [html](pig_batch500k.html) · [text](pig_batch500k.txt) | [html](pig_nobatch.html) · [text](pig_nobatch.txt) |

- [compare_all_engines.txt](compare_all_engines.txt): `python -m etl report --compare`, runtimes and equivalence of every engine at every batch size.
- [equivalence_pig_vs_mapreduce_500k.txt](equivalence_pig_vs_mapreduce_500k.txt), [equivalence_hive_vs_mongodb_nobatch.txt](equivalence_hive_vs_mongodb_nobatch.txt): `python -m etl.compare`, row by row.

Commands that produced them:

```bash
.venv/bin/python -m etl report --run-id RUN_ID --html docs/sample_outputs/pig_batch500k.html
.venv/bin/python -m etl report --compare
.venv/bin/python -m etl.compare --left-run PIG_RUN --right-run MAPREDUCE_RUN
```

The comparison averages include earlier MongoDB and Hive runs from Phase 1 that are still in the database.
