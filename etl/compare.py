"""Read-only SQL equivalence check for two explicit run IDs of different pipelines
(any two of hive, mongodb, pig, mapreduce).

python -m etl.compare --left-run HIVE_UUID --right-run MONGO_UUID
Add --allow-reference only for a development check against the test-only engine.
Exit codes: 0 = equivalent, 1 = different results, 2 = invalid/incomplete comparison.
"""
from __future__ import annotations

import argparse
import json
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from . import config, contracts
from .engine import PIPELINES


class ComparisonError(ValueError):
    pass


def compare_runs(conn, left_run: str, right_run: str, *, allow_reference=False) -> dict:
    left_id, right_id = UUID(str(left_run)), UUID(str(right_run))
    if left_id == right_id:
        raise ComparisonError("Choose two different runs; a run cannot validate itself.")
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM etl.etl_run WHERE run_id IN (%s,%s)", (left_id, right_id))
        runs = {r['run_id']: r for r in cur.fetchall()}
        if len(runs) != 2:
            raise ComparisonError("Both run IDs must exist in this PostgreSQL database.")
        left, right = runs[left_id], runs[right_id]
        pair = {left['pipeline'], right['pipeline']}
        real = set(PIPELINES)
        if not ((len(pair) == 2 and pair <= real) or (
            allow_reference and 'reference' in pair and len(pair - {'reference'} & real) == 1
        )):
            raise ComparisonError("Expected two different pipelines (hive, mongodb, pig, mapreduce), "
                                  "or --allow-reference with one real engine.")
        for run in (left, right):
            if run['status'] != 'succeeded':
                raise ComparisonError(f"Run {run['run_id']} is {run['status']}, not succeeded.")
            if not run['input_sha256'] or not run['input_manifest'] or not run['num_batches']:
                raise ComparisonError("A run is missing input identity or batch metadata.")
        fields = ('batch_size', 'total_records', 'valid_records', 'malformed_records',
                  'num_batches', 'input_sha256', 'input_manifest')
        for field in fields:
            if left[field] != right[field]:
                raise ComparisonError(f"Incomparable runs: {field} differs.")

        # Same number/contents of batches, including a short final batch. The source
        # pieces record separate files, preserving July's unterminated last line.
        ledgers = []
        for run_id, run in ((left_id, left), (right_id, right)):
            cur.execute("""SELECT batch_id,record_count,valid_records,malformed_records,
                           first_line_no,last_line_no,source_pieces
                           FROM etl.etl_batch WHERE run_id=%s ORDER BY batch_id""", (run_id,))
            ledger = cur.fetchall()
            if [b['batch_id'] for b in ledger] != list(range(1, run['num_batches'] + 1)):
                raise ComparisonError("Missing or non-sequential batch metadata.")
            first = 1
            for i, b in enumerate(ledger):
                expected = min(run['batch_size'], run['total_records'] - first + 1)
                if (b['first_line_no'] != first or b['record_count'] != expected
                        or expected <= 0 or b['last_line_no'] != first + expected - 1):
                    raise ComparisonError("Batch boundaries do not match the recorded run.")
                first += expected
            if (first - 1 != run['total_records'] or
                sum(b['valid_records'] for b in ledger) != run['valid_records'] or
                sum(b['malformed_records'] for b in ledger) != run['malformed_records']):
                raise ComparisonError("Batch totals do not match the recorded run.")
            cur.execute("SELECT count(*) n FROM etl.query_execution WHERE run_id=%s", (run_id,))
            if cur.fetchone()['n'] != 3 * (run['num_batches'] + 1):
                raise ComparisonError("Missing or extra query execution records.")
            ledgers.append(ledger)
        if ledgers[0] != ledgers[1]:
            raise ComparisonError("Batch input/count metadata differs.")

        tolerance = contracts.outputs()['files']['q3.tsv']['columns'][4]['compare_tolerance']
        cur.execute((config.PROJECT_ROOT / 'sql/compare_runs.sql').read_text(encoding='utf-8'),
                    {'left_run': left_id, 'right_run': right_id, 'rate_tolerance': tolerance})
        results = cur.fetchall()
        if len(results) != 3 or any(r['invalid_executions'] for r in results):
            raise ComparisonError("Missing query results or stored row counts do not match actual rows.")
        # Compare the malformed records as a multiset, including their batch IDs.
        cur.execute("""WITH l AS (SELECT batch_id,raw_line FROM etl.malformed_record WHERE run_id=%s),
                        r AS (SELECT batch_id,raw_line FROM etl.malformed_record WHERE run_id=%s)
                        SELECT (SELECT count(*) FROM l) left_n, (SELECT count(*) FROM r) right_n,
                        (SELECT count(*) FROM ((TABLE l EXCEPT ALL TABLE r)
                        UNION ALL (TABLE r EXCEPT ALL TABLE l)) d) differences""", (left_id, right_id))
        malformed = cur.fetchone()
        if malformed['left_n'] != left['malformed_records'] or malformed['right_n'] != right['malformed_records']:
            raise ComparisonError("Stored malformed records do not match run counts.")
        return {'left_run': str(left_id), 'right_run': str(right_id),
                'pipelines': [left['pipeline'], right['pipeline']],
                'batch_size': left['batch_size'], 'total_records': left['total_records'],
                'scope': 'every batch and final output', 'rate_tolerance': tolerance,
                'queries': results, 'malformed_differences': malformed['differences'],
                'equivalent': not any(r['differences'] for r in results) and not malformed['differences']}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--left-run', required=True, type=UUID)
    parser.add_argument('--right-run', required=True, type=UUID)
    parser.add_argument('--allow-reference', action='store_true')
    parser.add_argument('--json', action='store_true', help='machine-readable report')
    args = parser.parse_args(argv)
    try:
        with psycopg.connect(config.PG_DSN) as conn:
            conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
            report = compare_runs(conn, args.left_run, args.right_run, allow_reference=args.allow_reference)
    except (ComparisonError, psycopg.Error) as exc:
        if args.json:
            print(json.dumps({'status': 'invalid', 'error': str(exc)}))
        else:
            print(f'COMPARISON INVALID: {exc}')
        return 2
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"Comparing {' vs '.join(report['pipelines'])}; batch size {report['batch_size']:,}")
        print('Scope: every batch and final output (compared inside PostgreSQL)')
        for r in report['queries']:
            print(f"  {r['query_id']}: {r['left_rows']} / {r['right_rows']} rows; {r['differences']} differences")
        print(f"  malformed records: {report['malformed_differences']} differences")
        print('EQUIVALENT' if report['equivalent'] else 'NOT EQUIVALENT')
    return 0 if report['equivalent'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
