"""Verify a full official-dataset run in PostgreSQL against the committed JSON oracle.

python scripts/verify_reference.py --run-id UUID
"""
import argparse
import datetime
import json
import sys
from pathlib import Path
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from etl import config, contracts, outputs


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-id', required=True, type=UUID)
    args = p.parse_args()
    ref = json.loads((config.PROJECT_ROOT / 'reference/reference_results.json').read_text())
    table_names = {'q1': 'q1_daily_traffic', 'q2': 'q2_top_resources', 'q3': 'q3_hourly_errors'}
    report = {'run_id': str(args.run_id), 'queries': {}}
    ok = True
    with psycopg.connect(config.PG_DSN) as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        run = conn.execute('SELECT status,total_records,valid_records,malformed_records FROM etl.etl_run WHERE run_id=%s',
                           (args.run_id,)).fetchone()
        report['run_counts_match'] = run == ('succeeded', 3461613, 3461612, 1)
        ok &= report['run_counts_match']
        for qid, table in table_names.items():
            columns = contracts.output_columns(qid + '.tsv')
            query = sql.SQL('SELECT {} FROM etl.{} WHERE run_id=%s AND batch_id IS NULL').format(
                sql.SQL(',').join(map(sql.Identifier, columns)), sql.Identifier(table))
            rows = [[v.isoformat() if isinstance(v, datetime.date) else v for v in row]
                    for row in conn.execute(query, (args.run_id,)).fetchall()]
            key = (lambda row: tuple(row[:2])) if qid != 'q2' else (lambda row: row[0])
            actual, expected = {key(r):r for r in rows}, {key(r):r for r in ref[qid]}
            differences = len(actual.keys() ^ expected.keys())
            tolerance = contracts.outputs()['files']['q3.tsv']['columns'][4]['compare_tolerance']
            for k in actual.keys() & expected.keys():
                a, b = actual[k], expected[k]
                equal = (a == b) if qid != 'q3' else (a[:4] == b[:4] and a[5:] == b[5:]
                                                               and abs(a[4]-b[4]) <= tolerance)
                differences += not equal
            fingerprint = outputs.fingerprint(qid, [[str(v) for v in r] for r in ref[qid]])
            stored = conn.execute('SELECT fingerprint FROM etl.query_execution WHERE run_id=%s '
                                  'AND query_id=%s AND batch_id IS NULL', (args.run_id, qid)).fetchone()
            fingerprint_ok = stored is not None and stored[0] == fingerprint
            report['queries'][qid] = {'rows': len(rows), 'expected_rows':len(ref[qid]),
                'differences':differences, 'fingerprint_matches':fingerprint_ok, 'fingerprint':fingerprint}
            ok &= not differences and fingerprint_ok
    report['passed'] = bool(ok)
    print(json.dumps(report, indent=2))
    return 0 if ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
