"""Real MongoDB/PostgreSQL regression tests; Docker services must be healthy.

python -m unittest discover -s tests/integration -p test_person_b.py -v
Uses unique collections/run IDs; removes only records created by this test.
"""
import tempfile
import unittest
import uuid
from pathlib import Path

import psycopg
from pymongo import MongoClient

from etl import config
from etl.compare import ComparisonError, compare_runs
from etl.engines.mongodb import MongoEngine
from etl.runner import run


class MongoQueriesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
        cls.client.admin.command('ping')
        cls.collection = cls.client.nasa_etl['person_b_test_' + uuid.uuid4().hex]
        cls.engine = MongoEngine()

    @classmethod
    def tearDownClass(cls):
        cls.collection.drop()
        cls.client.close()

    def setUp(self):
        self.collection.delete_many({})

    def aggregate(self, query, documents):
        if documents:
            self.collection.insert_many(documents)
        return list(self.collection.aggregate([{'$match': {'ok': True}}] +
                    getattr(self.engine, '_' + query + '_pipeline')(),
                    allowDiskUse=True, collation={'locale': 'simple'}))

    def test_q2_exact_hosts_bytes_and_no_method_status_filter(self):
        docs = [{'ok': True, 'resource_path': '/a', 'host': h, 'bytes_transferred': n,
                 'status_code': s, 'http_method': m, 'b': b}
                for h, n, s, m, b in [('same', 2**32, 200, 'GET', 1),
                                      ('same', 0, 404, 'POST', 2),
                                      ('other', 7, 599, 'HEAD', 2)]]
        docs.append({'ok': False, 'resource_path': '/a', 'host': 'bad', 'bytes_transferred': 99})
        self.assertEqual(self.aggregate('q2', docs), [dict(resource_path='/a', request_count=3,
            total_bytes=2**32+7, distinct_host_count=2)])

    def test_q2_top20_binary_tiebreak(self):
        paths = ['/z', '/Z', '/a', '/A', '/é'] + [f'/p{i:02}' for i in range(20)]
        rows = self.aggregate('q2', [dict(ok=True, resource_path=p, host='h', bytes_transferred=1)
                                     for p in reversed(paths)])
        self.assertEqual([r['resource_path'] for r in rows], sorted(paths, key=lambda s:s.encode())[:20])

    def test_q3_boundaries_denominator_and_distinct_error_hosts(self):
        docs = [dict(ok=True, log_date='1995-07-01', log_hour=0, host=h, status_code=s)
                for h,s in [('repeat',399),('repeat',400),('repeat',599),('other',500),('ok',600)]]
        rows = self.aggregate('q3', docs)
        self.assertEqual(rows, [dict(log_date='1995-07-01', log_hour=0, error_request_count=3,
             total_request_count=5, error_rate=0.6, distinct_error_hosts=2)])

    def test_q3_zero_error_hour_and_separate_dates(self):
        rows = self.aggregate('q3', [dict(ok=True,log_date=d,log_hour=h,host='x',status_code=s)
            for d,h,s in [('1995-07-01',1,200),('1995-07-01',2,404),('1995-07-02',1,302)]])
        self.assertEqual([(r['log_date'],r['log_hour'],r['error_rate'],r['distinct_error_hosts']) for r in rows],
                         [('1995-07-01',1,0,0),('1995-07-01',2,1,1),('1995-07-02',1,0,0)])

    def test_empty_valid_input(self):
        self.assertEqual(self.aggregate('q2', [{'ok': False}]), [])
        self.assertEqual(self.aggregate('q3', []), [])


class EndToEndComparisonTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.run_ids = []
        cls.original_runs = config.RUNS_DIR
        config.RUNS_DIR = Path(cls.tmp.name) / 'runs'
        try:
            # The same host recurs across boundaries; include zero-error hours,
            # byte counts above 32 bits, a tab path, and an unterminated malformed file.
            lines = [f'h{i%3} - - [01/Jul/1995:00:00:00 -0400] "GET /p{i%23:02} HTTP/1.0" '
                     f'{[200,399,400,599,600][i%5]} {2**32 if i==0 else i}' for i in range(65)]
            lines += ['ok - - [01/Jul/1995:01:00:00 -0400] "GET /zero HTTP/1.0" 200 -',
                      'tab - - [02/Jul/1995:01:00:00 -0400] "GET /has\ttab HTTP/1.0" 404 4',
                      'alyssa.p']
            cls.path = Path(cls.tmp.name) / 'fixture.log'
            cls.path.write_bytes('\n'.join(lines).encode())
            cls.ids = {}
            for pipeline in ('reference', 'mongodb'):
                summary = run(pipeline, 17, [cls.path], log=lambda _:None)
                cls.run_ids.append(summary.run_id)
                cls.ids[pipeline] = summary.run_id
        except BaseException:
            cls.tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls):
        config.RUNS_DIR = cls.original_runs
        if cls.run_ids:
            with psycopg.connect(config.PG_DSN) as conn:
                conn.execute('DELETE FROM etl.etl_run WHERE run_id = ANY(%s::uuid[])', (cls.run_ids,))
        cls.tmp.cleanup()

    def setUp(self):
        self.conn = psycopg.connect(config.PG_DSN)

    def tearDown(self):
        self.conn.rollback()  # All corruption scenarios are rolled back.
        self.conn.close()

    def compare(self):
        return compare_runs(self.conn, self.ids['reference'], self.ids['mongodb'], allow_reference=True)

    def test_every_batch_and_final_equal(self):
        self.assertTrue(self.compare()['equivalent'])

    def test_reference_requires_explicit_opt_in(self):
        with self.assertRaises(ComparisonError):
            compare_runs(self.conn, self.ids['reference'], self.ids['mongodb'])

    def test_self_comparison_rejected(self):
        with self.assertRaises(ComparisonError):
            compare_runs(self.conn, self.ids['mongodb'], self.ids['mongodb'])

    def test_missing_run_rejected(self):
        with self.assertRaises(ComparisonError):
            compare_runs(self.conn, uuid.uuid4(), self.ids['mongodb'])

    def test_wrong_dataset_rejected(self):
        self.conn.execute("UPDATE etl.etl_run SET input_sha256=%s WHERE run_id=%s",
                          ('0'*64, self.ids['mongodb']))
        with self.assertRaises(ComparisonError): self.compare()

    def test_failed_run_rejected(self):
        self.conn.execute("UPDATE etl.etl_run SET status='failed' WHERE run_id=%s", (self.ids['mongodb'],))
        with self.assertRaises(ComparisonError): self.compare()

    def test_different_batch_size_rejected(self):
        self.conn.execute('UPDATE etl.etl_run SET batch_size=18 WHERE run_id=%s', (self.ids['mongodb'],))
        with self.assertRaises(ComparisonError): self.compare()

    def test_changed_q2_detected_even_with_unchanged_fingerprint(self):
        self.conn.execute('UPDATE etl.q2_top_resources SET total_bytes=total_bytes+1 '
                          'WHERE run_id=%s AND batch_id IS NULL', (self.ids['mongodb'],))
        self.assertFalse(self.compare()['equivalent'])

    def test_deleted_final_rows_rejected(self):
        self.conn.execute('DELETE FROM etl.q2_top_resources WHERE run_id=%s AND batch_id IS NULL',
                          (self.ids['mongodb'],))
        with self.assertRaises(ComparisonError): self.compare()

    def test_missing_execution_rejected(self):
        self.conn.execute("DELETE FROM etl.query_execution WHERE run_id=%s AND query_id='q2' AND batch_id IS NULL",
                          (self.ids['mongodb'],))
        with self.assertRaises(ComparisonError): self.compare()

    def test_q3_rate_tolerance(self):
        self.conn.execute('UPDATE etl.q3_hourly_errors SET error_rate=error_rate+5e-13 '
                          'WHERE run_id=%s AND batch_id IS NULL AND error_rate BETWEEN 0.01 AND 0.99',
                          (self.ids['mongodb'],))
        self.assertTrue(self.compare()['equivalent'])
        self.conn.execute('UPDATE etl.q3_hourly_errors SET error_rate=error_rate+5e-10 '
                          'WHERE run_id=%s AND batch_id IS NULL AND error_rate BETWEEN 0.01 AND 0.99',
                          (self.ids['mongodb'],))
        self.assertFalse(self.compare()['equivalent'])


if __name__ == '__main__':
    unittest.main()
