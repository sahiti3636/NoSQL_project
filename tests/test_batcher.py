"""Unit tests for the batcher.  Run:  .venv/bin/python -m unittest discover -s tests -v"""
import hashlib
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.batcher import Batcher  # noqa: E402


class BatcherTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        # Like the real data: file A has no final newline, file B does.
        self.a = self.dir / "A"
        self.b = self.dir / "B"
        self.a.write_bytes(b"a1\na2\n\na4\xff\tx\na5-truncated")      # 5 lines, one empty, odd bytes
        self.b.write_bytes(b"b1\nb2\nb3\n")                            # 3 lines

    def tearDown(self):
        self.tmp.cleanup()

    def run_batcher(self, size):
        bt = Batcher([self.a, self.b], size, self.dir / "run")
        return bt, list(bt.batches())

    def test_counts_and_ids(self):
        bt, batches = self.run_batcher(3)
        self.assertEqual(bt.total_records, 8)
        self.assertEqual([b.batch_id for b in batches], [1, 2, 3])
        self.assertEqual([b.record_count for b in batches], [3, 3, 2])   # only the last is short
        self.assertEqual([(b.first_line_no, b.last_line_no) for b in batches], [(1, 3), (4, 6), (7, 8)])

    def test_batch_crossing_files_has_two_unchanged_pieces(self):
        _, batches = self.run_batcher(3)
        crossing = batches[1]                        # lines 4..6 = a4, a5-truncated, b1
        self.assertEqual([p.source_file for p in crossing.pieces], ["A", "B"])
        self.assertEqual((crossing.input_dir / "A").read_bytes(), b"a4\xff\tx\na5-truncated")
        self.assertEqual((crossing.input_dir / "B").read_bytes(), b"b1\n")

    def test_pieces_reassemble_to_exact_original_bytes(self):
        for size in (1, 2, 3, 4, 5, 7, 8, 100):
            with self.subTest(batch_size=size):
                tmp = tempfile.TemporaryDirectory()
                bt = Batcher([self.a, self.b], size, Path(tmp.name) / "run")
                got = {"A": b"", "B": b""}
                for batch in bt.batches():
                    for p in batch.pieces:
                        got[p.source_file] += p.path.read_bytes()
                        data = (self.a if p.source_file == "A" else self.b).read_bytes()
                        self.assertEqual(p.path.read_bytes(), data[p.byte_start:p.byte_end])
                self.assertEqual(got["A"], self.a.read_bytes())
                self.assertEqual(got["B"], self.b.read_bytes())
                self.assertEqual(sum(f["lines"] for f in bt.manifest), 8)
                tmp.cleanup()

    def test_manifest_hashes_match_files(self):
        bt, _ = self.run_batcher(4)
        self.assertEqual([m["sha256"] for m in bt.manifest],
                         [hashlib.sha256(self.a.read_bytes()).hexdigest(),
                          hashlib.sha256(self.b.read_bytes()).hexdigest()])
        self.assertEqual([m["lines"] for m in bt.manifest], [5, 3])

    def test_exact_multiple_has_no_empty_batch(self):
        _, batches = self.run_batcher(4)
        self.assertEqual([b.record_count for b in batches], [4, 4])

    def test_rejects_bad_input(self):
        with self.assertRaises(ValueError):
            Batcher([self.a], 0, self.dir / "r1")
        with self.assertRaises(FileNotFoundError):
            Batcher([self.dir / "missing"], 10, self.dir / "r2")


if __name__ == "__main__":
    unittest.main()
