"""Phase 2 plumbing that needs no Docker: engine registry, Pig string quoting, counts reader."""
import re
import shutil
import tempfile
import unittest
from pathlib import Path

from etl import contracts
from etl.engine import ENGINES, PIPELINES, load_engine
from etl.engines import hadoop_common as hc


def unquote(literal: str) -> str:
    """Reads a Pig/Java single-quoted literal back (the inverse of quote_string)."""
    assert literal[0] == literal[-1] == "'"
    return re.sub(r"\\(.)", r"\1", literal[1:-1])


class Phase2SetupTest(unittest.TestCase):
    def test_all_four_pipelines_registered(self):
        self.assertEqual(set(PIPELINES), set(ENGINES) - {"reference"})
        self.assertEqual(load_engine("pig").name, "pig")
        self.assertEqual(load_engine("mapreduce").name, "mapreduce")

    def test_contract_regexes_survive_quoting(self):
        for regex in (contracts.envelope_regex(), contracts.request_regex(),
                      contracts.request_fallback_regex(), "it's a \\ test"):
            self.assertEqual(unquote(hc.quote_string(regex)), regex)

    def test_read_counts_from_part_files(self):
        d = Path(tempfile.mkdtemp())
        try:
            out = d / "counts"
            out.mkdir()
            (out / "part-r-00000").write_text("2000\t1999\n")
            (out / "_SUCCESS").write_text("")
            (out / ".part-r-00000.crc").write_text("x")
            self.assertEqual(hc.read_counts(out, 2), (2000, 1999))
            self.assertFalse(out.exists())
        finally:
            shutil.rmtree(d, ignore_errors=True)


    def test_pig_copy_escape(self):
        """Pig's REPLACE is Java replaceAll inside a Pig literal; simulate both layers and check
        the result is PostgreSQL COPY text that reads back to the original value."""
        from etl import outputs
        from etl.engines.pig import _copy_escape
        expr = _copy_escape("x")
        steps = re.findall(r"REPLACE\(", expr)
        self.assertEqual(len(steps), 4)
        pairs = re.findall(r", '((?:[^'\\]|\\.)*)', '((?:[^'\\]|\\.)*)'\)", expr)
        value = "a\\b\tc\nd\re/é"
        out = value
        for regex, replacement in pairs:
            out = re.sub(unquote(f"'{regex}'"), unquote(f"'{replacement}'"), out)
        self.assertEqual(out, "a\\\\b\\tc\\nd\\re/é")
        self.assertEqual(outputs.parse_line(out), [value])


if __name__ == "__main__":
    unittest.main()
