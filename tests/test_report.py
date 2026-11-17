"""Report formatting tests (no database needed)."""
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal

from etl.report import Report, cell, render_html, render_text, table


def run_row(**over):
    row = dict(run_id="11111111-2222-3333-4444-555555555555", pipeline="hive", status="succeeded",
               started_at=datetime(2026, 11, 18, 0, 0, tzinfo=timezone.utc),
               finished_at=datetime(2026, 11, 18, 0, 2, tzinfo=timezone.utc),
               runtime_ms=133730, batch_size=1_000_000, num_batches=4,
               avg_batch_size=Decimal("865403.25"), total_records=3_461_613,
               valid_records=3_461_612, malformed_records=1, error_message=None)
    row.update(over)
    return row


def sample_report():
    rep = Report(run=run_row())
    rep.queries = [dict(query_id="q1", query_name="Daily Traffic Summary", query_duration_ms=1940),
                   dict(query_id="q2", query_name="Top Requested Resources", query_duration_ms=7180),
                   dict(query_id="q3", query_name="Hourly Error Analysis", query_duration_ms=2140)]
    rep.batches = [dict(batch_id=1, batch_size=1_000_000, valid_records=1_000_000, malformed_records=0,
                        duration_ms=35010, q1_ms=1940, q2_ms=3620, q3_ms=2030)]
    rep.results = {
        "q1": [(date(1995, 7, d), 200, 1000 + d, 2**33) for d in range(1, 16)],
        "q2": [(1, "/a\tb", 10, 20, 3), (2, "<script>x</script>", 9, 1, 1)],
        "q3": [(date(1995, 7, 1), 0, 24, 3565, 24 / 3565, 12)],
    }
    return rep


class CellTest(unittest.TestCase):
    def test_numbers_get_thousands_separators(self):
        self.assertEqual(cell(3461613), "3,461,613")
        self.assertEqual(cell(2**33), "8,589,934,592")

    def test_control_characters_are_shown_escaped(self):
        self.assertEqual(cell("/a\tb"), "/a\\tb")
        self.assertEqual(cell("\x05\x01"), "\\x05\\x01")

    def test_none_and_bool(self):
        self.assertEqual(cell(None), "")
        self.assertEqual(cell(True), "yes")


class TableTest(unittest.TestCase):
    def test_numbers_right_aligned_text_left(self):
        out = table(["path", "n"], [("/x", 5), ("/longer", 12345)]).splitlines()
        self.assertEqual(out[2], "/x            5")
        self.assertEqual(out[3], "/longer  12,345")

    def test_long_text_is_cut(self):
        self.assertIn("…", table(["p"], [("x" * 100,)], max_width=10))


class RenderTest(unittest.TestCase):
    def test_text_shows_required_metadata(self):
        out = render_text(sample_report())
        for needed in ("hive", "11111111-2222-3333-4444-555555555555", "133.73 s",
                       "1,000,000 lines", "865,403.25", "Daily Traffic Summary", "batch_id"):
            self.assertIn(needed, out)

    def test_rows_limit_applies_to_q1_and_q3_only(self):
        out = render_text(sample_report(), rows=3)
        self.assertIn("... 12 more rows", out)
        self.assertIn("/a\\tb", out)                          # Q2 always printed in full

    def test_all_rows(self):
        self.assertNotIn("more rows", render_text(sample_report(), rows=None))

    def test_failed_run_shows_error_and_no_results(self):
        rep = Report(run=run_row(status="failed", runtime_ms=None, finished_at=None,
                                 error_message="Hive stalled"))
        out = render_text(rep)
        self.assertIn("Hive stalled", out)
        self.assertIn("did not succeed", out)

    def test_html_escapes_values(self):
        page = render_html(sample_report())
        self.assertNotIn("<script>x</script>", page)
        self.assertIn("&lt;script&gt;x&lt;/script&gt;", page)
        self.assertIn("(15 rows)", page)                      # every Q1 row is in the HTML


if __name__ == "__main__":
    unittest.main()
