"""Reporting module: reads stored results from PostgreSQL and displays them with the run's
execution metadata (pipeline, run ID, time of execution, runtime, batch IDs, batch size,
average batch size, per-query times, malformed count).

    python -m etl report                          latest successful run
    python -m etl report --pipeline hive          latest successful Hive run
    python -m etl report --run-id <uuid>          one specific run
    python -m etl report --rows all               print every Q1/Q3 row (default: first 10)
    python -m etl report --html report.html       also write the full report as one HTML file
    python -m etl report --list                   recent runs
    python -m etl report --compare                runtimes and equivalence across pipelines

Everything is read through the views in sql/schema.sql (v_run_report, v_batch_report,
v_q1_final, v_q2_final, v_q3_final, v_pipeline_comparison, v_equivalence). The report only
reads; rendering it is not part of a run's measured runtime.
"""
from __future__ import annotations

import html
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from . import config

QUERY_SQL = {
    "q1": ("SELECT log_date, status_code, request_count, total_bytes FROM etl.v_q1_final "
           "WHERE run_id = %s ORDER BY log_date, status_code"),
    "q2": ("SELECT rank, resource_path, request_count, total_bytes, distinct_host_count "
           "FROM etl.v_q2_final WHERE run_id = %s ORDER BY rank"),
    "q3": ("SELECT log_date, log_hour, error_request_count, total_request_count, error_rate, "
           "distinct_error_hosts FROM etl.v_q3_final WHERE run_id = %s ORDER BY log_date, log_hour"),
}
QUERY_COLUMNS = {
    "q1": ["log_date", "status_code", "request_count", "total_bytes"],
    "q2": ["rank", "resource_path", "request_count", "total_bytes", "distinct_host_count"],
    "q3": ["log_date", "log_hour", "error_request_count", "total_request_count", "error_rate",
           "distinct_error_hosts"],
}


class ReportError(RuntimeError):
    pass


@dataclass
class Report:
    run: Dict[str, Any]                                    # one row of etl.etl_run
    queries: List[Dict[str, Any]] = field(default_factory=list)       # v_run_report rows
    batches: List[Dict[str, Any]] = field(default_factory=list)       # v_batch_report + query ms
    results: Dict[str, List[Sequence[Any]]] = field(default_factory=dict)


# ---------------------------------------------------------------------- reading
def _connect():
    import psycopg                                         # imported late: formatting needs no DB
    from psycopg.rows import dict_row
    return psycopg.connect(config.PG_DSN, row_factory=dict_row)


_NO_BATCH_NOTE = "  (no batching: the whole input in one batch)"


def _unbatched(run) -> bool:
    """A run without batching: one batch that holds every input line."""
    return run["num_batches"] == 1 and run["batch_size"] == run["total_records"]


def pick_run_id(conn, run_id: Optional[str], pipeline: Optional[str]) -> str:
    """The given run, or the latest successful real run (optionally of one pipeline)."""
    if run_id:
        return run_id
    sql = ("SELECT run_id FROM etl.etl_run WHERE status = 'succeeded' AND pipeline <> 'reference'"
           + (" AND pipeline = %s" if pipeline else "") + " ORDER BY finished_at DESC LIMIT 1")
    row = conn.execute(sql, (pipeline,) if pipeline else ()).fetchone()
    if not row:
        raise ReportError("No successful run found" + (f" for pipeline {pipeline}" if pipeline else "")
                          + ". Run one first: python -m etl run --pipeline <name> --batch-size <n>")
    return str(row["run_id"])


def load_report(conn, run_id: str) -> Report:
    run = conn.execute("SELECT * FROM etl.etl_run WHERE run_id = %s", (run_id,)).fetchone()
    if not run:
        raise ReportError(f"Run {run_id} is not in the database.")
    rep = Report(run=run)
    if run["status"] != "succeeded":
        return rep                                         # metadata + error only
    rep.queries = conn.execute(
        "SELECT query_id, query_name, query_duration_ms FROM etl.v_run_report "
        "WHERE run_id = %s ORDER BY query_id", (run_id,)).fetchall()
    if not rep.queries and run["pipeline"] == "reference":
        rep.queries = conn.execute(                        # reference runs are hidden from the view
            "SELECT e.query_id, d.query_name, e.duration_ms AS query_duration_ms "
            "FROM etl.query_execution e JOIN etl.query_def d USING (query_id) "
            "WHERE e.run_id = %s AND e.batch_id IS NULL ORDER BY e.query_id", (run_id,)).fetchall()
    rep.batches = conn.execute("""
        SELECT b.batch_id, b.batch_size, b.valid_records, b.malformed_records, b.duration_ms,
               max(e.duration_ms) FILTER (WHERE e.query_id = 'q1') AS q1_ms,
               max(e.duration_ms) FILTER (WHERE e.query_id = 'q2') AS q2_ms,
               max(e.duration_ms) FILTER (WHERE e.query_id = 'q3') AS q3_ms
        FROM etl.v_batch_report b
        LEFT JOIN etl.query_execution e ON e.run_id = b.run_id AND e.batch_id = b.batch_id
        WHERE b.run_id = %s
        GROUP BY b.batch_id, b.batch_size, b.valid_records, b.malformed_records, b.duration_ms
        ORDER BY b.batch_id""", (run_id,)).fetchall()
    with conn.cursor() as cur:                             # plain tuples for the result tables
        for qid, sql in QUERY_SQL.items():
            cur.execute(sql, (run_id,))
            rep.results[qid] = [tuple(r.values()) for r in cur.fetchall()]
    return rep


def load_runs(conn, limit: int = 20) -> List[Dict[str, Any]]:
    return conn.execute(
        "SELECT run_id, pipeline, status, batch_size, num_batches, avg_batch_size, runtime_ms, "
        "total_records, malformed_records, started_at FROM etl.etl_run "
        "ORDER BY started_at DESC LIMIT %s", (limit,)).fetchall()


def load_comparison(conn) -> Dict[str, List[Dict[str, Any]]]:
    return {
        "runtime": conn.execute(
            "SELECT pipeline, batch_size, runs, avg_runtime_ms, best_runtime_ms, num_batches, "
            "avg_batch_size FROM etl.v_pipeline_comparison ORDER BY batch_size, pipeline").fetchall(),
        "equivalence": conn.execute(
            "SELECT batch_size, query_id, pipelines, distinct_answers, all_agree "
            "FROM etl.v_equivalence ORDER BY batch_size, query_id").fetchall(),
    }


# ---------------------------------------------------------------------- formatting
def cell(v: Any) -> str:
    """One value as display text. Control characters inside paths (tabs, bytes like 0x01) are
    shown escaped so they can't break the table layout."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "yes" if v else "NO"
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, float):
        return f"{v:.6f}"
    if isinstance(v, datetime):
        return v.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    s = str(v)
    if any(ord(c) < 32 or ord(c) == 127 for c in s):
        s = s.encode("unicode_escape").decode("ascii")
    return s


def seconds(ms: Optional[int]) -> str:
    return "" if ms is None else f"{ms / 1000:,.2f} s"


def table(headers: Sequence[str], rows: Sequence[Sequence[Any]], max_width: int = 60) -> str:
    """A plain-text table. Numbers are right-aligned, text left-aligned and cut at max_width."""
    text = [[cell(v) for v in r] for r in rows]
    text = [[s if len(s) <= max_width else s[:max_width - 1] + "…" for s in r] for r in text]
    numeric = [all(isinstance(r[i], (int, float)) and not isinstance(r[i], bool)
                   for r in rows if r[i] is not None) and bool(rows) for i in range(len(headers))]
    widths = [max([len(h)] + [len(r[i]) for r in text]) for i, h in enumerate(headers)]
    fmt = lambda vals: "  ".join(v.rjust(w) if n else v.ljust(w)
                                 for v, w, n in zip(vals, widths, numeric)).rstrip()
    lines = [fmt(headers), "  ".join("-" * w for w in widths)]
    lines += [fmt(r) for r in text]
    return "\n".join(lines)


def render_text(rep: Report, rows: Optional[int] = 10) -> str:
    r = rep.run
    out = [f"NASA log ETL report  ·  run {r['run_id']}", ""]
    meta = [
        ("Pipeline", r["pipeline"] + ("  (test-only engine, not a real pipeline)"
                                      if r["pipeline"] == "reference" else "")),
        ("Status", r["status"]),
        ("Started", cell(r["started_at"])),
        ("Finished", cell(r["finished_at"])),
        ("Runtime", seconds(r["runtime_ms"]) + "  (first byte read → final PostgreSQL commit)"
         if r["runtime_ms"] is not None else ""),
        ("Batch size", cell(r["batch_size"]) + " lines" + (_NO_BATCH_NOTE if _unbatched(r) else "")),
        ("Batches", cell(r["num_batches"])),
        ("Average batch size", f"{r['avg_batch_size']:,.2f}" if r["avg_batch_size"] is not None else ""),
        ("Records", f"{cell(r['total_records'])}  ({cell(r['valid_records'])} valid, "
                    f"{cell(r['malformed_records'])} malformed)" if r["total_records"] is not None else ""),
    ]
    if r.get("error_message"):
        meta.append(("Error", r["error_message"]))
    w = max(len(k) for k, _ in meta)
    out += [f"  {k:<{w}}  {v}" for k, v in meta]
    if r["status"] != "succeeded":
        out += ["", "This run did not succeed, so it has no results to show."]
        return "\n".join(out)

    out += ["", "Queries (whole run)", table(
        ["query", "name", "final query time"],
        [(q["query_id"].upper(), q["query_name"], seconds(q["query_duration_ms"])) for q in rep.queries])]
    out += ["", "Batches", table(
        ["batch_id", "records", "valid", "malformed", "batch time", "q1", "q2", "q3"],
        [(b["batch_id"], b["batch_size"], b["valid_records"], b["malformed_records"],
          seconds(b["duration_ms"]), seconds(b["q1_ms"]), seconds(b["q2_ms"]), seconds(b["q3_ms"]))
         for b in rep.batches])]

    names = {q["query_id"]: q["query_name"] for q in rep.queries}
    for qid in ("q1", "q2", "q3"):
        data = rep.results.get(qid, [])
        shown = data if rows is None or qid == "q2" else data[:rows]
        out += ["", f"{qid.upper()}  {names.get(qid, '')}  ({len(data):,} rows)",
                table(QUERY_COLUMNS[qid], shown)]
        if len(shown) < len(data):
            out.append(f"... {len(data) - len(shown):,} more rows (use --rows all, or --html for the full report)")
    return "\n".join(out)


def render_list(runs: List[Dict[str, Any]]) -> str:
    return table(["run_id", "pipeline", "status", "batch size", "batches", "runtime", "records",
                  "malformed", "started"],
                 [(str(r["run_id"]), r["pipeline"], r["status"], r["batch_size"], r["num_batches"],
                   seconds(r["runtime_ms"]), r["total_records"], r["malformed_records"],
                   r["started_at"]) for r in runs])


def render_comparison(c: Dict[str, List[Dict[str, Any]]]) -> str:
    out = ["Runtime by pipeline and batch size (successful runs)", table(
        ["pipeline", "batch size", "runs", "avg runtime", "best runtime", "batches", "avg batch size"],
        [(r["pipeline"], r["batch_size"], r["runs"], seconds(int(r["avg_runtime_ms"])),
          seconds(r["best_runtime_ms"]), r["num_batches"], f"{r['avg_batch_size']:,.2f}")
         for r in c["runtime"]])]
    out += ["", "Do the pipelines give the same answers? (same input, same batch size)", table(
        ["batch size", "query", "pipelines", "distinct answers", "all agree"],
        [(r["batch_size"], r["query_id"].upper(), r["pipelines"], r["distinct_answers"], r["all_agree"])
         for r in c["equivalence"]])]
    return "\n".join(out)


def render_html(rep: Report) -> str:
    """The full report (every row) as one self-contained HTML page. Every value is escaped:
    real request paths contain characters like < and control bytes."""
    e = lambda v: html.escape(cell(v))
    r = rep.run

    def tbl(headers, rows):
        head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
        body = "".join("<tr>" + "".join(
            f'<td class="{"n" if isinstance(v, (int, float)) and not isinstance(v, bool) else ""}">{e(v)}</td>'
            for v in row) + "</tr>" for row in rows)
        return f"<div class=w><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"

    meta = [("Pipeline", r["pipeline"]), ("Run ID", r["run_id"]), ("Status", r["status"]),
            ("Started", r["started_at"]), ("Finished", r["finished_at"]),
            ("Runtime", seconds(r["runtime_ms"])),
            ("Batch size", f"{r['batch_size']:,} lines" + (_NO_BATCH_NOTE if _unbatched(r) else "")),
            ("Batches", r["num_batches"]),
            ("Average batch size", f"{r['avg_batch_size']:,.2f}" if r["avg_batch_size"] is not None else ""),
            ("Records", r["total_records"]), ("Valid", r["valid_records"]),
            ("Malformed", r["malformed_records"])]
    if r.get("error_message"):
        meta.append(("Error", r["error_message"]))
    parts = [f"<h1>NASA log ETL report</h1><p class=sub>{e(r['pipeline'])} · run {e(r['run_id'])}</p>",
             "<dl>" + "".join(f"<dt>{html.escape(k)}</dt><dd>{e(v)}</dd>" for k, v in meta) + "</dl>"]
    if r["status"] == "succeeded":
        names = {q["query_id"]: q["query_name"] for q in rep.queries}
        parts.append("<h2>Queries (whole run)</h2>" + tbl(
            ["query", "name", "final query time"],
            [(q["query_id"].upper(), q["query_name"], seconds(q["query_duration_ms"])) for q in rep.queries]))
        parts.append("<h2>Batches</h2>" + tbl(
            ["batch_id", "records", "valid", "malformed", "batch time", "q1", "q2", "q3"],
            [(b["batch_id"], b["batch_size"], b["valid_records"], b["malformed_records"],
              seconds(b["duration_ms"]), seconds(b["q1_ms"]), seconds(b["q2_ms"]), seconds(b["q3_ms"]))
             for b in rep.batches]))
        for qid in ("q1", "q2", "q3"):
            data = rep.results.get(qid, [])
            parts.append(f"<h2>{qid.upper()} · {html.escape(names.get(qid, ''))} "
                         f"<span class=sub>({len(data):,} rows)</span></h2>" + tbl(QUERY_COLUMNS[qid], data))
    style = """
:root{--bg:#fbfbfa;--fg:#1b1b19;--mute:#5d5c57;--line:#e3e2dd;--head:#f1f0ec;color-scheme:light}
@media (prefers-color-scheme:dark){:root{--bg:#191918;--fg:#ecebe7;--mute:#a9a8a1;--line:#33332f;--head:#232321;color-scheme:dark}}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 -apple-system,"Segoe UI",Inter,sans-serif;
padding:32px max(16px,4vw);max-width:1100px}
h1{font-size:26px;margin:0}h2{font-size:17px;margin:32px 0 8px}.sub{color:var(--mute);font-weight:400}
dl{display:grid;grid-template-columns:max-content 1fr;gap:4px 20px;margin:20px 0}dt{color:var(--mute)}dd{margin:0}
.w{overflow-x:auto;border:1px solid var(--line);border-radius:6px;max-height:520px}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}
th,td{padding:5px 10px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}
th{background:var(--head);position:sticky;top:0}td.n{text-align:right}"""
    return (f"<!doctype html><html lang=en><head><meta charset=utf-8>"
            f"<meta name=viewport content='width=device-width,initial-scale=1'>"
            f"<title>ETL report {e(r['pipeline'])} {e(r['run_id'])}</title><style>{style}</style></head>"
            f"<body>{''.join(parts)}</body></html>")


# ---------------------------------------------------------------------- entry point
def main(run_id: Optional[str] = None, pipeline: Optional[str] = None, rows: Optional[int] = 10,
         html_path: Optional[Path] = None, list_runs: bool = False, compare: bool = False) -> int:
    try:
        with _connect() as conn:
            if list_runs:
                print(render_list(load_runs(conn)))
                return 0
            if compare:
                print(render_comparison(load_comparison(conn)))
                return 0
            rep = load_report(conn, pick_run_id(conn, run_id, pipeline))
    except ReportError as exc:
        print(f"report: {exc}")
        return 2
    except Exception as exc:                               # e.g. PostgreSQL down, malformed run ID
        if type(exc).__module__.split(".")[0] != "psycopg":
            raise
        print(f"report: database error: {str(exc).strip().splitlines()[0]}")
        print(f"        (is PostgreSQL running? docker compose ps; DSN: {config.PG_DSN.split(' password')[0]})")
        return 2
    print(render_text(rep, rows))
    if html_path:
        html_path = Path(html_path)
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text(render_html(rep), encoding="utf-8")
        print(f"\nfull report written to {html_path}")
    return 0 if rep.run["status"] == "succeeded" else 1
