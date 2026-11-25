"""Pig pipeline: Pig Latin on Pig 0.18 / Hadoop 3.3.6 in local mode.

Aman (Person C):    ingestion, parsing with the contract regexes, malformed records, Q1.
Prajwal (Person D): Q2, Q3, whole-run results (finalize), cleanup.

How the work is split between Python and Pig (same idea as Hive)
-------------------------------------------------------------------
Python only writes Pig Latin scripts and starts them inside the hadoop container. It never reads
a log line. Pig does everything else:

  batch_000001/input  --TextLoader-->  raw lines (1 chararray each)
  raw lines  --RegexFind(envelope), JOIN months.tsv-->  SPLIT into parsed / malformed
  parsed     --STORE BinStorage-->  pig_data/parsed/batch_000001     (kept for finalize)
  malformed  --STORE BinStorage-->  pig_data/malformed/batch_000001  (kept for finalize)
  malformed  --STORE-->  malformed.tsv;  every line  --GROUP ALL-->  counts
  parsed     --one script per query-->  q1.tsv, q2.tsv, q3.tsv
  finalize(): the same query scripts over pig_data/parsed/batch_*, and a counts script

Design choices
  * One `pig` call per batch: a small driver script runs the parse, Q1, Q2 and Q3 scripts with
    `exec` in one Pig session and times each step with `sh date`, so every query still gets its
    own time in etl.query_execution. Starting `pig` once per script cost several seconds each.
  * pig.jobcontrol.sleep = 100 ms: Pig checks for finished jobs every 100 ms instead of 5 s.
  * RegexFind = REGEX_EXTRACT_ALL('false'): Matcher.find(), like Hive and MapReduce. The fallback
    regex runs only when the request regex does not match.
  * Lines end at \\n only (the -D option below); one trailing \\r is stripped. The batch folder
    (1 or 2 pieces) is loaded as is, never joined.
  * Month not in months.tsv -> malformed. log_date and log_timestamp are sliced from the regex
    groups (no date functions). Missing parts are '' (never null); bytes '-' -> 0.
  * Q2: top 20 by request_count desc, then resource_path in UTF-8 byte order (the Java UDF
    pig/src/etl/pig/Utf8Key.java, compiled once per run), picked in one reducer without a
    sampling job; distinct hosts only for the 20.
  * Output files are PostgreSQL COPY text: _copy_escape() escapes \\\\ \\t \\n \\r in text fields.
  * Parsed/malformed rows are kept with BinStorage(): 7 real request lines contain byte 0x01 and
    a path may contain a tab, which a text format would split.
See docs/pig_engine.md.
"""
from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .. import config, contracts
from ..engine import BatchResult, BatchSpec, Engine, RunContext
from . import hadoop_common as hc

# First lines of every generated script.
_HEADER = "SET default_parallel 1;\n"

# Schema of the parsed rows kept per batch with BinStorage (BinStorage keeps the types).
_PARSED_SCHEMA = ("host:chararray, log_timestamp:chararray, log_date:chararray, log_hour:int, "
                  "http_method:chararray, resource_path:chararray, protocol_version:chararray, "
                  "status_code:int, bytes:long")

# Pig options for every call:
#   lines end at \n only (Hadoop's default reader also ends a line at a lone \r; a Pig SET
#   statement cannot hold a real newline character, so it is passed on the command line), and
#   check for finished jobs every 100 ms (Pig's default 5 s left every job idle after it ended).
_PIG_OPTIONS = ["-Dtextinputformat.record.delimiter=\n", "-Dpig.jobcontrol.sleep=100"]

_UDF_SOURCES = config.PROJECT_ROOT / "pig" / "src"      # Java UDFs, compiled once per run
_PIG_JAR = "/opt/pig/pig-0.18.0-core-h3.jar"             # in the hadoop container
_TICK = "ETL_TICK_"                                       # timing marks printed by the driver


def _copy_escape(field: str) -> str:
    """Pig expression for Contract 2's COPY text escaping of one chararray field:
    backslash -> \\\\, tab -> \\t, newline -> \\n, carriage return -> \\r.
    REPLACE is Java String.replaceAll: the first argument is a regex, the second a replacement
    string, and both sit inside a Pig '...' literal, so every backslash is doubled twice."""
    expr = field
    # (regex, replacement) exactly as they appear in the Pig script
    for regex, replacement in ((r"\\\\", r"\\\\\\\\"), (r"\\t", r"\\\\t"),
                               (r"\\n", r"\\\\n"), (r"\\r", r"\\\\r")):
        expr = f"REPLACE({expr}, '{regex}', '{replacement}')"
    return expr


class PigEngine(Engine):
    name = "pig"

    def __init__(self) -> None:
        self.run_dir: Optional[Path] = None
        self.scripts_dir: Optional[Path] = None
        self.logs_dir: Optional[Path] = None
        self.data_dir: Optional[Path] = None

    # ------------------------------------------------------------------ setup
    def prepare(self, run: RunContext) -> None:
        self.run_dir = run.run_dir
        self.scripts_dir = run.run_dir / "pig_scripts"      # every script is kept for review
        self.logs_dir = run.run_dir / "pig_logs"
        self.data_dir = run.run_dir / "pig_data"            # parsed/ and malformed/, one folder per batch
        for d in (self.scripts_dir, self.logs_dir, self.data_dir / "parsed", self.data_dir / "malformed"):
            d.mkdir(parents=True, exist_ok=True)
        self._compile_udfs()

        # Contract regexes as Pig string literals, ready to drop into RegexFind(...)
        self.env = hc.quote_string(contracts.envelope_regex())
        self.req = hc.quote_string(contracts.request_regex())
        self.fb = hc.quote_string(contracts.request_fallback_regex())

    def parsed_path(self, batch_id: int) -> str:
        """Where batch <batch_id>'s parsed rows live, as Pig sees it."""
        return hc.container_path(self.data_dir / "parsed" / f"batch_{batch_id:06d}")

    def malformed_path(self, batch_id: int) -> str:
        return hc.container_path(self.data_dir / "malformed" / f"batch_{batch_id:06d}")

    # ------------------------------------------------------------------ Aman: parse one batch
    def process_batch(self, spec: BatchSpec) -> BatchResult:
        """Parses one batch inside Pig, keeps the parsed and malformed rows for finalize(), writes
        this batch's malformed.tsv and counts, then runs Q1-Q3 over this batch's parsed rows.

        The parse script reads the batch once; Pig's multi-query execution feeds all four STOREs
        (parsed, malformed, malformed.tsv, counts) from that single scan. All four scripts run
        from one `pig` call.
        """
        b = spec.batch_id
        label = f"batch_{b:06d}"
        out = spec.output_dir
        for path in (out / "malformed.tsv", out / "counts", self.data_dir / "parsed" / label,
                     self.data_dir / "malformed" / label):
            hc.clear_output(path)                            # a retried batch starts clean

        parse = f"""
DEFINE RegexFind REGEX_EXTRACT_ALL('false');
raw = LOAD '{hc.container_path(spec.input_dir)}' USING TextLoader() AS (line:chararray);
{self._parse_statements('raw')}

STORE parsed INTO '{self.parsed_path(b)}' USING BinStorage();
STORE malformed INTO '{self.malformed_path(b)}' USING BinStorage();

-- malformed.tsv: the raw line, COPY-escaped (a raw line can hold a tab or a backslash)
malformed_tsv = FOREACH malformed GENERATE {_copy_escape('line')};
STORE malformed_tsv INTO '{hc.container_path(out / "malformed.tsv")}' USING PigStorage('\\t');

-- one row: records_read, valid, malformed. Every line is counted here, valid or not.
flags = FOREACH checked GENERATE (month_number IS NULL ? 0L : 1L) AS ok;
all_flags = GROUP flags ALL;
counts = FOREACH all_flags GENERATE COUNT_STAR(flags), SUM(flags.ok), COUNT_STAR(flags) - SUM(flags.ok);
STORE counts INTO '{hc.container_path(out / "counts")}' USING PigStorage('\\t');
"""
        ms = self._run_scripts([("parse", parse)] + self._query_scripts(self.parsed_path(b), out), label)
        read, valid, malformed = hc.read_counts(out / "counts", 3, "Pig")
        return BatchResult(batch_id=b, records_read=read, valid_records=valid,
                           malformed_records=malformed, engine_ms=ms)

    def _parse_statements(self, raw: str) -> str:
        """Pig Latin that turns relation `raw` (line:chararray) into `parsed` and `malformed`.
        Also leaves `checked` (every line plus its month number, null when malformed) for counts.

        Rules (contracts/README.md):
          * strip one trailing \\r (Hadoop already split on \\n only, see _PIG_OPTIONS)
          * malformed = envelope does not match, or the month is not in months.tsv
          * log_date = year-month-day, log_timestamp = log_date HH:MM:SS zone, both by slicing
            regex groups (no date functions)
          * request regex gives method, path, protocol; when it does not match (and only then)
            the fallback regex gives the path and method/protocol are ''
          * missing parts are '' never null; bytes '-' -> 0; bytes are long
        """
        return f"""
lines = FOREACH {raw} GENERATE REPLACE(line, '\\\\r\\\\z', '') AS line;
env = FOREACH lines GENERATE line, RegexFind(line, {self.env}) AS e;
env_month = FOREACH env GENERATE line, e, (e IS NULL ? NULL : (chararray) e.$4) AS month_name;

-- month map read from the contract file at run time (header row never matches a 3-letter month)
months = LOAD '{hc.CONTRACTS}/months.tsv' USING PigStorage('\\t') AS (name:chararray, number:chararray);
joined = JOIN env_month BY month_name LEFT OUTER, months BY name USING 'replicated';
checked = FOREACH joined GENERATE env_month::line AS line, env_month::e AS e, months::number AS month_number;

SPLIT checked INTO good IF month_number IS NOT NULL, bad OTHERWISE;
malformed = FOREACH bad GENERATE line;

with_req = FOREACH good GENERATE e, month_number,
           CONCAT((chararray) e.$5, '-', month_number, '-', (chararray) e.$3) AS log_date,
           RegexFind((chararray) e.$10, {self.req}) AS r;
parsed = FOREACH with_req GENERATE
    (chararray) e.$0 AS host,
    CONCAT(log_date, ' ', (chararray) e.$6, ':', (chararray) e.$7, ':', (chararray) e.$8, ' ', (chararray) e.$9) AS log_timestamp,
    log_date,
    (int) e.$6 AS log_hour,
    (r IS NULL ? '' : (chararray) r.$0) AS http_method,
    (r IS NULL ? (chararray) RegexFind((chararray) e.$10, {self.fb}).$0 : (chararray) r.$1) AS resource_path,
    (r IS NULL ? '' : ((chararray) r.$2 IS NULL ? '' : (chararray) r.$2)) AS protocol_version,
    (int) e.$11 AS status_code,
    ((chararray) e.$12 == '-' ? 0L : (long) e.$12) AS bytes;
"""

    # ------------------------------------------------------------------ Prajwal: whole run
    def finalize(self, run: RunContext) -> BatchResult:
        """Q1-Q3 over every batch's parsed rows (the glob pig_data/parsed/batch_*), never added up
        from batch outputs. The whole-run counts are counted by Pig over the kept parsed and
        malformed folders, not by Python."""
        out = run.final_output_dir
        out.mkdir(parents=True, exist_ok=True)
        parsed_all = hc.container_path(self.data_dir / "parsed") + "/batch_*"
        malformed_all = hc.container_path(self.data_dir / "malformed") + "/batch_*"
        hc.clear_output(out / "counts")
        counts = f"""
parsed = LOAD '{parsed_all}' USING BinStorage() AS ({_PARSED_SCHEMA});
malformed = LOAD '{malformed_all}' USING BinStorage() AS (line:chararray);
v = FOREACH parsed GENERATE 1L AS valid, 0L AS bad;
m = FOREACH malformed GENERATE 0L AS valid, 1L AS bad;
both = UNION v, m;
g = GROUP both ALL;
counts = FOREACH g GENERATE SUM(both.valid), SUM(both.bad);
STORE counts INTO '{hc.container_path(out / "counts")}' USING PigStorage('\\t');
"""
        ms = self._run_scripts(self._query_scripts(parsed_all, out) + [("counts", counts)], "final")
        ms.pop("counts", None)                               # not a query: not stored as a query time
        valid, malformed = hc.read_counts(out / "counts", 2, "Pig")
        return BatchResult(batch_id=None, records_read=valid + malformed, valid_records=valid,
                           malformed_records=malformed, engine_ms=ms)

    def cleanup(self, run: RunContext) -> None:
        """Deletes the kept parsed/malformed data. Must not raise."""
        if self.data_dir is not None:
            shutil.rmtree(self.data_dir, ignore_errors=True)

    # ------------------------------------------------------------------ queries (Aman: Q1, Prajwal: Q2, Q3)
    def _query_scripts(self, parsed_input: str, out_dir: Path) -> List[Tuple[str, str]]:
        """Q1-Q3 over parsed_input (one batch folder, or the glob .../parsed/batch_* for the whole
        run), written as q1.tsv, q2.tsv, q3.tsv into out_dir. Used by process_batch and finalize.
        Each query is its own script, so each gets its own time, as in the Hive engine."""
        load = f"parsed = LOAD '{parsed_input}' USING BinStorage() AS ({_PARSED_SCHEMA});"
        o = {q: hc.container_path(out_dir / f"{q}.tsv") for q in ("q1", "q2", "q3")}
        for q in o:
            hc.clear_output(out_dir / f"{q}.tsv")
        return [
            # Q1 Daily Traffic Summary (Aman)
            ("q1", f"""
{load}
g = GROUP parsed BY (log_date, status_code);
q1 = FOREACH g GENERATE group.log_date, group.status_code, COUNT_STAR(parsed), SUM(parsed.bytes);
STORE q1 INTO '{o["q1"]}' USING PigStorage('\\t');
"""),
            # Q2 Top 20 Requested Resources (Prajwal). The top 20 is picked in one reducer from the
            # per-path counts (no sampling job); ties go to the path in UTF-8 byte order. Distinct
            # hosts are counted only for the 20 winners, joined back to the parsed rows in memory.
            # Row order in q2.tsv does not matter: the report ranks with the same rule in SQL.
            ("q2", f"""
REGISTER '{self.udf_jar}';
DEFINE Utf8Key etl.pig.Utf8Key();
{load}
by_path = GROUP parsed BY resource_path;
counted = FOREACH by_path GENERATE group AS resource_path, COUNT_STAR(parsed) AS request_count,
                                   Utf8Key(group) AS path_key;
everything = GROUP counted ALL;
top = FOREACH everything {{
    ranked = ORDER counted BY request_count DESC, path_key ASC;
    best = LIMIT ranked 20;
    GENERATE FLATTEN(best) AS (resource_path:chararray, request_count:long, path_key:chararray);
}}
top20 = FOREACH top GENERATE resource_path;
rows = JOIN parsed BY resource_path, top20 BY resource_path USING 'replicated';
g = GROUP rows BY top20::resource_path;
q2 = FOREACH g {{
    hosts = DISTINCT rows.parsed::host;
    GENERATE group AS resource_path, COUNT_STAR(rows) AS request_count,
             SUM(rows.parsed::bytes) AS total_bytes, COUNT_STAR(hosts) AS distinct_host_count;
}}
q2_out = FOREACH q2 GENERATE {_copy_escape('resource_path')}, request_count, total_bytes, distinct_host_count;
STORE q2_out INTO '{o["q2"]}' USING PigStorage('\\t');
"""),
            # Q3 Hourly Error Analysis (Prajwal). Every hour with traffic keeps its row, so hours
            # with zero errors appear (the error filter is inside the group only).
            ("q3", f"""
{load}
flagged = FOREACH parsed GENERATE log_date, log_hour, host,
          ((status_code >= 400 AND status_code <= 599) ? 1L : 0L) AS is_error;
g = GROUP flagged BY (log_date, log_hour);
q3 = FOREACH g {{
    errors = FILTER flagged BY is_error == 1L;
    error_hosts = DISTINCT errors.host;
    GENERATE group.log_date, group.log_hour,
             SUM(flagged.is_error) AS error_request_count,
             COUNT_STAR(flagged) AS total_request_count,
             (double) SUM(flagged.is_error) / (double) COUNT_STAR(flagged) AS error_rate,
             COUNT_STAR(error_hosts) AS distinct_error_hosts;
}}
STORE q3 INTO '{o["q3"]}' USING PigStorage('\\t');
"""),
        ]

    # ------------------------------------------------------------------ helpers
    def _compile_udfs(self) -> None:
        """Compiles pig/src (Java UDFs) into runs/<run>/pig_build/etl-pig.jar inside the container."""
        build = self.run_dir / "pig_build"
        hc.clear_output(build)
        shutil.copytree(_UDF_SOURCES, build / "src")             # visible to the container under /runs
        b = hc.container_path(build)
        self.udf_jar = f"{b}/etl-pig.jar"
        hc.run_in_container(["sh", "-c", f'set -e; mkdir -p {b}/classes; '
                             f'javac -encoding UTF-8 -nowarn -cp "{_PIG_JAR}:$(hadoop classpath)" '
                             f'-d {b}/classes $(find {b}/src -name "*.java"); '
                             f'jar cf {self.udf_jar} -C {b}/classes .'],
                            "00_compile_udfs", self.logs_dir, what="Pig")

    def _run_scripts(self, scripts: List[Tuple[str, str]], label: str) -> Dict[str, int]:
        """Saves each script as runs/<run>/pig_scripts/<label>_<nn>_<name>.pig and runs them all,
        in order, from ONE `pig` call: a driver script `exec`s each one and prints a clock reading
        (`sh date`) before the first and after each, so every step gets its own time.
        Raises with Pig's error if any script fails; the full log is in pig_logs/<label>.log."""
        if self.scripts_dir is None:
            raise RuntimeError("PigEngine.prepare() has not run")
        tick = f"sh date +{_TICK}%s%N;\n"
        driver = [tick]
        for i, (name, body) in enumerate(scripts, 1):
            script = self.scripts_dir / f"{label}_{i:02d}_{name}.pig"
            script.write_text(_HEADER + body, encoding="utf-8")
            driver += [f"exec {hc.container_path(script)};\n", tick]
        main = self.scripts_dir / f"{label}.pig"
        main.write_text("".join(driver), encoding="utf-8")
        hc.run_in_container(["pig", *_PIG_OPTIONS, "-x", "local", "-f", hc.container_path(main)],
                            label, self.logs_dir, what="Pig")
        ticks = [int(t) for t in re.findall(_TICK + r"(\d+)", (self.logs_dir / f"{label}.log").read_text())]
        if len(ticks) != len(scripts) + 1:
            raise RuntimeError(f"Pig: expected {len(scripts) + 1} timing marks in {label}.log, got {len(ticks)}")
        return {name: (ticks[i + 1] - ticks[i]) // 1_000_000 for i, (name, _) in enumerate(scripts)}

    def _run(self, body: str, label: str) -> int:
        """Runs one script on its own with `pig -f` and returns its time in ms. Used by the setup
        check (scripts/check_phase2.py); the pipeline itself uses _run_scripts()."""
        if self.scripts_dir is None:
            raise RuntimeError("PigEngine.prepare() has not run")
        script = self.scripts_dir / f"{label}.pig"
        script.write_text(_HEADER + body, encoding="utf-8")
        return hc.run_in_container(["pig", *_PIG_OPTIONS, "-x", "local", "-f", hc.container_path(script)],
                                   label, self.logs_dir, what="Pig")
