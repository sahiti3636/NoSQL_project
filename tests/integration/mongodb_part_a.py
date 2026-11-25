"""MongoDB engine, Part A check (ingestion, parsing, malformed, Q1) on the FULL dataset.

Drives the engine directly with Q2/Q3 stubbed, so it works before Person B is done.
Checks every batch count, the malformed line, final Q1 vs reference/reference_results.json,
and all 3,461,613 parsed lines field by field against the Python reference. About 2 minutes.

    docker compose up -d --wait
    .venv/bin/python tests/integration/mongodb_part_a.py
"""
import json, re, sys, time, uuid, csv
from pathlib import Path
P = Path(__file__).resolve().parents[2]; sys.path.insert(0, str(P))
from etl import config, contracts
from etl.batcher import Batcher
from etl.engine import BatchSpec, RunContext
from etl.engines.mongodb import MongoEngine
from etl.outputs import read_rows

eng = MongoEngine()
eng._q2_rows = lambda match: []                             # Person B's part, stubbed for this test only
eng._q3_pipeline = lambda: [{"$match": {"_id": None}}]
run_id = str(uuid.uuid4()); run_dir = config.RUNS_DIR / "_tests" / run_id
ctx = RunContext(run_id, "mongodb", 500_000, run_dir)
bt = Batcher(config.DEFAULT_INPUT_FILES, 500_000, run_dir)
eng.prepare(ctx); t0 = time.monotonic(); ok = True
try:
    for b in bt.batches():
        spec = BatchSpec(run_id, "mongodb", b.batch_id, 500_000, b.first_line_no, b.last_line_no,
                         b.record_count, b.input_dir, b.output_dir, [p.as_json() for p in b.pieces])
        r = eng.process_batch(spec)
        good = r.records_read == spec.record_count and r.valid_records + r.malformed_records == r.records_read
        ok &= good
        print(f"batch {b.batch_id}: read {r.records_read:,} (sent {spec.record_count:,}) valid {r.valid_records:,} "
              f"malformed {r.malformed_records}  {r.engine_ms}  {'OK' if good else 'COUNT MISMATCH'}")
        if r.malformed_records:
            print("   malformed.tsv:", [row for row in read_rows(b.output_dir / "malformed.tsv")])
    ctx.final_output_dir.mkdir(parents=True)
    fin = eng.finalize(ctx)
    print(f"finalize: valid {fin.valid_records:,} malformed {fin.malformed_records}  total {time.monotonic()-t0:.1f}s")
    ref = json.load(open(P / "reference/reference_results.json"))
    q1 = sorted([[d, int(s), int(n), int(b)] for d, s, n, b in read_rows(ctx.final_output_dir / "q1.tsv")])
    print("final Q1 equals reference:", q1 == ref["q1"], f"({len(q1)} rows)"); ok &= q1 == ref["q1"]
    # per-batch Q1 must add up to the final Q1
    from collections import Counter
    s = Counter()
    for f in sorted(run_dir.glob("batch_*/output/q1.tsv")):
        for d, st, n, by in read_rows(f): s[(d, int(st))] += int(n)
    print("per-batch Q1 counts add up to final:", s == Counter({(d, st): n for d, st, n, _ in q1}))
    # field-by-field: every parsed document vs the Python reference parse of the same line
    ENV, REQ, FB = (re.compile(x, re.S | re.A) for x in (contracts.envelope_regex(), contracts.request_regex(), contracts.request_fallback_regex()))
    MON = contracts.months()
    def expected():
        for path in config.DEFAULT_INPUT_FILES:
            lines = path.read_bytes().split(b"\n")
            if lines[-1] == b"": lines.pop()
            for raw in lines:
                s_ = raw.rstrip(b"\r").decode("utf-8", "replace"); m = ENV.match(s_)
                if not m or m.group(5) not in MON: yield (False, s_); continue
                h, _, _, dd, mo, yy, hh, mi, ss, tz, rq, sc, by = m.groups(); r = REQ.match(rq)
                me, pa, pr = (r.group(1), r.group(2), r.group(3) or "") if r else ("", FB.match(rq).group(1), "")
                yield (True, h, f"{yy}-{MON[mo]}-{dd} {hh}:{mi}:{ss} {tz}", f"{yy}-{MON[mo]}-{dd}", int(hh), me, pa, pr, int(sc), 0 if by == "-" else int(by))
    keys = ["host", "log_timestamp", "log_date", "log_hour", "http_method", "resource_path", "protocol_version", "status_code", "bytes_transferred"]
    cur = eng.parsed.find({}, {"_id": 0}).sort("n", 1).batch_size(50_000)
    mism = n = 0
    for exp, doc in zip(expected(), cur):
        n += 1
        got = (False, doc["raw"]) if not doc["ok"] else (True, *[doc[k] for k in keys])
        if got != exp:
            mism += 1
            if mism <= 3: print("   MISMATCH line", doc["n"], "\n     expected", exp, "\n     got     ", got)
    print(f"field-by-field check: {n:,} lines compared, {mism} mismatches"); ok &= mism == 0 and n == 3_461_613
finally:
    eng.cleanup(ctx)
print("PART A PASSED" if ok else "PART A FAILED")
