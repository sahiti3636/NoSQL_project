"""Phase 2 parsing check: runs the Pig or MapReduce engine on the 2,038-line smoke sample
(tests/smoke/make_sample.py) plus made-up lines (a tab and a backslash in a path, a trailing
\\r, an unknown month, a malformed line with a tab and a backslash, and 21 paths tied for the
top 20 whose cut-off depends on byte order), and compares every count and every Q1-Q3 and malformed row,
for the batch and for finalize(), with the contract rules computed in Python.

    .venv/bin/python scripts/check_phase2_sample.py pig
    .venv/bin/python scripts/check_phase2_sample.py mapreduce

Needs the hadoop container (docker compose up -d --wait hadoop) and the two logs in data/.
Does not touch PostgreSQL. Ends with SAMPLE CHECK PASSED.
"""
import collections, csv, re, shutil, subprocess, sys
from pathlib import Path

P = Path(__file__).resolve().parent.parent; engine_name = sys.argv[1] if len(sys.argv) > 1 else "pig"
sys.path.insert(0, str(P))
if not (P / "data/samples/input/edge_sample.log").exists():
    subprocess.run([sys.executable, "-I", str(P / "tests/smoke/make_sample.py"), str(P)], check=True)
from etl import outputs                                    # noqa
from etl.engine import BatchSpec, RunContext, load_engine  # noqa

C = P / "contracts"
rd = lambda f: (C / f).read_text().rstrip("\n")
ENV, REQ, FB = (re.compile(rd(f), re.S | re.A) for f in ("envelope.regex", "request.regex", "request_fallback.regex"))
MON = {r["month_name"]: r["month_number"] for r in csv.DictReader(open(C / "months.tsv"), delimiter="\t")}

sample = (P / "data/samples/input/edge_sample.log").read_bytes()
extra = (
         b'\n' b'h2 - - [01/Jul/1995:00:00:02 -0400] "GET /x\ty\\z HTTP/1.0" 404 -'
         b'\n' b'h3 - - [01/Jul/1995:00:00:03 -0400] "GET /x\ty\\z HTTP/1.0" 200 7\r'
         b'\n' b'h4 - - [01/Foo/1995:00:00:03 -0400] "GET / HTTP/1.0" 200 7'
         b'\n' b'bad\tline with a \\ backslash')
# Q2 boundary test: 21 paths with the same, highest request count, so exactly one of them must
# fall out of the top 20 on the path tie-break. Byte order (the contract, PostgreSQL C collation)
# drops the emoji path (UTF-8 F0..); Java String order would drop the U+FFFD path instead
# (UTF-16 D83D < FFFD). One path holds a tab and a backslash, so it is written to q2.tsv and must
# be COPY-escaped.
tie_paths = [f"/t/a{i:02d}".encode() for i in range(18)] + [b"/t/a\tb\\c", "/t/\ufffd".encode(), "/t/\U0001F600".encode()]
for i, path in enumerate(tie_paths):
    for k in range(300):
        extra += (b'\n' + f"t{k % 7} - - [02/Jul/1995:1{k % 10}:00:00 -0400] ".encode()
                  + b'"GET ' + path + b' HTTP/1.0" ' + (b"404" if k % 5 == 0 else b"200") + b' 10')
data = sample + extra

lines = [l.removesuffix(b"\r").decode("utf-8", "replace") for l in data.split(b"\n")]
rows, bad, parsed_exp = [], [], []
for s in lines:
    m = ENV.match(s)
    if not m or m.group(5) not in MON:
        bad.append(s); continue
    host, _, _, dd, mon, yy, hh, mi, ss, tz, req, sc, b = m.groups()
    r = REQ.match(req)
    me, pa, pr = (r.group(1), r.group(2), r.group(3) or "") if r else ("", FB.match(req).group(1), "")
    rows.append((host, f"{yy}-{MON[mon]}-{dd}", int(hh), me, pa, pr, int(sc), 0 if b == "-" else int(b)))
    parsed_exp.append([host, f"{yy}-{MON[mon]}-{dd} {hh}:{mi}:{ss} {tz}", f"{yy}-{MON[mon]}-{dd}", str(int(hh)),
                       me, pa, pr, str(int(sc)), "0" if b == "-" else str(int(b))])

q1 = collections.Counter(); q1b = collections.Counter()
q2c = collections.Counter(); q2b = collections.Counter(); q2h = collections.defaultdict(set)
q3t = collections.Counter(); q3e = collections.Counter(); q3h = collections.defaultdict(set)
for host, d, h, me, pa, pr, sc, b in rows:
    q1[(d, sc)] += 1; q1b[(d, sc)] += b
    q2c[pa] += 1; q2b[pa] += b; q2h[pa].add(host)
    q3t[(d, h)] += 1
    if 400 <= sc <= 599: q3e[(d, h)] += 1; q3h[(d, h)].add(host)
exp = {
    "q1": sorted([d, str(sc), str(q1[(d, sc)]), str(q1b[(d, sc)])] for d, sc in q1),
    "q2": sorted([p, str(c), str(q2b[p]), str(len(q2h[p]))]
                 for p, c in sorted(q2c.items(), key=lambda kv: (-kv[1], kv[0].encode()))[:20]),
    "q3": sorted([d, str(h), str(q3e[(d, h)]), str(q3t[(d, h)]), str(len(q3h[(d, h)]))] for d, h in q3t),
    "malformed": sorted([s] for s in bad),
}

run_dir = P / "runs" / f"sample_{engine_name}"
shutil.rmtree(run_dir, ignore_errors=True)
(run_dir / "batch/input").mkdir(parents=True)
(run_dir / "batch/output").mkdir(parents=True)
(run_dir / "batch/input/piece_1").write_bytes(data)
eng = load_engine(engine_name)
ctx = RunContext("sample", engine_name, len(lines), run_dir)
eng.prepare(ctx)
spec = BatchSpec("sample", engine_name, 1, len(lines), 1, len(lines), len(lines),
                 run_dir / "batch/input", run_dir / "batch/output")
res = eng.process_batch(spec)


def parsed_rows():
    """Every parsed record the engine kept for batch 1, all nine fields, as COPY-text rows."""
    from etl.engines import hadoop_common as hc
    if engine_name == "mapreduce":       # LogRecord.toString() is a COPY row; key prints as (null)
        dump = subprocess.run(["docker", "compose", "exec", "-T", "-e", f"HADOOP_CLASSPATH={eng.jar}", "hadoop",
                               "sh", "-c", f"hadoop fs -text {eng.parsed_path(1)}/parsed-* 2>/dev/null"],
                              cwd=P, capture_output=True, text=True, check=True).stdout
        return [outputs.parse_line(l[len("(null)\t"):]) for l in dump.split("\n") if l]
    from etl.engines.pig import _PARSED_SCHEMA, _copy_escape
    names = [f.split(":")[0] for f in _PARSED_SCHEMA.split(", ")]
    out = run_dir / "parsed_dump"
    hc.clear_output(out)
    eng._run(f"""parsed = LOAD '{eng.parsed_path(1)}' USING BinStorage() AS ({_PARSED_SCHEMA});
d = FOREACH parsed GENERATE {", ".join(_copy_escape(n) if ":chararray" in f else n for n, f in zip(names, _PARSED_SCHEMA.split(", ")))};
STORE d INTO '{hc.container_path(out)}' USING PigStorage('\\t');""", "dump_parsed")
    return [list(r) for r in outputs.read_rows(out)]


got_parsed = sorted(parsed_rows())
same_parsed = got_parsed == sorted(parsed_exp)
print(f"parsed records: {len(got_parsed)} rows, every field incl. log_timestamp  {'OK' if same_parsed else 'MISMATCH'}")
if not same_parsed:
    print("  missing:", [r for r in sorted(parsed_exp) if r not in got_parsed][:2])
    print("  extra:  ", [r for r in got_parsed if r not in parsed_exp][:2])
fin = eng.finalize(ctx)
ok = same_parsed and (res.records_read, res.valid_records, res.malformed_records) == (len(lines), len(rows), len(bad))
print(f"counts batch {res.records_read} {res.valid_records} {res.malformed_records}  final "
      f"{fin.records_read} {fin.valid_records} {fin.malformed_records}  expected {len(lines)} {len(rows)} {len(bad)}")
ok &= (fin.valid_records, fin.malformed_records) == (len(rows), len(bad))
for where in ("batch/output", "final/output"):
    for q in ("q1", "q2", "q3", "malformed"):
        if q == "malformed" and where == "final/output":
            continue
        got = [list(r) for r in outputs.read_rows(run_dir / where / f"{q}.tsv")]
        if q == "q3":
            for r in got:
                assert abs(float(r[4]) - int(r[2]) / int(r[3])) < 1e-12, r
            got = [r[:4] + r[5:] for r in got]
        got = sorted([str(int(v)) if v.isdigit() else v for v in r] for r in got)
        same = got == exp[q]
        ok &= same
        print(f"{where:13s} {q:9s} rows {len(got):5d}  {'OK' if same else 'MISMATCH'}")
        if not same:
            print("  missing:", [r for r in exp[q] if r not in got][:3])
            print("  extra:  ", [r for r in got if r not in exp[q]][:3])
eng.cleanup(ctx)
print("SAMPLE CHECK", "PASSED" if ok else "FAILED")
