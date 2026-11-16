import re, csv, json, sys, collections
P = sys.argv[1]; C = f'{P}/contracts'
rd = lambda f: open(f'{C}/{f}').read().rstrip('\n')
ENV, REQ, FB = (re.compile(rd(f), re.S | re.A) for f in ('envelope.regex', 'request.regex', 'request_fallback.regex'))
MON = {r['month_name']: r['month_number'] for r in csv.DictReader(open(f'{C}/months.tsv'), delimiter='\t')}
exp = []
data = open(f'{P}/data/samples/input/edge_sample.log', 'rb').read().split(b'\n')
for raw in data:
    s = raw.rstrip(b'\r').decode('utf-8', 'replace'); m = ENV.match(s)
    if not m or m.group(5) not in MON: exp.append(('MALFORMED',) + ('',) * 8); continue
    host, _, _, dd, mon, yy, hh, mi, ss, tz, req, sc, b = m.groups(); r = REQ.match(req)
    me, pa, pr = (r.group(1), r.group(2), r.group(3) or '') if r else ('', FB.match(req).group(1), '')
    exp.append(('OK', host, f'{yy}-{MON[mon]}-{dd}', str(int(hh)), me, pa, pr, sc, '0' if b == '-' else b))
def unhive(line):  # Hive ESCAPED BY '\\': backslash + char = that char; unescaped tab = field separator
    out, cur, i = [], '', 0
    while i < len(line):
        ch = line[i]
        if ch == '\\' and i + 1 < len(line): cur += line[i + 1]; i += 2; continue
        if ch == '\t': out.append(cur); cur = ''
        else: cur += ch
        i += 1
    return tuple(out + [cur])
hive = [unhive(l) for l in open(f'{P}/runs/regex_test_hive/000000_0', encoding='utf-8').read().split('\n') if l]
mongo = [tuple(json.loads(l)) for l in open(f'{P}/runs/regex_test_mongo.jsonl', encoding='utf-8')]
bad = 0
for name, got, ordered in (('MongoDB', mongo, True), ('Hive', hive, False)):
    a, b = (exp, got) if ordered else (sorted(exp), sorted(got))
    diff = [(i + 1, x, y) for i, (x, y) in enumerate(zip(a, b)) if x != y]
    print(f'{name}: {len(got)} rows, {sum(r[0] == "MALFORMED" for r in got)} malformed, mismatches vs reference: {len(diff) + abs(len(a) - len(b))}')
    bad += len(diff) + abs(len(a) - len(b))
    for d in diff[:5]: print('   line', d[0], '\n   expected', d[1], '\n   got     ', d[2])
tabrow = [r for r in hive if any('\t' in v for v in r)]
print('Hive row whose path contains a real tab survived escaping:', len(tabrow) == 1)
sys.exit(0 if bad == 0 and len(tabrow) == 1 else 1)
