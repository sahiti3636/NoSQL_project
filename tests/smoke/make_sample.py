# Builds data/samples/input/edge_sample.log: first 2000 July lines + every known odd line + the truncated last line.
import re, csv, sys, os
P = sys.argv[1]; C = f'{P}/contracts'; D = f'{P}/data'
jul = open(f'{D}/NASA_access_log_Jul95', 'rb').read().split(b'\n')
aug = open(f'{D}/NASA_access_log_Aug95', 'rb').read().split(b'\n')
aug.pop()  # trailing newline
pick = jul[:2000]
odd = [l for l in jul[2000:] + aug if re.search(rb'[^ -~]', l)]                       # binary bytes, tab, \x01
quotes = [l for l in jul if l.count(b'"') > 2][:5]                                    # quote inside request
noproto = [l for l in jul if re.search(rb'"[A-Z]+ [^ "]+" ', l)][:5]                  # no protocol
fallback = [l for l in jul + aug if re.search(rb'\] "[^ "]*" [0-9]{3} ', l)][:5]       # one token / empty
lines = pick + odd + quotes + noproto + fallback + [jul[-1]]                           # jul[-1] = b'alyssa.p'
os.makedirs(f'{D}/samples/input', exist_ok=True)
open(f'{D}/samples/input/edge_sample.log', 'wb').write(b'\n'.join(lines))                   # no final newline, like July
print(len(lines), 'lines:', len(odd), 'binary/odd,', len(quotes), 'inner-quote,', len(noproto), 'no-protocol,', len(fallback), 'fallback, 1 truncated')
