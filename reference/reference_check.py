import re, sys, collections, json
ENV = re.compile(r'^([^ ]+) ([^ ]+) ([^ ]+) \[([0-9]{2})/([A-Za-z]{3})/([0-9]{4}):([0-9]{2}):([0-9]{2}):([0-9]{2}) ([+-][0-9]{4})\] "(.*)" ([0-9]{3}) ([0-9]+|-) *$', re.S | re.A)
ENV_DOT_NODOTALL = re.compile(ENV.pattern, re.A)  # what plain '.' would do
REQ = re.compile(r'^ *([^ ]+) +(.*?)(?: +(HTTP/[0-9]+\.[0-9]+))? *$', re.S | re.A)
TRIM = re.compile(r'^ *(.*?) *$', re.S | re.A)
MON = {m:i+1 for i,m in enumerate('Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split())}
st = collections.Counter(); bad=[]; reqkind=collections.Counter(); proto=collections.Counter(); meth=collections.Counter()
status=collections.Counter(); q1=collections.Counter(); q1b=collections.Counter()
q2c=collections.Counter(); q2b=collections.Counter(); q2h=collections.defaultdict(set)
q3t=collections.Counter(); q3e=collections.Counter(); q3h=collections.defaultdict(set)
totbytes=0; dash=0; quote_in_req=0; nodotall_fail=0; hosts=set(); tabline=0
for path in sys.argv[1:]:
    with open(path,'rb') as f:
        data=f.read()
    lines=data.split(b'\n')
    if lines and lines[-1]==b'': lines.pop()   # trailing newline does not create a record
    st['lines:'+path.split('/')[-1]]=len(lines)
    for raw in lines:
        if raw.endswith(b'\r'): raw=raw[:-1]
        s=raw.decode('utf-8','replace')
        if '\t' in s: tabline+=1
        m=ENV.match(s)
        if not m or m.group(5) not in MON:
            bad.append(s); continue
        if not ENV_DOT_NODOTALL.match(s): nodotall_fail+=1
        host,_,_,dd,mon,yy,hh,mi,ss,tz,req,sc,b=m.groups()
        date=f'{yy}-{MON[mon]:02d}-{dd}'; hour=int(hh); sc=int(sc)
        if b=='-': b=0; dash+=1
        else: b=int(b)
        if '"' in req: quote_in_req+=1
        r=REQ.match(req)
        if r:
            me,p,pr=r.group(1),r.group(2),r.group(3) or ''
            reqkind['full' if pr else 'no_protocol']+=1
            if p=='': reqkind['empty_path_after_method']+=1
        else:
            me,p,pr='',TRIM.match(req).group(1),''
            reqkind['single_token_or_empty']+=1
        meth[me]+=1; proto[pr]+=1; status[sc]+=1; totbytes+=b; hosts.add(host)
        q1[(date,sc)]+=1; q1b[(date,sc)]+=b
        q2c[p]+=1; q2b[p]+=b; q2h[p].add(host)
        q3t[(date,hour)]+=1
        if 400<=sc<=599: q3e[(date,hour)]+=1; q3h[(date,hour)].add(host)
valid=sum(q3t.values())
print('records', dict(st), 'valid', valid, 'malformed', len(bad))
print('malformed lines:'); [print('  ', repr(x)[:160]) for x in bad]
print('lines failing without DOTALL:', nodotall_fail, ' lines with tab:', tabline, ' quote inside request:', quote_in_req)
print('request kinds', dict(reqkind))
print('methods', meth.most_common()); print('protocols', proto.most_common())
print('status', sorted(status.items())); print('bytes "-" count', dash, 'total bytes', totbytes, '> 2^31:', totbytes>2**31, '> 2^63?', totbytes>2**63)
print('distinct hosts', len(hosts), ' hosts differing only by case:', len(hosts)-len({h.lower() for h in hosts}))
top=sorted(q2c.items(), key=lambda kv:(-kv[1], kv[0].encode()))
print('Q2 rank 19-22:', [(i+1,k,v) for i,(k,v) in enumerate(top[18:22],18)])
print('Q2 top20 paths with query string or uppercase?', [k for k,_ in top[:20] if '?' in k])
print('max single-path bytes', max(q2b.values()), '> 2^31', max(q2b.values())>2**31)
print('Q1 rows', len(q1), ' Q3 rows', len(q3t), ' Q3 hours with zero errors', sum(1 for k in q3t if q3e[k]==0))
# missing hours
import datetime
allh=set(); d=datetime.datetime(1995,7,1)
while d<datetime.datetime(1995,9,1):
    allh.add((d.strftime('%Y-%m-%d'),d.hour)); d+=datetime.timedelta(hours=1)
miss=sorted(allh-set(q3t))
print('hours with no requests', len(miss), miss[:3], '...', miss[-3:])
json.dump({'q1':sorted([[k[0],k[1],v,q1b[k]] for k,v in q1.items()]),
           'q2':[[k,v,q2b[k],len(q2h[k])] for k,v in top[:20]],
           'q3':sorted([[k[0],k[1],q3e[k],v,q3e[k]/v,len(q3h[k])] for k,v in q3t.items()])},
          open(sys.argv[1].rsplit('/',1)[0]+'/reference_results.json','w'))
