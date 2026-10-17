from pathlib import Path
import re,json,statistics as s,csv
p=Path(__file__).resolve().parent
out={}
for fname in ['01-go-raw.log','01-go-full-raw.log']:
 groups={}
 for line in (p/fname).read_text().splitlines():
  m=re.match(r'BenchmarkAdversarial/(\S+)-2\s+\d+\s+([\d.]+) ns/op\s+[\d.]+ MB/s\s+(\d+) B/op\s+(\d+) allocs/op',line)
  if m:groups.setdefault(m[1],[]).append(list(map(float,m.groups()[1:])))
 out[fname]={name:{metric:{'median':s.median(row[i] for row in rows),'mean':s.mean(row[i] for row in rows),'stdev':s.stdev(row[i] for row in rows) if len(rows)>1 else 0,'min':min(row[i] for row in rows),'max':max(row[i] for row in rows)} for i,metric in enumerate(['ns/op','B/op','allocs/op'])}|{'samples':len(rows)} for name,rows in groups.items()}
rf=p/'01-rust-raw.log'
if rf.exists():
 groups={}
 for line in rf.read_text().splitlines():
  if re.match(r'^\d+,(dual|single),',line):
   round,arm,size,n,ns,bytes,allocs=line.split(',')
   groups.setdefault(size+'/'+arm,[]).append([float(ns),float(bytes),float(allocs)])
 out['rust']={name:{metric:{'median':s.median(row[i] for row in rows),'mean':s.mean(row[i] for row in rows),'stdev':s.stdev(row[i] for row in rows)} for i,metric in enumerate(['ns/op','allocated_B/op','allocs/op'])}|{'samples':len(rows)} for name,rows in groups.items()}
(p/'01-summary.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out,indent=2))
