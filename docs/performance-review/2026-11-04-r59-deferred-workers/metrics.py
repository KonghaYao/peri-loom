import json,sys,urllib.request
d=json.load(open(sys.argv[1]))
req=urllib.request.Request(d['endpoint']+'/v1/metrics', headers={'Authorization':'Bearer '+d['global_api_key']})
with urllib.request.urlopen(req, timeout=10) as r:
    doc=json.load(r)
m=doc.get('result',doc)
if len(sys.argv)>1 and sys.argv[2:]:
    for k in sys.argv[2:]:
        print(k, '=', json.dumps(m.get(k), sort_keys=True)[:400])
else:
    for k in sorted(m):
        v=m[k]
        if isinstance(v,(str,int,float,bool)) or v is None:
            print(k,'=',v)
        elif isinstance(v,dict):
            print(k,'=',json.dumps(v,sort_keys=True)[:300])
