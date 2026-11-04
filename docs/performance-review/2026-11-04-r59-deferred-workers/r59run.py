import json,sys,time,urllib.request,subprocess,os
conn=sys.argv[1]; out=sys.argv[2]
d=json.load(open(conn))
def metrics():
    req=urllib.request.Request(d['endpoint']+'/v1/metrics', headers={'Authorization':'Bearer '+d['global_api_key']})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r).get('result',{})
def wait_idle(limit=30):
    t0=time.time()
    while time.time()-t0<limit:
        m=metrics(); a=m.get('audit') or {}; s=m.get('state_operation_cost') or {}
        if int(a.get('queue_count') or 0)==0 and int(m.get('active_permits') or 0)==0:
            return time.time()-t0, m
        time.sleep(0.2)
    return time.time()-t0, metrics()
def counters(m):
    a=m.get('audit') or {}; s=m.get('state_operation_cost') or {}
    return {
      'audit_q': a.get('queue_count'), 'audit_age': a.get('queue_age_ms'), 'audit_inflight': a.get('in_flight_count'),
      'j_appends': s.get('operation_journal_appends_total'), 'j_syncs': s.get('operation_journal_syncs_total'),
      'j_coal': s.get('operation_journal_coalesced_total'), 'j_batchmax': s.get('operation_journal_batch_max'),
    }
waited,m0=wait_idle()
before=counters(m0)
t0=time.time()
r=subprocess.run([sys.executable,'scripts/perf-reliability/issue28/run.py','--connection-file',conn,
 '--scenario','X1.concurrency','--protocol','json-pipeline','--rounds','1','--warmup','1','--samples','3','--out',out],capture_output=True,text=True)
el=time.time()-t0
m1=metrics(); after=counters(m1)
print('out',out,'harness_rc',r.returncode,'wall %.2fs'%el,'wait_idle %.2fs'%waited)
for line in r.stdout.strip().splitlines()[-5:]: print(' ',line)
print(' before',json.dumps(before)); print(' after ',json.dumps(after))
