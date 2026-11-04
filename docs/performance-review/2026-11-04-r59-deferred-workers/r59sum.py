import json,sys
for p in sys.argv[1:]:
    s=json.load(open(p+'/summary.json'))
    print('==',p)
    for c in s.get('concurrency') or []:
        print('  c%-2s n=%3d median=%7.2f p95=%8.2f p99=%8.2f max=%8.2f err=%s 429=%s verdict=%s'%(c['level'],c['requests'],c['median_ms'],c['p95_ms'],c['p99_ms'],c['max_ms'],c['errors'],c['status_429'],c.get('verdict')))
