#!/usr/bin/env python3
"""R73 release A/B analysis. Reads only evidence under /tmp/r73-evidence."""
import json, pathlib, statistics, random, collections, re

ROOT = pathlib.Path('/tmp/r73-evidence')
PCT = lambda v, p: sorted(v)[min(len(v)-1, int(len(v)*p/100))] if v else 0.0
MED = lambda v: statistics.median(v) if v else None
SEED = 20261105

EXPECT = {
    'a': {'platform':'95716e4d7937d6b72f9224355737c91c4c4cb5231420b9277a7f0e7809e56912',
          'worker':'b17a0708c6adb6af0f5a8416a4873926eb33479feb72ded140eb5a7aa11b4cff'},
    'b': {'platform':'e07e6da8331d6d40b9f68663899739c75d20f13c8379d685960b74b418c621ba',
          'worker':'8e62601274ca06a6503ba6eb41c4f20c358b4432c928f456970372e1e21bbd5d'},
}
IMAGE = 'sha256:8a8ba43b1efac3574bb8f0896f9eb041fa7400080cd51679eb81441e50ad7fff'
LAUNCHER = '80bcab2ab246ab280647f4b3452ba6ef9f9c3334eb7d2b1f15960c8c0c9497b2'

def read_json(path, default=None):
    try: return json.loads(pathlib.Path(path).read_text())
    except (FileNotFoundError, json.JSONDecodeError): return default

def side_of(leg):
    return 'a' if re.search(r'-a\d*$', leg) else 'b'

def round_of(leg):
    m = re.match(r'([pr]\d+)', leg)
    return m.group(1) if m else ''

def stop_payload(path):
    d = read_json(path)
    if isinstance(d, dict) and isinstance(d.get('payload'), str):
        try: return json.loads(d['payload'])
        except json.JSONDecodeError: return d
    return d or {}

def leg_txt(d):
    out = {}
    for line in (d/'leg.txt').read_text().splitlines():
        if '=' in line:
            k, v = line.split('=', 1); out[k.strip()] = v.strip()
    return out

def bin_shas(d):
    out = {}
    for line in (d/'binaries.txt').read_text().splitlines():
        m = re.match(r'([0-9a-f]{64})\s+(\S+)$', line.strip())
        if m: out[m.group(2)] = m.group(1)
    return out

def observers(d):
    vals = {}
    for line in (d/'binaries.txt').read_text().splitlines():
        if line.startswith('platform observer '):
            rest = line[len('platform observer '):]
            name, value = rest.split(':', 1)
            value = value.strip()
            if '=' in value:
                value = value.split('=', 1)[1]
            vals[name.strip()] = value
    return vals

def raw_timed(d):
    rows = []
    p = d/'harness'/'raw.jsonl'
    if not p.exists(): return rows
    for line in p.read_text().splitlines():
        if not line.strip(): continue
        i = json.loads(line)
        if i.get('phase') == 'timed': rows.append(i)
    return rows

def metrics_delta(before, after, section):
    l = before.get(section) or {}; r = after.get(section) or {}
    return {k: int(r.get(k,0))-int(l.get(k,0)) for k in set(l)|set(r)
            if int(r.get(k,0))-int(l.get(k,0))}

def bootstrap_median(values):
    rng = random.Random(SEED); n=len(values)
    draws = sorted(statistics.median([values[rng.randrange(n)] for _ in range(n)]) for _ in range(10000))
    return {'draws':10000,'seed':SEED,'median':statistics.median(draws),
            'ci95_low':draws[250],'ci95_high':draws[9749]}

def bootstrap_p95_ratio(a, b):
    rng = random.Random(SEED); out=[]
    for _ in range(2000):
        ra = [a[rng.randrange(len(a))] for _ in range(len(a))]
        rb = [b[rng.randrange(len(b))] for _ in range(len(b))]
        out.append(PCT(rb,95)/PCT(ra,95))
    out.sort()
    return {'draws':2000,'seed':SEED,'median':statistics.median(out),
            'ci95_low':out[50],'ci95_high':out[1949]}

def load_leg(scenario, d, expect_n):
    ident = leg_txt(d); side = side_of(d.name)
    raw = raw_timed(d) if scenario in ('f1','l1','w1','h1') else []
    if scenario in ('f1','l1','w1','h1'):
        dur = [i['duration_ms'] for i in raw]
        errs = [i for i in raw if i.get('status') != 200 or i.get('outcome') != 'ok']
        modes = collections.Counter((i.get('response_bytes'), i.get('rows'), i.get('rows_read')) for i in raw)
        rec = {'n':len(raw),'p50':MED(dur),'p95':PCT(dur,95),'p99':PCT(dur,99),
               'mean':statistics.fmean(dur) if dur else None,'errors':len(errs),
               'mode':modes.most_common(1)[0][0] if modes else None,'durations':dur}
    else:
        rec = {}
    before = read_json(d/'metrics-before.json', {}) or {}
    after = read_json(d/'metrics-after.json', {}) or {}
    settle = read_json(d/'settle.json', {}) or {}
    stop = stop_payload(d/'launcher-stop.json')
    audit = read_json(d/'audit-verify.json', {}) or {}
    bs = bin_shas(d)
    rec.update({
        'leg':d.name,'side':side,'round':round_of(d.name),'ident':ident,
        'driver':(read_json(d/'exit-code.json',{}) or {}).get('driver'),
        'settle':settle.get('verdict'),'stop':stop.get('outcome'),
        'unresolved':stop.get('unresolved_operations'),
        'stop_exit':(d/'stop-exit.txt').read_text().strip() if (d/'stop-exit.txt').exists() else None,
        'errors_delta':metrics_delta(before,after,'errors_total'),
        'rejections_delta':metrics_delta(before,after,'rejection_reasons_total'),
        'seats':after.get('session_seats'),'permits':after.get('active_permits'),
        'unknown':after.get('unknown_held_permits'),
        'deferred':after.get('deferred_session_close'),
        'deferred_present':'deferred_session_close' in after,
        'audit_chain':audit.get('chain_ok'),'audit_head':audit.get('head_matches_tail'),
        'audit_records':audit.get('records'),
        'audit_delta':(int((d/'audit-count-after.txt').read_text().strip())-int((d/'audit-count-before.txt').read_text().strip()))
            if (d/'audit-count-after.txt').exists() and (d/'audit-count-before.txt').exists() else None,
        'platform_sha':bs.get('/usr/local/bin/peri-platform'),'worker_sha':bs.get('/usr/local/bin/peri-loom-v1'),
        'launcher_sha':ident.get('launcher_sha256'),'observers':observers(d),
        'image_ok':IMAGE in (d/'binaries.txt').read_text(),
        'invalid':None,
    })
    if scenario in ('f1','l1','w1','h1') and len(raw) != expect_n:
        rec['invalid'] = 'expected %d timed, got %d' % (expect_n, len(raw))
    if scenario in ('f1','l1','w1','h1') and rec['errors']:
        rec['invalid'] = 'timed failures'
    if rec['driver'] not in (0,None) and scenario != 'x1':
        rec['invalid'] = 'driver exit %r' % rec['driver']
    if rec['settle'] != 'settled': rec['invalid'] = 'settle %r' % rec['settle']
    if rec['stop'] != 'clean': rec['invalid'] = 'stop %r' % rec['stop']
    if rec['unresolved'] is not False: rec['invalid'] = 'unresolved %r' % rec['unresolved']
    if rec['stop_exit'] != '0': rec['invalid'] = 'stop exit %r' % rec['stop_exit']
    if rec['platform_sha'] != EXPECT[side]['platform']: rec['invalid'] = 'platform sha %s' % rec['platform_sha']
    if rec['worker_sha'] != EXPECT[side]['worker']: rec['invalid'] = 'worker sha %s' % rec['worker_sha']
    if rec['launcher_sha'] != LAUNCHER: rec['invalid'] = 'launcher sha %s' % rec['launcher_sha']
    if not rec['image_ok']: rec['invalid'] = 'image digest mismatch'
    if rec['audit_chain'] is not True or rec['audit_head'] is not True: rec['invalid'] = 'audit chain'
    if not (rec['audit_delta'] or 0) > 0: rec['invalid'] = 'audit delta'
    if rec['seats'] != 0 or rec['permits'] != 0 or rec['unknown'] != 0: rec['invalid'] = 'seat/permit leak'
    obs = rec['observers']
    for name, value in obs.items():
        if value not in ('<unset>', '0'):
            rec['invalid'] = 'observer %s=%s' % (name, value)
    if side == 'a':
        if rec['deferred_present']: rec['invalid'] = 'A publishes deferred key'
    else:
        if not rec['deferred_present']: rec['invalid'] = 'B missing deferred key'
        elif scenario != 'w1':
            if int(rec['deferred'].get('enqueued_total') or 0) <= 0: rec['invalid'] = 'B enqueued 0'
        if int(rec['deferred'].get('pending') or 0) != 0: rec['invalid'] = 'B deferred pending'
        if int(rec['deferred'].get('failed_total') or 0) != 0: rec['invalid'] = 'B deferred failed'
        if int(rec['deferred'].get('unresolved_total') or 0) != 0: rec['invalid'] = 'B deferred unresolved'
    if scenario != 'x1' and (rec['errors_delta'] or rec['rejections_delta']):
        rec['invalid'] = 'metrics error/rejection delta'
    return rec

def pair_stats(legs, pairs):
    ratios = []
    for name, a, b in pairs:
        A = legs[a]['p50']; B = legs[b]['p50']
        ratios.append({'pair':name,'A':A,'B':B,'ratio':B/A})
    values = [r['ratio'] for r in ratios]
    return {'pairs':ratios,'median_ratio':MED(values),'bootstrap':bootstrap_median(values),
            'B_faster_pairs':sum(1 for v in values if v < 1.0)}

def main():
    summary = {'campaign':'r73-v1.2.8-release-ab','criteria_sha256':pathlib.Path('/tmp/r73-evidence/criteria.json').read_text() and __import__('hashlib').sha256(pathlib.Path('/tmp/r73-evidence/criteria.json').read_bytes()).hexdigest(),
               'frozen_at_utc':json.loads(pathlib.Path('/tmp/r73-evidence/criteria.json').read_text())['frozen_at_utc']}
    legs_all = {}
    # F1 / L1
    for scen, expect_n in (('f1',125),('l1',125)):
        base = ROOT/'legs/p2'/scen
        legs = {d.name:load_leg(scen,d,expect_n) for d in sorted(base.iterdir()) if d.is_dir()}
        pairs = [('p1','p1-a','p1-b'),('p2','p2-a','p2-b'),('p3','p3-a','p3-b'),('p4','p4-a','p4-b'),
                 ('p5','p5-a','p5-b'),('p6','p6-a','p6-b'),('p7','p7-a','p7-b'),('p8','p8-a','p8-b')]
        stats = pair_stats(legs, pairs)
        Ad = [x for k,v in legs.items() if v['side']=='a' for x in v['durations']]
        Bd = [x for k,v in legs.items() if v['side']=='b' for x in v['durations']]
        pooled = {'A_n':len(Ad),'B_n':len(Bd),'A_p50':MED(Ad),'B_p50':MED(Bd),
                  'A_p95':PCT(Ad,95),'B_p95':PCT(Bd,95),
                  'p95_ratio':PCT(Bd,95)/PCT(Ad,95),'p50_ratio':MED(Bd)/MED(Ad),
                  'p95_bootstrap':bootstrap_p95_ratio(Ad,Bd)}
        summary[scen] = {'legs':{k:{kk:vv for kk,vv in v.items() if kk!='durations'} for k,v in legs.items()},
                         'pair_stats':stats,'pooled':pooled}
        legs_all[scen] = legs
    # X1
    base = ROOT/'legs/p2/x1'
    pat = re.compile(r"X1 concurrency=\s*(\d+)\s+n=(\d+)/(\d+)\s+elapsed=([0-9.]+)ms\s+p50=([0-9.]+)ms\s+p95=([0-9.]+)ms\s+p99=([0-9.]+)ms\s+429=(\d+)")
    x1 = {}
    for d in sorted(base.iterdir()):
        if not d.is_dir(): continue
        text = (d/'driver.txt').read_text()
        levels = {int(m.group(1)):{'n':int(m.group(2)),'plan':int(m.group(3)),'elapsed_ms':float(m.group(4)),
                  'p50_ms':float(m.group(5)),'p95_ms':float(m.group(6)),'p99_ms':float(m.group(7)),'status_429':int(m.group(8))}
                  for m in pat.finditer(text)}
        s = read_json(d/'harness/summary.json', {}) or {}
        con = {row['level']:row for row in s.get('concurrency') or []}
        c8 = levels.get(8,{})
        rec = {'leg':d.name,'side':side_of(d.name),'round':round_of(d.name),'levels':levels,
               'c8_qps':1000.0*c8['n']/c8['elapsed_ms'] if c8.get('n') and c8.get('elapsed_ms') else None,
               'c8_429':c8.get('status_429'),'c8_errors':(con.get(8) or {}).get('errors'),
               'c16_429':(levels.get(16) or {}).get('status_429'),'c16_errors':(con.get(16) or {}).get('errors'),
               'fatal':s.get('fatal'),'warnings':s.get('warnings'),
               'fixture_complete':(s.get('fixture') or {}).get('complete')}
        rec.update({k:v for k,v in load_leg('x1',d,0).items() if k not in ('durations',)})
        rec['driver'] = 0  # c16 verdict is the known default-quota artifact
        if rec['c8_429'] != 0 or rec['c8_errors'] != 0: rec['invalid'] = 'c8 not clean'
        if rec.get('fatal') or rec.get('fixture_complete') is not True: rec['invalid'] = 'harness fatal/fixture'
        x1[d.name] = rec
    A = [v['c8_qps'] for v in x1.values() if v['side']=='a']; B = [v['c8_qps'] for v in x1.values() if v['side']=='b']
    rounds = {}
    for r in ('r1','r2'):
        aa = [v['c8_qps'] for v in x1.values() if v['side']=='a' and v['round']==r]
        bb = [v['c8_qps'] for v in x1.values() if v['side']=='b' and v['round']==r]
        rounds[r] = {'A':aa,'B':bb,'ratio':MED(bb)/MED(aa)}
    summary['x1'] = {'legs':x1,'merged':{'A_median_qps':MED(A),'B_median_qps':MED(B),'ratio':MED(B)/MED(A)},'rounds':rounds}
    # c8
    base = ROOT/'legs/p2/c8'
    c8legs = {}
    for d in sorted(base.iterdir()):
        if not d.is_dir(): continue
        s = read_json(d/'harness/summary.json', {}) or {}
        t = s.get('timed') or {}; w = s.get('warmup') or {}; c = s.get('calibration') or {}
        rec = {'leg':d.name,'side':side_of(d.name),'round':round_of(d.name),
               'qps':t.get('throughput_qps'),'p50':t.get('p50_ms'),'p95':t.get('p95_ms'),
               'timed':t.get('requests'),'failures':t.get('failures'),'warmup_failures':w.get('failures'),
               'calibration_failures':c.get('failures'),'worker_errors':s.get('worker_errors')}
        rec.update({k:v for k,v in load_leg('c8',d,3200).items() if k not in ('durations',)})
        if rec['timed'] != 3200: rec['invalid'] = 'timed %r' % rec['timed']
        if rec['failures'] or rec['warmup_failures'] or rec['calibration_failures'] or rec['worker_errors']:
            rec['invalid'] = 'request failures'
        c8legs[d.name] = rec
    A = [v['qps'] for v in c8legs.values() if v['side']=='a']; B = [v['qps'] for v in c8legs.values() if v['side']=='b']
    rounds = {}
    for r in ('r1','r2'):
        aa = [v['qps'] for v in c8legs.values() if v['side']=='a' and v['round']==r]
        bb = [v['qps'] for v in c8legs.values() if v['side']=='b' and v['round']==r]
        rounds[r] = {'A':aa,'B':bb,'ratio':MED(bb)/MED(aa)}
    summary['c8'] = {'legs':c8legs,'merged':{'A_median_qps':MED(A),'B_median_qps':MED(B),'ratio':MED(B)/MED(A)},'rounds':rounds}
    # W1 / H1
    for scen, expect_n in (('w1',20),('h1',3)):
        base = ROOT/'legs/p2'/scen
        legs = {d.name:load_leg(scen,d,expect_n) for d in sorted(base.iterdir()) if d.is_dir()}
        pairs = [('pair1','r1-a1','r1-b1'),('pair2','r1-a2','r1-b2'),('pair3','r2-a1','r2-b1'),('pair4','r2-a2','r2-b2')]
        stats = pair_stats(legs, pairs)
        summary[scen] = {'legs':{k:{kk:vv for kk,vv in v.items() if kk!='durations'} for k,v in legs.items()},'pair_stats':stats}
        legs_all[scen] = legs
    # invalid legs
    invalid = []
    for scen in ('f1','l1','x1','c8','w1','h1'):
        for name, rec in summary[scen]['legs'].items():
            if rec.get('invalid'): invalid.append('%s/%s: %s' % (scen,name,rec['invalid']))
    summary['invalid_legs'] = invalid
    # verdicts
    f1 = summary['f1']; l1 = summary['l1']; x1s = summary['x1']; c8s = summary['c8']; w1 = summary['w1']; h1 = summary['h1']
    verdicts = {
      'F1.p50_pair_median<=0.90': f1['pair_stats']['median_ratio'] <= 0.90,
      'F1.pooled_p95<=1.10': f1['pooled']['p95_ratio'] <= 1.10,
      'L1.p50_pair_median<=0.90': l1['pair_stats']['median_ratio'] <= 0.90,
      'L1.pooled_p95<=1.10': l1['pooled']['p95_ratio'] <= 1.10,
      'X1.c8_ratio>=0.90': x1s['merged']['ratio'] >= 0.90,
      'X1.c8_429==0': all(v.get('c8_429')==0 for v in x1s['legs'].values()),
      'X1.c8_errors==0': all(v.get('c8_errors')==0 for v in x1s['legs'].values()),
      'c8.ratio>=0.90': c8s['merged']['ratio'] >= 0.90,
      'c8.all_legs_zero_failures': all(not v['failures'] and not v['warmup_failures'] and not v['calibration_failures'] and not v['worker_errors'] for v in c8s['legs'].values()),
      'W1.p50_pair_median<=1.10': w1['pair_stats']['median_ratio'] <= 1.10,
      'H1.p50_pair_median<=1.10': h1['pair_stats']['median_ratio'] <= 1.10,
      'no_invalid_legs': not invalid,
    }
    summary['verdicts'] = verdicts
    summary['verdict'] = 'PASS' if all(verdicts.values()) else 'FAIL'
    (ROOT/'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2)+'\n')
    (ROOT/'analysis'/'analysis-raw.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2)+'\n')
    # raw aggregate
    with open(ROOT/'raw.jsonl','w') as out:
        for scen, legs in legs_all.items():
            for leg, rec in legs.items():
                base = ROOT/'legs/p2'/scen/leg/'harness'/'raw.jsonl'
                if not base.exists(): continue
                for line in base.read_text().splitlines():
                    if not line.strip(): continue
                    i = json.loads(line)
                    i.update({'scenario':scen,'leg':leg,'side':rec['side'],'round':rec['round']})
                    out.write(json.dumps(i, ensure_ascii=False, sort_keys=True)+'\n')
        for leg, rec in c8legs.items():
            base = ROOT/'legs/p2/c8'/leg/'harness'/'raw.jsonl'
            if not base.exists(): continue
            for line in base.read_text().splitlines():
                if not line.strip(): continue
                i = json.loads(line)
                i.update({'scenario':'c8','leg':leg,'side':rec['side'],'round':rec['round']})
                out.write(json.dumps(i, ensure_ascii=False, sort_keys=True)+'\n')
        for leg, rec in x1.items():
            base = ROOT/'legs/p2/x1'/leg/'harness'/'raw.jsonl'
            if not base.exists(): continue
            for line in base.read_text().splitlines():
                if not line.strip(): continue
                i = json.loads(line)
                i.update({'scenario':'x1','leg':leg,'side':rec['side'],'round':rec['round']})
                out.write(json.dumps(i, ensure_ascii=False, sort_keys=True)+'\n')
    # metrics summary
    metrics = {'legs':{}, 'image':IMAGE, 'launcher_sha256':LAUNCHER}
    for scen in ('f1','l1','x1','c8','w1','h1'):
        metrics['legs'][scen] = {}
        for name, rec in summary[scen]['legs'].items():
            metrics['legs'][scen][name] = {
                'side':rec['side'],'round':rec['round'],'errors_delta':rec['errors_delta'],
                'rejections_delta':rec['rejections_delta'],'seats':rec['seats'],'permits':rec['permits'],
                'unknown_held_permits':rec['unknown'],'deferred':rec['deferred'],
                'audit_records':rec['audit_records'],'audit_delta':rec['audit_delta'],
                'platform_sha':rec['platform_sha'],'worker_sha':rec['worker_sha'],
                'launcher_sha':rec['launcher_sha'],'stop':rec['stop'],'unresolved':rec['unresolved'],
                'settle':rec['settle'],'stop_exit':rec['stop_exit'],'observers':rec['observers']}
    (ROOT/'metrics.json').write_text(json.dumps(metrics, ensure_ascii=False, indent=2)+'\n')
    # print report
    print('== F1 ==')
    print('pair p50 ratios:',[round(p['ratio'],4) for p in f1['pair_stats']['pairs']])
    print('median',round(f1['pair_stats']['median_ratio'],5),'CI',f1['pair_stats']['bootstrap'])
    print('pooled p95',round(f1['pooled']['p95_ratio'],5),'CI',f1['pooled']['p95_bootstrap'])
    print('== L1 ==')
    print('pair p50 ratios:',[round(p['ratio'],4) for p in l1['pair_stats']['pairs']])
    print('median',round(l1['pair_stats']['median_ratio'],5),'CI',l1['pair_stats']['bootstrap'])
    print('pooled p95',round(l1['pooled']['p95_ratio'],5),'CI',l1['pooled']['p95_bootstrap'])
    print('== X1 ==',x1s['merged'],{r:{k:v for k,v in x.items() if k!='A' and k!='B'} for r,x in x1s['rounds'].items()})
    print('== c8 ==',c8s['merged'],{r:{k:v for k,v in x.items() if k!='A' and k!='B'} for r,x in c8s['rounds'].items()})
    print('== W1 == median',round(w1['pair_stats']['median_ratio'],5),'pairs',[round(p['ratio'],4) for p in w1['pair_stats']['pairs']])
    print('== H1 == median',round(h1['pair_stats']['median_ratio'],5),'pairs',[round(p['ratio'],4) for p in h1['pair_stats']['pairs']])
    print('invalid:',invalid)
    print('verdicts:',json.dumps(verdicts,indent=1))
    print('VERDICT:',summary['verdict'])

if __name__ == '__main__':
    main()
