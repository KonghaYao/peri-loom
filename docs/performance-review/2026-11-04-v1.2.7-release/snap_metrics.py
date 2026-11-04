import json, sys, urllib.request
doc = json.load(open(sys.argv[1]))
req = urllib.request.Request(doc['endpoint'] + '/v1/metrics',
                             headers={'Authorization': 'Bearer ' + doc['global_api_key']})
with urllib.request.urlopen(req, timeout=10) as response:
    result = json.load(response)['result']
open(sys.argv[2], 'w').write(json.dumps(result, indent=2, sort_keys=True))
