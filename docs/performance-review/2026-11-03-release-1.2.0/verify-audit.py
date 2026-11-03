import hashlib, json, sys
prev = "0" * 64
seq = 0
n = 0
zero = b'0' * 64
for line in sys.stdin.buffer:
    line = line.strip()
    if not line:
        continue
    idx = line.rfind(b',"hash":"')
    payload = line[len(b'{"payload":'):idx]
    obj = json.loads(line)
    digest = hashlib.sha256(payload).hexdigest()
    if digest != obj["hash"]:
        print("hash mismatch at", seq + 1, file=sys.stderr); sys.exit(1)
    if obj["payload"]["previous"] != prev:
        print("previous mismatch at", seq + 1, file=sys.stderr); sys.exit(1)
    if obj["payload"]["sequence"] != seq + 1:
        print("sequence mismatch at", seq + 1, file=sys.stderr); sys.exit(1)
    prev = digest
    seq += 1
    n += 1
print(json.dumps({"events": n, "last_sequence": seq, "last_hash": prev}))
