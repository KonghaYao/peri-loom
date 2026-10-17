#!/usr/bin/env python3
"""Freeze full V1 working tree and install bytes into a build lab with fresh mtimes."""
import argparse, datetime, hashlib, io, json, os, pathlib, subprocess, tarfile
parser=argparse.ArgumentParser();parser.add_argument('--output',type=pathlib.Path,required=True);parser.add_argument('--lab',type=pathlib.Path,required=True);args=parser.parse_args()
repo=pathlib.Path.cwd();args.output.mkdir(parents=True,exist_ok=False)
files=subprocess.check_output(['git','ls-files','-z','--cached','--others','--exclude-standard','prototypes/v1','rust-toolchain.toml']).decode().split('\0')
files=sorted(set(f for f in files if f and (repo/f).is_file()))
data={f:(repo/f).read_bytes() for f in files};manifest={f:hashlib.sha256(v).hexdigest() for f,v in data.items()}
for f,sha in manifest.items():
 if hashlib.sha256((repo/f).read_bytes()).hexdigest()!=sha:raise SystemExit('source changed: '+f)
blob=json.dumps(manifest,sort_keys=True,indent=2)+'\n';source=hashlib.sha256(blob.encode()).hexdigest()
(args.output/'source-manifest.json').write_text(blob)
archive=args.output/'source.tar.gz'
with tarfile.open(archive,'w:gz') as tar:
 for f,v in data.items():
  info=tarfile.TarInfo(f);info.size=len(v);tar.addfile(info,io.BytesIO(v))
patch=subprocess.check_output(['git','diff','HEAD','--','prototypes/v1']);(args.output/'tracked.patch').write_bytes(patch)
meta={'source_id':source,'head':subprocess.check_output(['git','rev-parse','HEAD']).decode().strip(),'files':len(data),'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'tracked_patch_sha256':hashlib.sha256(patch).hexdigest(),'harness_sha256':manifest['prototypes/v1/tests/local_performance.rs']}
(args.output/'source.json').write_text(json.dumps(meta,indent=2)+'\n')
for f,v in data.items():
 target=args.lab/f;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(v);os.utime(target,None)
print(json.dumps(meta))
