import pathlib, json, hashlib, difflib, subprocess, platform, datetime, shutil
root=pathlib.Path(__file__).resolve().parents[3]
e=root/'docs/experiment-read-write-adversarial/evidence'
identity=json.loads((e.parent/'identity.json').read_text())
lab=pathlib.Path(identity['lab'])
paths=['src/io.rs','src/io/experiment02.rs','src/database.rs','src/recovery/tests.rs','src/recovery/fixture.rs','src/authority/store_scaling_tests.rs','src/io/files.rs']
hashes={}
patch=[]
for rel in paths:
    target=lab/'prototypes/v1'/rel
    name='02-final-'+rel.replace('/','-')
    shutil.copyfile(target,e/name)
    hashes['prototypes/v1/'+rel]=hashlib.sha256(target.read_bytes()).hexdigest()
    if rel in ['src/io.rs','src/io/experiment02.rs','src/database.rs','src/recovery/tests.rs']:
        before=subprocess.run(['git','show',identity['experiment_sha']+':prototypes/v1/'+rel],cwd=root,capture_output=True,text=True)
        patch.extend(difflib.unified_diff(before.stdout.splitlines(True),target.read_text().splitlines(True),fromfile='a/prototypes/v1/'+rel if before.returncode==0 else '/dev/null',tofile='b/prototypes/v1/'+rel))
(e/'02-lab-tests.patch').write_text(''.join(patch))
lock=lab/'prototypes/v1/Cargo.lock'
assert lock.read_bytes()==(e/'02-Cargo.lock').read_bytes()
env={'archived_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'identity':identity,'platform':platform.platform(),'cargo_target_dir':str(lab/'target-02'),'CARGO_BUILD_JOBS':2,'GOMAXPROCS':2,'profile':'dev test, unoptimized, debug=1; no timing inference','source_sha256':hashes,'lock_sha256':hashlib.sha256(lock.read_bytes()).hexdigest(),'tools':{}}
for command in [['rustc','--version'],['cargo','--version'],['go','version'],['uname','-a']]:
    p=subprocess.run(command,text=True,capture_output=True)
    env['tools'][' '.join(command)]={'exit_code':p.returncode,'stdout':p.stdout.strip(),'stderr':p.stderr.strip()}
(e/'02-environment.json').write_text(json.dumps(env,indent=2)+'\n')
rows=[]
for filename in ['02-frozen-raw.log','02-patched-raw.log']:
    for line in (e/filename).read_text().splitlines():
        if 'RW02 ' in line:
            row=json.loads(line.split('RW02 ',1)[1]); row['raw_file']=filename; rows.append(row)
(e/'02-results.json').write_text(json.dumps(rows,indent=2)+'\n')
print('archived',len(rows),'observations; source and lock identities verified')
