import json, pathlib, shutil, hashlib
root=pathlib.Path(__file__).resolve().parents[3]
evidence=root/'docs/experiment-read-write-adversarial/evidence'
lab=pathlib.Path(json.loads((evidence.parent/'identity.json').read_text())['lab'])
for relative,addition in [('src/io.rs','\n#[cfg(test)]\nmod experiment02;\n'),('src/recovery/tests.rs',(evidence/'02-recovery-test.rs').read_text()),('src/database.rs',(evidence/'02-maintenance-test.rs').read_text())]:
    target=lab/'prototypes/v1'/relative
    text=target.read_text()
    if 'rw02_' not in text and 'mod experiment02;' not in text:
        target.write_text(text+addition)
shutil.copyfile(evidence/'02-io-tests.rs',lab/'prototypes/v1/src/io/experiment02.rs')
print('prepared',lab)
