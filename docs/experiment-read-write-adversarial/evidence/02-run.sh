#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
EVIDENCE="$ROOT/docs/experiment-read-write-adversarial/evidence"
LAB=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["lab"])' "$EVIDENCE/../identity.json")
OUT=${RW02_OUT_DIR:-$(mktemp -d -t peri-rw02-replay)}
mkdir -p "$OUT"
unset RW02_PATCHED
export CARGO_BUILD_JOBS=2 GOMAXPROCS=2 CARGO_TARGET_DIR="$LAB/target-02"
# Changes only the independent lab; the baseline file is reset before the pair.
cp "$EVIDENCE/02-frozen-files.rs" "$LAB/prototypes/v1/src/io/files.rs"
python3 "$EVIDENCE/02-prepare.py"
cd "$LAB/prototypes/v1"
cargo test --locked --offline --lib rw02_ -- --nocapture --test-threads=1 > "$OUT/frozen.log" 2>&1
cd "$LAB"
git apply "$EVIDENCE/02-main-pread.patch"
cd "$LAB/prototypes/v1"
RW02_PATCHED=1 cargo test --locked --offline --lib rw02_file_lock_and_controls -- --nocapture --test-threads=1 > "$OUT/patched.log" 2>&1
for filter in a_rejected_append_leaves_the_validated_prefix_reusable an_overwriting_append_forces_the_next_one_to_re_derive_the_prefix a_head_is_decoded_once_per_document; do
    cargo test --locked --offline --lib "$filter" -- --nocapture --test-threads=1
done > "$OUT/existing-controls.log" 2>&1
printf 'Logs: %s\n' "$OUT"
