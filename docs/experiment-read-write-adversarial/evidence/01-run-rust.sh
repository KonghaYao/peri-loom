#!/bin/sh
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
python3 "$HERE/01-prepare-rust.py"
LAB=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["lab"])' "$HERE/../identity.json")
cd "$LAB/rw01-rust"
export CARGO_BUILD_JOBS=2
if [ -f "$HERE/01-rust-Cargo.lock" ]; then cp "$HERE/01-rust-Cargo.lock" Cargo.lock; fi
cargo build --release --offline
./target/release/rw01-parser
cp Cargo.lock "$HERE/01-rust-Cargo.lock"
