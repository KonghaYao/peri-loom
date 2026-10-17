#!/bin/sh
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
LAB=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["lab"])' "$HERE/../identity.json")
cp "$HERE/01-go-bench_test.go" "$LAB/prototypes/v1/platform/adversarial_rw01_test.go"
cd "$LAB/prototypes/v1/platform"
export GOMAXPROCS=2 GOPROXY=off GOSUMDB=off
for round in 1 2 3 4 5 6; do
 case "$round" in 2|4|6) export RW01_ORDER=reverse;; *) export RW01_ORDER=forward;; esac
 printf '\nround=%s order=%s\n' "$round" "$RW01_ORDER"
 go test -run '^$' -bench '^BenchmarkAdversarial$' -benchmem -benchtime=250ms -count=1 .
done
