# CLI caller safety — independent bounded codegate

Result: **PASS for the frozen CLI/SDK mock codegate only**; no real-service API, SQL, Native gate or latest Linux image acceptance is inferred.
- Independent verifier made no production-source edits. All five author full SHA256 values matched before and after execution; observed HEAD stayed `362ac96dfbfd3caff924e9f5305282ffe33ec592`.
- `cmd/peri/main.go`: `e24520ab6e3f1e8de0f028061a96995d524fb91bd84f2d1bd95dbdc5c98e183f`.
- `cmd/peri/main_unknown_cleanup_test.go`: `1437681981cb4103c9e6222ac5ee970a6642eb1afc9a5d8de7fe62b8652838ca`.
- `cmd/peri/main_test.go`: `5d4f3649cf61b1ba71047e75f287b3a136453e23042d79a6b264cee79f55b0f0`.
- `cmd/peri/shell.go`: `c7f37b3afdea9d4058d572df3de0c4a90547dc9f59598645e7712a0fc23f790f`.
- `cmd/peri/shell_unknown_cleanup_test.go`: `622bcf2287c2bb181d1c9206b8ff5ec5df079bb8ec33f4a0b7919aa8c0c130eb`.
- Paths above are relative to `prototypes/v1/platform`; `credentials.go`, SDK sources and required module/toolchain inputs are included in the closure manifest, not omitted from verification.
- `go test -json -count=1 ./cmd/peri ./sdk/go`: exit0; CLI **6 roots / 43 leaves**, SDK **7 roots / 19 leaves**, zero failed or skipped tests.
- Same command with `-race`: exit0 and identical unique counts. Do not sum repetitions or raw parent/subtest events as new independent cases.
- `go vet ./cmd/peri ./sdk/go`: exit0; package gofmt and scoped diff checks clean.
- `go build -trimpath -o ARTIFACT/peri-cli ./cmd/peri`: exit0; immutable host CLI mode **0500**, path `/private/tmp/peri-cli-caller-safety-verify.4XwQipUo/peri-cli`.
- CLI SHA256: `26f7a378f3b1b6cda38d329308adc99285478e197141b9e6ae5d5f7d96b61291`; host/toolchain/build metadata is retained separately. This executable is not a Linux image binary.
- Closure captures 152 Go-listed packages / 1006 source, target-test/embed, module, include-header and toolchain inputs; before/after manifests are byte-identical, SHA256 `7100b6729f7135666027e5544ca40470454bc9d653520fbdb603dc0ac6e970e9`.
- Closure is complete for the listed Go source boundary, not a claim that external system libraries or the C compiler SDK are hermetic. It is not the historical whole-module 135-file claim.
- Static review plus mock assertions prove true unknown stops immediately without automatic Close, SQL retry or reconnect Open; malformed/lost ACK paths retain their original failure/correlation.
- Only typed `session_moved` with `OutcomeUnknown=false` permits explicit local discard and Open; the old session receives no Close. The original moved UX remains opens2/queries2/executes1, with closes1 strengthened.
- Healthy EOF/quit closes once; failed or missing Close ACK is returned, not reported as successful cleanup. No unknown outcome is declared resolved by these tests.
- SDK Go's 8 tracked files match the pre-CLI-fix `bd325e3` filtered source snapshot. No production SDK, server, Auth or Native change was made by this verifier; historical c252/64ec runtime provenance is not a fresh global gate.
- Evidence: `/private/tmp/peri-cli-caller-safety-verify.4XwQipUo` (`normal.jsonl`, `race.jsonl`, `inputs-before.json`, `inputs-after.json`, `test-counts.json`, `evidence.sha256`). All local independent commands passed; earlier author over-stop FAIL remains separate historical evidence, not erased.
- Latest Native/P4 remains HOLD per main. The old Linux image remains superseded; any final delivery requires newly frozen context/tag containing this CLI, with actual cache reuse determined by source inputs.
