# Root UI stage reporter independent gate — offline delta ACCEPT

- Non-author scope: two frozen helper/test sources, static privacy/control-flow review, syntax and fake-only Node tests; no actual UI/API/browser acceptance.
- ui-smoke.mjs153 lines SHA256 `fb242982c277395cecb1bb9dcf368487847d461963c532abca105db4a5e7d83d`; ui-smoke.test.mjs149 lines SHA256 `9384ee2cda6ed8b4e13bc889f5a2748b7dd4ec39a9d2ad08c89a6546a79f79c2`.
- Both hashes match author freeze before/after independent execution; selected-content proof only, not whole-Root/production-artifact freeze.
- Actual independent node --check for source/test each exit0; node --test independently14PASS/0fail/0skip, using mocked fetch/WebSocket and synthetic private input.
- Evidence `/private/tmp/peri-ui-stage-independent-ql9xz11g`; proof SHA256 `8d3a12c32582d62d2c38ec33d0be4750b55468fafce212adbefa0184045badd6`; raw commands/exits/log hashes retained.
- Failure stderr is one closed JSON event/stage/category; stages are literal control-flow labels, category checked against the fixed set; no raw exception/header/SQL/token/namespace output.
- Synthetic sensitive strings are absent from captured test output; malformed CDP, transport/protocol/command timeout, DOM/oracle and private-config failures stay machine-readable.
- Before normal Close submission, tested failures send0 SQL Close, at most1 open/query, no reconnect/replay/retry; first query failure survives a subsequent CDP release failure.
- CloseACK-missing/storage/release failures cannot emit success or retry Close; these later paths have one previously submitted normal Close, not a failure-triggered cleanup Close.
- Healthy fake path submits one open/four queries/one Close and checks refs/storage; reported complete fields are helper assertions, not independent actual wire/model oracles.
- CDP target release/socket close is browser-transport cleanup only, not SQL Close, Native drain, rollback or resolution of a potential Unknown session.
- Timer/pending cleanup and first-failure preservation were reviewed; existing page source has no unload/pagehide cleanup hook found in inspected app/rpc files.
- This bounded reporter gate does not strengthen public SDK Close semantics, prove private-input ownership/TOCTOU hardening, or establish TLS/runtime/browser correctness.
- R14/R15 UI actual failures, audit absence/no-dispatch uncertainty and dangling-lock evidence remain immutable; static earlier catalog defect is not retrospectively assigned as actual first cause.
- SDK known-success facts remain separate; P4 original FAIL and later NOT_REPRODUCED remain qualified; performance NOT PASS and parent joint/UI acceptance HOLD.
- No project HTTP/SQL,CDP/Chrome launch,API probe,source edits,old Unknown operations,production build or commit; only independent fake processes and this report added.
- Decision: ACCEPT frozen offline reporter delta only; private v4 whitelist capture requires its own gate, and every actual fresh UI case requires explicit main GO.
