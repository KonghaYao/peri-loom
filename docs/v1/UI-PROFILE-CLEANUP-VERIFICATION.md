# UI Profile Cleanup — Independent Scoped Verification

- Offline verdict: ACCEPT the frozen guards/mock evidence only; actual execution is NOT authorized or performed by F. Actual cleanup must remain a separate stage in this report after main GO and read-only evidence audit.
- Freeze `/private/tmp/peri-ui-lock-cleanup-dev.thxf47/freeze.json`, SHA256 `84e43df773fddf700fb19573ca09051bbdb544641f57ea4f14329b16f5c78850`.
- Private sources: `cleanup.py` (225 lines), SHA256 `0665b4b17f6106e3a6a22f697dc3b42b86d29cf219f7ee3f72c536aec7a3452e`; `test_cleanup.py` (167), SHA256 `85203dcaf655804c16f562e11a4ce1feac32613fbc132097caacc43293e85eec`.
- Actual F Python 3.12.8 unittest discovery: **17 PASS, 0 FAIL/errors**, body 0.086 seconds, exit 0; syntax compilation exit 0 with private cache outside the frozen source directory.
- Every test patches actual `os.unlink` to forbid deletion; successful deletion is FakeBackend-only. No actual signals, PID probe, UI, SQL/API or cleanup invocation was performed.
- F hashed two sources and all three pinned original proof files before/after: unchanged. Target `SingletonLock` remains lexists true with unchanged lstat metadata; evidence maps both SHA256 `1cea97d2e63a5eb7ffafc5f4ac3bb4c8b942540e6bd2fc658adbd3ed720becfd`.
- Independent evidence `/tmp/peri-ui-cleanup-independent/`; unittest log SHA256 `6fe5dcaa2e2b7b3e1ad2e92190d4fe6e4cefb6198938bc1f3d18e99118fa298d`.
- Static guards restrict deletion to fixed own profile/PID 74178 and `SingletonLock`; pinned preparation/owner/supervisor hashes, Chrome identity/joined exit 0/twice-verified proof, canonical current-UID mode-0700 directories, own symlink/nlink/PID target, dangling status and two absent-ps checks are required.
- Proof reads reject nonregular/symlink/oversize files before opening, use NONBLOCK/NOFOLLOW, and compare fstat identity before/after. Root/profile dirfds use NOFOLLOW and exact lstat/fstat identity; two metadata snapshots compare root/profile/link identity and target before dirfd-relative unlink.
- Active/zombie/ambiguous PID, foreign ownership/path, changed inode/ctime, live target, failed proof and post-unlink uncertainty are covered by mocks. This is not actual OS signal-delivery or real filesystem race testing.
- No-argument entry returns PLAN_ONLY without filesystem inspection. Execution reserves a new 0600 O_EXCL/NOFOLLOW result before mutation and fsyncs its result; post-unlink observation failure retains delete_count 1/unknown after, not false zero/clean.
- Fixed proposed execution command, ONLY after separate main authorization: `/Users/konghayao/.pyenv/versions/3.12.8/bin/python3 /private/tmp/peri-ui-lock-cleanup-dev.thxf47/cleanup.py --execute --output /private/tmp/peri-ui-lock-cleanup-dev.thxf47/cleanup-result.json`. That output did not exist at offline audit.
- POSIX final metadata-check-to-unlink is not atomic against a malicious concurrent same-UID actor. This scoped guard does not claim that stronger property or generic caller-selected cleanup safety.
- Original v5 supervisor exit 1/lock-true history remains unchanged; cleanup cannot turn that UI/modeltrace run into PASS. The cleanup result's historical exit field is a fixed annotation, not a new independent proof of the original supervisor exit.
- Actual cleanup: NOT RUN in this offline gate. Pending separately authorized executor evidence; no production source or original raw proof was edited by F.

## Subsequent Actual Cleanup — Read-Only Audit
- Main separately authorized/executed exactly one cleanup and reported exit 0/`OWN_LOCK_REMOVED`; F did not execute cleanup. Result `/private/tmp/peri-ui-lock-cleanup-dev.thxf47/cleanup-result.json` is 0600, SHA256 `08f6edbb6b050c1f16971cb8d1955e193612ed935dbae26e2592e342fdce2df5`.
- F independently matched recorded before=true, after=false, delete_count=1, owned_pid=74178 and retained original_supervisor_exit=1/original_lock_true_retained=true; current `lstat` returns FileNotFoundError and lexists=false for the exact fixed lock path.
- Both frozen source hashes and all three original pinned proof hashes remain unchanged. No rerun, API, signal, UI operation or original R17 report/modeltrace edit was performed by F.
- Final verdict: limited ACCEPT of this later own-lock cleanup/result/current absence, not a retroactive UI PASS or independently proven original exit; executor exit attribution remains main's record. The non-atomic same-UID stat-to-unlink limitation remains unchanged.
