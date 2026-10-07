# Guard profiles independent verification

- Decision: ACCEPT the four frozen Python paths; RELEASE the verifier window. No production/test source edits.
- Independent executor: F; explicit Python 3.12.8, `PYTHONDONTWRITEBYTECODE=1`.
- Actual command: `python3.12 -m unittest discover -s prototypes/v1/stress -p 'test_*guard*.py' -v` (absolute interpreter recorded in private result).
- Actual result: exit 0; 31 unique PASS = 25 wrapper + 6 helper; 0 failures. Author normal/importlib repetitions are not additional unique tests.
- Evidence: `/tmp/peri-guard-profiles-independent/{before.json,tests.log,exit-code,after.json,result.json}`.
- Raw test-log SHA256: `74d069363994bbe017ee412eb917464efb015670a521f7c3788ca085a53824ba`.
- All four before/after hashes match the authorized freeze exactly:
  - `prototypes/v1/stress/run_guarded.py`: `cc652a8f128bf5b1e5840f510ee21303a5378eb64a3b8912bf583ba57510189e`.
  - `prototypes/v1/stress/test_run_guarded.py`: `149c853df8c68645a9e79eb0a84dfeadc047a13ff846f62485731323a7ab87b4`.
  - `prototypes/v1/stress/guard_profiles.py`: `15c8209fee4a6cc7d09b9a07ddc4c1b55a9762fb1c6a2b1240657d3203c82c05`.
  - `prototypes/v1/stress/test_guard_profiles.py`: `2371678f530e7820826f06123ba1e2c5a10ed70e47061caa6ce420c3f94e9369`.
- Static review: exactly mixed/architecture/common; omitted selection defaults to mixed; fresh parameter dictionaries; unknown names/types and changed fixed row/seed/timeout/resource limits are rejected.
- Exact CLI parameters are validated per selected profile; no quota/deadline expansion or arbitrary profile passthrough.
- Wrapper diff changes profile selection/validation/journal metadata and formatting only; hardjoin, owned-identity signaling, sticky STOP, retained-PID/HOLD and Unknown handling remain unchanged. Existing bounded hardjoin tests passed again.
- Limits: offline unit/static acceptance only. Process/clock/signals are mocked; no real SIGUSR1 delivery, API, service, load, Rust/Go build, S3 or performance test was executed. Historical failures remain in prior evidence.

## Focused checkpoint profile follow-up
- ACCEPT/RELEASE: independent Python3.12 discovery actual33 unique PASS (26 guard+7 helper), exit0; four-input before/after match `/tmp/peri-checkpoint-profile-independent/{before,after,result}.json`. Earlier31-test acceptance remains historical, not overwritten or added to this count.
- Accepted helper SHA: `guard_profiles.py` = `c78d38d94eaf6ef312b439e11bc5fb8f0394e0ef94616fccb456a7b382748481`; `test_guard_profiles.py` = `2cd67bba71d636bc8343a1d6b02cc1cbbc7c3b6056bc89f45d972ff4983ac4af`. Wrapper575a34f4…/test5b89927b… unchanged; raw test-log SHA `44b4450a019260b03b7ba4bf77052e0c0fbd1cb9b5279dacba1dc407c11dc260`.
- Static: new fixed checkpoint equals common except cases=`checkpoint_reopen`; old three/default mixed and strict unknown-name/fresh-copy/limit contracts retained, no arbitrary parameter passthrough. No Cargo/API/load or Native outcome/algorithm change; original S3 Unknown and private NOT_REPRODUCED remain unchanged. Main must separately authorize focused actual execution.
