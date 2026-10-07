# Provision project key: independent verification

- **ACCEPT this frozen provisioner change within the offline scope below.** Verifier C is not the author; no live API, SQL, S3, Go/Cargo/Docker build, fixture provisioning or Unknown database access.
- Frozen `prototypes/v1/stress/provision.py` SHA256 `13dffa14eea6b6d4cc809654e7e86b8d3c02612c183a807a39ee4199e20d592a`.
- Frozen helper `prototypes/v1/stress/provision_tests.py` SHA256 `ed527737a28dd900de3bf3bd68d1541be5330fe3b172618836998b0de95f0145`; unchanged helper identity.
- Independent Python3.12.8 `provision.py --self-test`: actual exit0, 19 existing + 1 new = **20 unique PASS**, 0 failures/errors; both socket-forbidden suites report network_calls=0.
- New test runs actual provision/output serialization with a mock API and its own temporary directory; verifies primary project-admin management/API key equality, no bootstrap-global key in output, independent noisy project credentials, tenant isolation and exactly ten mock calls. This is not a real service authorization test.
- Static production diff changes only emitted management_key from bootstrap-global to the issued primary project-admin key. Global remains internal bootstrap authority; server authorization is untouched. Endpoint, verified CA, worker/process metadata, migration selection, output guards and noisy-project construction are unchanged.
- Independent `python3.12 -m py_compile` for both frozen sources exits0; PYTHONPYCACHEPREFIX points into the verifier's private directory, not the repository.
- Evidence directory `/tmp/peri-provision-project-key-independent.cN10eY`; `before.json` and `after.json` identical SHA256 `a065e6ce89caa890f370cef4ab29424de5ff08166e9528b0e2dbf902202e0e9b`.
- Standalone two-source `source-manifest.json` SHA256 `a29a3151138ac2d2b3546749aa0fc4c6676e70f1bb7fa2c00df95cfc1958c3a0`; explicitly bind these inputs for R12, not inherited R11 coverage.
- Raw `self-test.log` SHA256 `bda2f1fddfbabad60a2ea503b2fb1d76f7eb199be29a0883ec0394a6b1b2268b`; commands/exits/names in `results.json` SHA256 `911c023007f29dd63f9be1a10b4d68227485520d969757b3f221a6f308796ddc`.
- Author's old-policy RED exit1 remains `/private/tmp/peri-provision-project-key.quz6PL31/red.log`, SHA256 `2acd947aad1ba9512841bf0196faa19c7694c78c9d185fe63232f2dc161e5fa0`; not rerun or relabeled by C.
- No live least-privilege checkpoint success, Native drain, historical root-cause fix, R12 workload, performance or whole-project PASS is inferred. Only this report/private evidence written; production/helper source unchanged through verification; commit/next-run authorization belongs to main.
