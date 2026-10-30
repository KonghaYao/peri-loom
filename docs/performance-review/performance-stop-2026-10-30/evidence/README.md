# 封存证据 ZIP（v1.1.22 Release Assets）

本目录在仓库中只保留说明。6 个原始证据 ZIP 作为 **v1.1.22 GitHub Release** 资产上传，
保持原字节，不在仓库内重复存储。

| 文件 | 字节 | SHA-256 | 用途 |
|---|---:|---|---|
| `peri-loom-r15-recovery-evidence-qualified-v1.zip` | 12,816,400 | `372743e0bceb6fa42526a0488f495bf5cf0ea40fbc3852c9340617b0733bb507` | 合格诊断；性能 **不被接受** |
| `peri-loom-r15-verified-targeted-v2-evidence.zip` | 8,625,228 | `a36dcc99cd1258bc35462908bf7e7f474eb5b15c6b796f628bb82dad773fb7e2` | 已接受的定向证据；组合候选未晋升 |
| `peri-loom-r15-json-and-cpu-evidence-v3.zip` | 7,288,599 | `73ff183ba44fb3cc39866dfb9633be7335d769562f9d68114e6926382156c802` | JSON-only gate、功能测试与限定 CPU/阶段诊断 |
| `json-wire-private-preflight-v1-capsule.zip` | 734,167 | `76b2ac89b4fbc16705f8299986d3df4bbea624f3a531ed18c135a8e9374f4f19` | JSON wire 功能证据 |
| `json-wire-private-gate-v1-capsule.zip` | 2,727,066 | `2de26ae5f9aeb097fab208ed55397a1e6733334cd5010c71242688d94f20dd42` | 已接受的 JSON wire 对照；目标未达成 |
| `owned-first-functional-capsule.zip` | 1,365,939 | `c04eb21a4f77f3d2ab409d03135dcd2775c5337cf11c000437edc996dc9ce795` | owned-first JSON/protobuf **仅功能**，无性能结论 |

校验方式：下载后对照 `../FILE-MANIFEST.json` 中的 `bytes` 与 `sha256`；不要解压后重新打包。

`qualified-v1` 含未被接受的诊断，必须阅读其原 README，不得作为 1.5× 成果或性能达标证据。
