# R8 mixed：CLI COMPLETE / exit0，external-builder guard STOP

Date: 2026-10-07 Asia/Shanghai. Executor/author: current agent. Independent pressure verifier: F (pending).

## 最终实际结果与边界
- Main GO仅mixed first。唯一run `513e698c6a26984b`、CLI PID11790自然exit0；run/scenario均COMPLETE，C1/C4 warmup/smoke/measure六phase全部COMPLETE，elapsed92.6741335s。
- Run窗口UTC `2026-10-07T15:32:52.308607Z`→`2026-10-07T15:34:24.945775Z`（本地23:32:52→23:34:24）。单次safeprovision actualexit0，没有第二runner/第二provision。
- **Operator wrapper最终STOP_GUARD_DURING_RUN，不是全guard PASS。** 期间external cargo出现；未signal/cancel CLI、未SQLretry，原CLI自然完成。只报告harness功能记录，不自行签verification/性能PASS。
- arch6/common13未provision、未执行，仍需F独立verify+main下一stage授权。Wholelauncher50542保持，未触碰用户rootapp55987/RustFS/Docker/rawbackendkeys/bucket。

## Candidate / profile / gate / source
- Root HEAD `09c478be663fff8c90da5bcc0dc697b378d17183`；native child `ad892d4f113b2e62544bef48be6cebfd6e20da52`。Acceptedreport `docs/v1/WRITER-ADMISSION-VERIFICATION.md` SHA `74f7f4afe100b09ad6a188e7118997cf468fd20f045d693a4559002b19b65e74`，仅限定Parent acceptance。
- Native `/tmp/peri-stress-native-writer-admission-bin` SHA `084c96d45b9e40e744e17e1abb8ace27c6afb1b8a73a171c8c8230cbaed51363`。
- Go `/tmp/peri-stress-platform-writer-admission-bin` SHA `edf09e768c5845918ec007f8dbf699cd4e4f930eacf0af1d11e77f28c977b14c`；harness `/tmp/peri-stress-phase-drain-bin` SHA `9aa86b3bb2a85326c0dfedd246507fd1f0c65fb761ee2a1daa1609b1109174a8`。未build/改runtime源码。
- Fixture `/private/tmp/peri-stress-r8-writer-l3qe2qth`、OUTPUT `/private/tmp/peri-stress-r8-results-sb3fz4hf`均0700/UID501；launcher50542/UID501/start `Wed Oct 7 23:08:03 2026`与exactfixtureargv匹配。Authority50548、platform50549、worker-a50673、worker-b50674的UID/start/PPID50542/argv/binpath均实际核验。
- LISTEN实际gateway19446/platform50549；authority52475/50548、broker52476/50549、workers52477/50673与52478/50674。Private连接配置读取仅授权guard/provision/runner，不输出credential。
- ProperCA TLS1.3实际GET metrics成功；main whole-smoke11checks PASS/exit0为main attribution，文件SHA `9aa0eee8edec5fe7c5cbd67ae91146af4337fa177d4070bf6ef5bd3fe4ad1f8f`，实际pass=true/backend=external_s3_fixture/tls=certificate_verified。未重跑smoke。
- Actualplatform config/launch writer_admission=true；两个Worker只读privatev2 profile_status均protocol2/writer_profiletrue/max_inflight128/control16/sql112/general110/owner1/cleanup1。Public max_concurrent4：新C4 general3/owner1，非旧general4 paired提升。
- 实际gate `OUTPUT/joint-pass-round8.json` exact六字段，SHA `40d844b4558d11c4d2c3008f7e73629a6d5dd2a514b998462706697ba819fac9`；sidecar `joint-pass-round8-provenance.json` SHA `a6e2c65576c08cc2317dea15fb55247e86db6deee438a1df99e997f0874d2bff`。Samecorefields/artifact/smoke绑定实际一致；scope非finalmission/Linux/SDK/UI，普通mainoperatorattestation非签名。
- Source frozen closure仅按F四份既有manifest明确输入逐文件hash，无全盘scan；Rust/Go/Python/accepted-path内容与F全部匹配，root/submodule/artifacts/gate/sidecar/smoke/plan beforeafter一致。
- `source-before-mixed-v6.json`与`source-after-mixed.json`完整相同，SHA均 `2e21a6b71d63d8deaad6e2d1e2714f4f2d1edf4ae799ff591dd668dbe6ae7705`；after是CLI自然退出后的只读作者核查，不是load中修改或Fverification。
- 首次preflight仅用户CLAUDE dirty；实际run冻结status另含main docs/v1/PROGRESS-2026-10-07.md dirty、USER-PATH-WRITER-ADMISSION.md untracked与本轮08docs。已真实记录、不修改这些用户/main文件；不假称全仓clean。

## 精确执行命令
```sh
/Users/konghayao/.pyenv/versions/3.12.8/bin/python3 prototypes/v1/stress/provision.py --execute \
  --run-dir /private/tmp/peri-stress-r8-writer-l3qe2qth \
  --output /private/tmp/peri-stress-r8-writer-l3qe2qth/stress-r8mixed.json --label r8mixed
/tmp/peri-stress-phase-drain-bin \
  -connection /private/tmp/peri-stress-r8-writer-l3qe2qth/stress-r8mixed.json \
  -output /private/tmp/peri-stress-r8-results-sb3fz4hf \
  -joint-pass /private/tmp/peri-stress-r8-results-sb3fz4hf/joint-pass-round8.json \
  -cases mixed_oltp -concurrency 1,4 -warmup 2s -smoke 10s -measure 30s \
  -offered-rate 50 -rows 1000 -seed 20261007 -request-timeout 10s \
  -max-process-rss-bytes 536870912 -max-generated-bytes 268435456 -max-artifact-bytes 67108864
```

## 全部phase / counts / recorded oracles
| C /phase | Status | elapsed s | offered | attempted /ACK | admission | clientdrop /expired |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 /warmup | COMPLETE | 2.195395834 | 100 | 28 /28 | 0 | 71 /1 |
| 1 /smoke | COMPLETE | 10.361798208 | 500 | 87 /87 | 0 | 413 /0 |
| 1 /measure | COMPLETE | 30.040923209 | 1500 | 204 /204 | 0 | 1295 /1 |
| 4 /warmup | COMPLETE | 2.562827750 | 100 | 36 /12 | 24 | 61 /3 |
| 4 /smoke | COMPLETE | 10.548669917 | 500 | 173 /47 | 126 | 324 /3 |
| 4 /measure | COMPLETE | 30.818977250 | 1500 | 506 /129 | 377 | 991 /3 |
- Totals4200offered、1034attempted/completed、507ACK、527typedHTTP429 admission/quota_exceeded、3155clientdrops、11queueexpired。Unknown/cleanup_unknown/timeouts/correctness_failures各0；1034实际businessIDs唯一、15sessioncleanup全部无failure。
- Admission是记录的真实negative，不能写全部请求成功/零error。冻结harness在任何case对class=admission不stop（phase.go契约），所以mixed仍COMPLETE；不是527个SQL Busy或unknown。Plan未声明把mixed admission算saturation coverage，不用它消除guard问题。
- ACK369read+138write；write唯一IDs138（mixed写任务无rows-hash的ACK记录）；preflight4 mutations，最终142ledgerrows匹配138+4。实际记录readback不是仅ACK推断。
- Recorded oracles：preflight4rows/sum100000004；postC1smoke38rows/sum100000002；postC4smoke107rows/sum99999982；最终142rows/sum99999960。四次statusPASS、expected/actual account与ledgerhash各一致；F独立重构仍pending。
- Final account hash `e1bea384c0acdf89d7587b9e1699ab0bff9974d4d5181510db761946cf1fa7c3`；final ledgerhash `25ea3acb05efaf557168343d2232b40ee3ab7d8c37a901e9050e90ca20fc0bd6`。
- C1measure recordedACKrate6.790736709/s、ACKworkflowp50/p95/p99=29.028583/581.043750/652.923708ms；C4=4.185732672/s、827.880625/1702.345625/2298.469708ms，均含原phase自然drain的rate、latency不含clientqueue。仅该confounded单次描述，不是paired效益/最大容量。

## Builder STOP、clock、awake与cleanup真实性
- Preflight3次连续短sample无activecompiler；既有用户esbuild55991 idle留confound。不是长quiet/host-isolation证明。Host18logicalCPUs/51539607552bytesmemory/disk约310GBavailable。
- Live guard实际触发external cargo：PID12553/PPID12519/UID501/start `Wed Oct 7 23:33:15 2026`，psCPU14.0%、RSS140361728bytes、executable `/Users/konghayao/.rustup/toolchains/stable-aarch64-apple-darwin/bin/cargo`。未取其env、未kill、不猜属于main/F或其build目标。
- **执行偏差：wrapper没把external-builder STOP传入frozenCLI的stopOffers（无该外部接口）；异常后只wait自然exit，因此CLI继续后续C4 phases，不只是已admitted peers的bounded drain。** 不把这称符合首guardSTOP；禁止后续provision，交main/F决定本功能记录可采范围。
- Wrapper异常同时退出周期guardloop：总7saved samples（3preflight+4awake），live awake只保存到UTC15:33:08.271585，后约76.7s没有持续host/awake samples或即时最终audit。Caffeinate仍live直到CLI返回；其alive不能补missingassertions。
- Own caffeinatePID11656/PPID11537/UID501/start `Wed Oct 7 23:32:48 2026`，argv `/usr/bin/caffeinate -i -t 3600 -w 50542`；已实际核PreventUserIdleSystemSleep断言4次。CLI退出后finally核exactUID/PPID/start/argv再SIGTERM，actualexit-15。事后ps确认11656与11790gone；只signal自己caffeinate。
- 作者事后clock audit2224windows=1run+6phase+1183RPC+1034workflows，maxabs(wall−mono)=36.9655ms，无倒退/差>1s窗口；这不补周期guard缺口/forcedsuspend安全证明。
- SafeOS before与事后run窗口powerd Sleep/Wake/DarkWake限定查询实际exit0、无选中events；仅selectedsubset，不宣称OS从未sleep/全部事件覆盖。未sleep/改用户pmset/keychain。
- Harness71resource samples一直到UTC15:34:24.551619，sampledownedRSSmax41353216bytes<512MiB；generatedrequestcharged355190bytes<256MiB，rawoperations3108893bytes/run.json12724bytes<64MiBartifactcap。均sample/counter非hardquota/nativeallocator峰值/S3持久体积证明。
- global/outputlocks实际absent；launcher50542仍originalUID/start/fixtureargv，未shutdown它或删除新bucket。No extraSQL/新label/arch/common/fault/SDK/UI。

## 原HOLD与local-wrapper历史保留
- 原main扩展gate含scope等六extrafields：preflight静态schemaHOLD，未provision/load；main仅迁移runtimeJSON六字段并保留侧证，非priorSQL/loadfailure。
- Local executor pre-provision attempts均保留：v1漏放行Fmanifest声明的3个/tmp输入；v2系统Python3.9/LibreSSL无TLS1.3导致握手失败；v3metricsGET合法request_id空串被guard误判；v4externalbuilderHOLD未保存triggerPID，不能归因；v5logshow元数据对象被误当sleep事件，后来仅接受actualeventMessage纠正。
- 以上未启动压力/SQLprovision；改用Python3.12/OpenSSL及修正localguard，并非SQLretry/原请求重放。V6仅一次实际provision/CLI；所有preflightresult/raw保留，不洗wrapperfails。
- R7 a22db275b5536ddf mixedFAIL60.469s/natural337ACK+1typedBusy WorkerB4.073s、arch/commonNOTRUN保持；新C4regime不与旧general4声称paired提升。

## Raw freeze / F交接
- Base `/private/tmp/peri-stress-r8-results-sb3fz4hf`；所有下列path相对于Base。作者analysis非F独立verification，下一matrix仍HOLD。
| Artifact | SHA256 |
| --- | --- |
| run_513e698c6a26984b/run.json | a3b2670aa540362a9d59e222f1477b1ad655e849357734ee68cf6e07cdaafca8 |
| run_513e698c6a26984b/operations.jsonl | 53e62417b8fa6f37bc33db5e35d4a5239cf6c3f1f507479229131090a8de32f7 |
| executor-r8mixed-result-v6.json | bdd2b4c2a31433e618bc208bcb6ec1892a2122ac84d1e57185881fecc8548600 |
| mixed-author-final-audit-v2.json | 02d84e8c04396fcc4ae683ff02fbda25e663e3c1f027f032d2a12cde38f787e7 |
| mixed-raw-freeze.json (24files) | 04c99338dd67d586becbbd2f324a4d27f465d05662e9ff8f92f065a29dd91323 |
| source-before-mixed-v6.json /source-after-mixed.json | 2e21a6b71d63d8deaad6e2d1e2714f4f2d1edf4ae799ff591dd668dbe6ae7705 |
| r8mixed-executor.py | 460bc071df012723354756ebffbd84265bb7e4e9b750787ed1c99f6312619af3 |
- Manifest还冻结originalHOLDattempts、safeOSsubset、gate/sidecar、stdout/stderr与源快照；不是签名livecapture。Runner/provision stderr各emptyhash，exactargv/actualexit在v6result，noartifact替换。
- **交接结论：mixed functional harness COMPLETE、recordedoracles PASS；operatorguard STOP与monitoringgap真实保留，整体接受/下一stage只由F+main决定。** 本作者不宣称wholepressure/fullproject/finalmissionPASS。
