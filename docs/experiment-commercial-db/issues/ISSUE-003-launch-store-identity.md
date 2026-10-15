# ISSUE-003: 正式夹具启动器未复用完整对象存储归属检查

- 状态：代码已修复并通过离线单测；**待冻结/正式验证**（2026-10-15，未执行 `launch`，没有证据表明已发生容器替换或凭据泄漏）。
- 严重性：P0／正式夹具的容器归属与凭据边界。
- 位置：`trial-ground/trialctl.py` 的 `launch` 仅核对容器 Running 与 Compose project 标签，就创建 run 目录、把 S3 凭据传给将启动的全栈；同文件 `preflight_store` 已要求 service、oneoff、digest-pinned image、回环端口、独立卷、资源限额及凭据身份。
- 根因：启动路径没有调用严格归属检查；单独运行 `preflight` 不能保证启动当刻仍是同一容器。更弱的检查允许错误 service/image/端口/卷在获认可的同名 project 下进入启动路径。
- 修复验收：`launch` 在任何 run 目录或进程创建前对夹具调用完整 `preflight_store`，失败 fail-closed；离线测试证明错误归属不调用 dev launcher 且不创建 run 目录，成功的 mock fixture 仍可以到达原入口。不要绕过 `main_ready`、改动私有证据或启动真实服务。
- 委派范围：`trial-ground/trialctl.py`、`trial-ground/tests/test_trialctl.py`；正式负载仍因 ISSUE-002 的外部门禁 HOLD。
- 处理记录：subagent 将 `launch` 的旧弱检查换成 `preflight_store` 严格验证；离线 mocked 归属异常均在创建运行目录之前拒绝。主流程检查资源限额（512 MiB、0.5 CPU、64 PID）并独立运行 Python 单测 18/18 PASS；ISSUE-002 仍未解除。
