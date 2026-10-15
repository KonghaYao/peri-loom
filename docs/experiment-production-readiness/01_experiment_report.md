# 实验 1：本地卷异常中断后原卷重启

执行时间：2026-10-15（Docker Desktop/OrbStack 本地 Linux daemon）。结论：**安全拒绝通过，可用性未通过**。一次已确认的 SQL 写入在 `SIGKILL` 前可读；异常终止后原容器、原卷直接启动，进程以 1 退出，未重新对外服务。因此不能声称该写入在重启后已通过 SQL 恢复验证。

## 固定输入与隔离

- 镜像引用：`ghcr.io/konghayao/peri-loom@sha256:901f37f2cceb62bf265072e5b0159fa86974af3440a79383ba31e351c0895333`
- 本机镜像 ID：`sha256:81ef392b7097d0474b5e1e19014b977a7b752f5e5cb86727f362127baf086102`；`linux/amd64`；运行用户 `10001:10001`。
- backend：`local`；运行目录：`/data/peri`；TLS 网关仅发布到随机 `127.0.0.1` 端口。
- 容器：`peri-verify-d740aee9652212f08e93cebd3ab446b9`；卷：`peri-verify-d740aee9652212f08e93cebd3ab446b9-data`。两者均带 `peri.verify-image.owner=d740aee9652212f08e93cebd3ab446b9` 标签。
- 资源限制：2 GiB 内存、2 CPU、128 PIDs、无自动重启、丢弃全部 capabilities、`no-new-privileges`。卷和容器是本轮随机新建，未操作既有部署。
- 私有证据目录：`/private/var/folders/d5/gpfmkm2s4sqgwz5wwnj44p500000gn/T/peri-image-verify-g9w0k895`。内含连接凭据，不能公开复制；报告只列脱敏观察值。失败容器与卷仍保留，供后续检查。

## 步骤与原始输出摘要

使用现有 `prototypes/v1/deploy/verify-image.py` 的 `Verification.prepare()`、`ready()` 创建并验证隔离资源。`ready()` 校验镜像内启动器与当前源码的哈希、运行 UID、CA/服务端证书链及回环 IP SAN；然后使用 `prototypes/v1/platform/dev.py` 的 `request()` 通过正常 TLS 与项目凭据发出 SQL 请求。写入一次随机命名表和随机值，收到执行响应后立即查询，结果与写入值相等。随后以 Docker `KILL` 杀死容器，并直接 `docker start` 同一容器，不修改标记、状态文件或卷。

| 观察点 | 原始结果摘要 |
| --- | --- |
| 初次启动 | `Running=true`；TLS `issuer_and_loopback_verified`；SQL open 有 session，execute 有响应，kill 前 SELECT 与确认写入一致 |
| `docker kill --signal KILL` 后 | `docker wait` 输出 `137`；`Running=false, ExitCode=137, OOMKilled=false, Pid=0` |
| Authority 标记 | kill 后 `/data/peri/authority/authority.lock` 字节为 `dirty\n`（`64 69 72 74 79 0a`） |
| 原容器与卷重启 | `docker start` 命令自身退出码为 0，但进程随即退出：`Running=false, ExitCode=1, OOMKilled=false, Pid=0` |
| 重启后标记 | 仍为 `dirty\n`；未被自动清理或伪装为 clean |
| 容器日志末尾 | `project stopped: owned stop proof set incomplete or unexpected` |
| 重启后 SQL | 网关进程未运行，无法进行 SQL 读回；持久数据在原卷内是否逻辑完整尚未验证 |

`docker start` 返回 0 只表示 Docker 接受启动请求，不表示服务恢复。日志还包含首次启动的 `ready` 事件，不能将其当作重启后的就绪证据。Authority 保持 dirty 且启动器拒绝不完整的 owned stop proof，符合安全拒绝预期；本轮不绕过 guard 来尝试读数。

## 可复现命令

以下从仓库根目录执行，使用新随机资源。命令仅展示操作骨架；实际 SQL 使用下方 Python 请求助手。不要在命令行或报告中打印 `connection.json`，它包含凭据。

```sh
IMAGE='ghcr.io/konghayao/peri-loom@sha256:901f37f2cceb62bf265072e5b0159fa86974af3440a79383ba31e351c0895333'
IMAGE_ID=$(docker image inspect "$IMAGE" --format '{{.Id}}')
RUN_ID=$(openssl rand -hex 16)
NAME="peri-prod-r1-$RUN_ID"
VOL="$NAME-data"
export NAME
docker volume create --label "peri.verify-image.owner=$RUN_ID" "$VOL"
docker create --name "$NAME" --label "peri.verify-image.owner=$RUN_ID" \
  --pull never --restart no --memory 2g --cpus 2 --pids-limit 128 \
  --cap-drop ALL --security-opt no-new-privileges \
  --publish 127.0.0.1::9443 \
  --mount "type=volume,source=$VOL,target=/data/peri" \
  "$IMAGE_ID" --run-dir /data/peri --backend local --listen 0.0.0.0:9443 --tls --no-build
docker start "$NAME"
# 运行下方 Python 请求助手，确认 SQL 写入及查询后，再继续：
docker kill --signal KILL "$NAME"
docker wait "$NAME"                         # 本轮输出 137
docker inspect "$NAME" --format '{{json .State}}'
docker start "$NAME"                        # Docker 命令可返回 0
docker wait "$NAME"                         # 本轮输出 1
docker inspect "$NAME" --format '{{json .State}}'
docker logs "$NAME"
```

SQL 请求助手，从仓库根目录运行；私有临时目录应保留在本机且不得公开：

```python
import importlib.util, json, os, pathlib, secrets, subprocess, tempfile, time

name = os.environ['NAME']
private = pathlib.Path(tempfile.mkdtemp(prefix='peri-prod-r1-'))
private.chmod(0o700)
def docker(*args):
    return subprocess.check_output(['docker', *args], text=True).strip()
for _ in range(240):
    if any(json.loads(line).get('ready') is True and json.loads(line).get('service') == 'local-project'
           for line in docker('logs', name).splitlines() if line.startswith('{')):
        break
    time.sleep(0.5)
else:
    raise RuntimeError('readiness timeout')
for src, dst in [('connection.json', 'connection.json'), ('keys/dev-ca.crt', 'ca.crt'),
                 ('keys/gateway.crt', 'tls.crt')]:
    subprocess.check_call(['docker', 'cp', f'{name}:/data/peri/{src}', str(private / dst)])
port = json.loads(docker('inspect', name))[0]['NetworkSettings']['Ports']['9443/tcp'][0]['HostPort']
subprocess.check_call(['openssl', 'verify', '-CAfile', str(private / 'ca.crt'),
                       '-purpose', 'sslserver', '-verify_ip', '127.0.0.1', str(private / 'tls.crt')])
cfg = json.loads((private / 'connection.json').read_text())
cfg['endpoint'] = f'https://127.0.0.1:{port}'
cfg['ca_file'] = str(private / 'ca.crt')
spec = importlib.util.spec_from_file_location('peri_dev', 'prototypes/v1/platform/dev.py')
dev = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dev)

def call(path, body):
    return dev.request(cfg['endpoint'], cfg['project_api_key'], path, body, cfg['ca_file'])

table = 'kill_probe_' + secrets.token_hex(8)
value = secrets.token_hex(16)
session = call('/v1/sql/open', {'database': cfg['database']})['session']
call('/v1/sql/execute', {'session': session,
    'sql': f"CREATE TABLE {table}(value TEXT NOT NULL);INSERT INTO {table} VALUES('{value}');"})
assert call('/v1/sql/query', {'session': session, 'sql': f'SELECT value FROM {table}'}) == [[value]]
call('/v1/sql/close', {'session': session})
print('SQL write acknowledged and read back; private evidence:', private)
```

实际执行复用 `verify-image.py` 的 `Verification.ready()`，其证书及连接校验与上述助手相同。检查标记可使用 `docker cp "$NAME:/data/peri/authority/authority.lock" "$PRIVATE_DIR/authority.lock"` 复制到私有目录后用 `od -An -tx1 "$PRIVATE_DIR/authority.lock"` 查看。**不要删除或重置 dirty 标记以求启动。**

## 局限与下一步

- 本轮仅验证异常 KILL 和原卷直接重启；未执行本轮独立的正常 TERM 对照、替代卷、备份恢复、S3 或自动接管。既有发布验收中的 clean restart 不能替代本轮对照测量。
- 进程拒绝启动使重启后 SQL 读回不可执行；这证明当前路径不可用，不证明卷中字节丢失或无损。确认写入在 KILL 前确实可读。
- Docker 本地单宿主模拟不能外推为 Linux 多主机故障行为。下轮若测试恢复，应设计有记录且不绕过 guard 的恢复程序，在新隔离资源中测量 RPO/RTO。
