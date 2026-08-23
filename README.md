# GitHub Actions 独占出口池 · Hysteria2 多端口

把 GitHub Actions runner 当作一次性出口节点。客户端先向控制服务申请一个空闲 runner，服务器为该会话启动一个独立 Hysteria2 UDP 端口；客户端释放或 TTL 到期后，服务器立即关闭 HY2 入口、销毁对应 runner，并自动补充一个新 runner/IP。

## 数据链路

```text
浏览器
  └─ SOCKS5 127.0.0.1:30001
       └─ 本地 Hysteria2 client
            └─ www.example.com:20001/UDP
                 └─ 服务器 hysteria-session@20001
                      └─ SOCKS5 127.0.0.1:20001/TCP
                           └─ gost relay + QUIC/UDP 8443
                                └─ GitHub Actions runner gost SOCKS5
                                     └─ 目标网站
```

端口同号映射：

```text
公网 HY2 20001/UDP → 服务器内部 SOCKS5 20001/TCP → runner A
公网 HY2 20002/UDP → 服务器内部 SOCKS5 20002/TCP → runner B
...
```

UDP 和 TCP 是不同套接字，同号不会冲突。应用代理当前仅支持 TCP：gost 的 `rtcp` 隧道只可靠承载 SOCKS5 TCP；HY2 和本地 SOCKS5 都显式禁用应用层 UDP。

## 核心语义

- 一个客户端会话独占一个 runner，绝不经过 mihomo 随机负载均衡。
- `POST /v1/client/acquire` 原子占用空闲 runner，并返回本次会话专属 HY2 凭据。
- `POST /v1/client/release` 先停止 HY2，再让 runner 进入 `DRAINING`。
- runner 下一次心跳收到 `recycle` 后停止 gost、调用 `/v1/bye` 并退出。
- 未在 `CLIENT_RELEASE_GRACE` 内退出的 run 会被 GitHub API 取消；只有 GitHub 确认 run 已完成后才释放并发名额。
- TTL 到期执行相同释放流程，因此客户端崩溃也不会永久占用节点。
- 端口在 HY2 未确认停止时进入 `quarantined`，不能分配给下一代 runner，避免旧凭据跨代生效。
- HY2 凭据由服务端确定性派生，不写入 SQLite；相同 `request_id` 可安全重试并拿回同一结果。

## 两套状态机

Runner 生命周期：

```text
DISPATCHED → PENDING → IN_USE → DRAINING → DEAD
```

| 状态 | 含义 |
|---|---|
| `DISPATCHED` | 已 dispatch，等待 GitHub runner 上线 |
| `PENDING` | runner 已注册，正在安装 gost/建立隧道 |
| `IN_USE` | 隧道 ready，可以分配给客户端；不代表已被客户端占用 |
| `DRAINING` | 等待 runner 优雅退出或 GitHub 确认取消 |
| `DEAD` | runner 已确认退出，端口可在安全条件满足后回收 |

客户端会话生命周期：

```text
STARTING → ACTIVE → RELEASING → RELEASED
                  ↘ FAILED
```

`ACTIVE` 才表示浏览器正在独占使用。会话有效期间不执行 runner 软轮换；硬寿命资格检查保证所选 runner 能覆盖请求 TTL、退出时间和安全余量。

## 公网与本机接口

| 端口 | 协议 | 用途 | 公网放行 |
|---|---|---|---|
| `80` | TCP | Caddy ACME | 是 |
| `443` | TCP | HTTPS 控制 API | 是 |
| `8443` | UDP | runner 拨入 gost relay | 是 |
| `20001..20060` | UDP | 独占 HY2 客户端入口 | 是 |
| `20001..20060` | TCP | runner SOCKS5 回环端口 | 否，仅 `127.0.0.1` |
| `8787` | TCP | FastAPI 回环监听 | 否，仅 `127.0.0.1` |

`/v1/admin/*` 经公网 Caddy 固定返回 403，只能通过服务器回环或 SSH 隧道访问。

## 客户端 API

所有客户端接口使用独立的 `CLIENT_TOKEN`：

```http
Authorization: Bearer <CLIENT_TOKEN>
```

### 申请

```http
POST /v1/client/acquire
Content-Type: application/json

{
  "request_id": "客户端生成的唯一ID",
  "ttl_seconds": 600
}
```

成功返回 `201`：

```json
{
  "session_id": "...",
  "state": "active",
  "hy2_host": "www.example.com",
  "hy2_port": 20001,
  "password": "...",
  "obfs_password": "...",
  "sni": "www.example.com",
  "runner_ip": "GitHub runner出口IP",
  "expires_at": 0,
  "ttl_seconds": 600
}
```

无空闲节点时返回 `503 no_available_node`，并带 `Retry-After: 5`。网络超时应使用同一个 `request_id` 重试，不会重复占用 runner。

### 状态

```http
GET /v1/client/sessions/{session_id}
```

### 释放

```http
POST /v1/client/release
Content-Type: application/json

{"session_id":"..."}
```

返回 `202`；重复释放是幂等的。

## Runner 节点 API

这些接口只供 GitHub workflow 使用，鉴权为 `NODE_TOKEN`，可选 GitHub OIDC 第二因子：

```text
POST /v1/register
POST /v1/ready
POST /v1/heartbeat
POST /v1/bye
```

`lease_id` 由服务器生成并通过 `workflow_dispatch.inputs` 传给 runner。runner 每 5 秒心跳；回收指令随心跳返回。

## 部署

### 前置条件

- Debian/Ubuntu 公网服务器和已解析到它的域名。
- 已安装 gost relay，监听 `8443/UDP`。
- 已安装 Hysteria2，证书位于 `/etc/hysteria/server.crt` 和 `/etc/hysteria/server.key`。
- GitHub 仓库中的 `.github/workflows/exit-node.yml` 已登记并启用。
- GitHub PAT 具备 Actions 读写和仓库读取权限。
- 云安全组放行 `20001..20060/UDP`；只改服务器防火墙不足以绕过云安全组。

### 安装 gost relay

```bash
sudo TOKEN='<强随机口令>' RELAY_PORT=8443 bash install_server_gost.sh
```

### 安装控制服务

首次只启动 2 个 runner：

```bash
sudo DOMAIN=www.example.com \
  GH_OWNER=your-owner \
  GH_REPO=your-repo \
  N_TARGET=2 N_MIN=1 MAX_INFLIGHT=5 \
  bash install_control_server.sh
```

脚本会：

1. 检查 PAT、仓库和 workflow。
2. 安装控制服务到 `/opt/exit-node-control`。
3. 创建 `exitctl` 非特权服务。
4. 安装只接受 stdin JSON 的受限 root helper。
5. 以现有 `hysteria` 非特权用户运行 `hysteria-session@.service`。
6. 写入 `/etc/exit-node-control.env`（权限 600）。
7. 配置 Caddy HTTPS 并重启控制服务。

### GitHub Actions Secrets

| Secret | 值 |
|---|---|
| `CONTROL_URL` | `https://www.example.com` |
| `NODE_TOKEN` | 与服务器一致 |
| `FRP_SERVER_ADDR` | 服务器公网 IP |
| `GOST_TOKEN` | 与 gost relay 一致 |

PAT 只保存在服务器，不放入仓库 Secrets。

## 本地客户端

本机需有与服务器兼容的 Hysteria2 客户端。推荐不把 `CLIENT_TOKEN` 写在命令行，而是按提示安全输入：

```powershell
python client\exit_node_client.py `
  --server https://www.example.com `
  start --socks-port 30001 --ttl 600 `
  --hysteria "C:\path\to\hysteria.exe"
```

启动成功后浏览器代理设置为：

```text
SOCKS5 127.0.0.1:30001
```

同时启动多个实例时使用不同本地端口，例如 `30001`、`30002`。按 `Ctrl+C` 或发送终止信号会主动 release；若本机断电，服务端 TTL 自动释放。

先查看不产生变更的本地执行计划：

```powershell
python client\exit_node_client.py `
  --server https://www.example.com `
  start --socks-port 30001 --ttl 600 --dry-run
```

## 运维

健康检查：

```bash
curl -s https://www.example.com/healthz | jq
```

返回字段包括 runner 在线数、可分配数、客户端 ACTIVE 数和 inflight 数。

管理接口通过 SSH 隧道：

```bash
ssh -L 8787:127.0.0.1:8787 root@server
curl -H "Authorization: Bearer $ADMIN_TOKEN" \
  http://127.0.0.1:8787/v1/admin/state | jq
```

重要参数位于 `/etc/exit-node-control.env`：

| 参数 | 默认 | 含义 |
|---|---:|---|
| `N_TARGET` | 16 | ready runner 目标数，包含空闲和已分配节点 |
| `N_MIN` | 12 | 空闲节点软轮换水位下限 |
| `MAX_INFLIGHT` | 19 | GitHub 并发名额上限 |
| `CLIENT_TTL_DEFAULT` | 600 | 默认独占时长（秒） |
| `CLIENT_TTL_MAX` | 1200 | 单次最大独占时长（秒） |
| `CLIENT_RELEASE_GRACE` | 10 | 优雅退出宽限（秒） |
| `SOFT_LIFETIME` | 600 | 未分配 runner 的软轮换寿命 |
| `HARD_LIFETIME` | 1800 | runner 硬寿命 |
| `PORT_COOLDOWN` | 10 | HY2 已停且 runner 已退出后的端口冷却 |
| `BASE_PORT/POOL_SIZE` | `20000/60` | HY2 UDP 与内部 SOCKS5 TCP 同号池 |

## 持久化与恢复

SQLite 使用 WAL，保存 runner lease 和客户端会话元数据。HY2 密码不入库。服务器重启后：

1. runner lease 进入冷启动待确认窗口。
2. ACTIVE 会话从 root 管理的现有配置恢复 HY2 单元。
3. 未完成的 STARTING 会话保留 `request_id`；runner 重新心跳确认后，相同请求可用确定性凭据恢复。超过启动窗口仍未恢复则进入 RELEASING。
4. HY2 未确认停止的端口保持 `quarantined`，不会跨代复用。
5. GitHub 取消请求会持续重试并查询 run 状态，确认 completed 后才写 `DEAD`。

## 代码结构

```text
control-server/app/
  auth.py          NODE/CLIENT/ADMIN 三类 Bearer 鉴权 + 可选 OIDC
  config.py        环境变量与容量/TTL 校验
  models.py        runner 与客户端会话状态模型
  store.py         SQLite(WAL)、端口 free/held/cooling/quarantined
  scheduler.py     runner 超时、轮换、补位和取消确认
  sessions.py      acquire/release/TTL 与 HY2 生命周期
  hy2.py           非特权控制服务调用 root helper
  main.py          FastAPI 路由
control-server/hy2_helper.py
                    校验端口并管理 hysteria-session@.service
client/exit_node_client.py
                    本地 acquire → HY2 SOCKS5 → release CLI
.github/workflows/exit-node.yml
                    1 run = 1 runner
```

旧 `frpc-node.yml` 和 `gost-node.yml` 仅作历史手动兜底，定时触发已禁用，不能与当前自动调度同时运行。

## 限制与风险

- GitHub runner 是 Azure 数据中心 IP，不是住宅 IP，也不保证每次获得全球唯一 IP。
- 释放后会销毁 runner 并补新节点，但 GitHub 可能再次分配相同 NAT 出口 IP。
- 公开仓库 Actions 通常不按私有仓库分钟数计费，但长时间运行代理可能触发平台滥用策略，应自行评估并遵守 GitHub 条款。
- 当前浏览器代理只支持 TCP；需要 UDP 应重新设计 gost 隧道，而不能假设 SOCKS5 UDP 可穿过 `rtcp`。
