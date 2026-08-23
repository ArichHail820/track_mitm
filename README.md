# GitHub Actions 出口 IP 池 · 服务器分配制(当前方案)

把 GitHub Actions 的 runner 当出口节点,但**调度权收在自己的服务器上**。服务器持 PAT,按需派发 job、分配端口、控制寿命、回收换代;runner 只负责当一个老实的租约。

穿透层沿用 gost relay+quic,数据面沿用 mihomo,都不动:

```
客户端 --hy2--> 服务器 mihomo --(127.0.0.1:200xx)--> gost relay --quic--> GHA节点 gost(socks5) --> 目标
                    ↑ 数据面,不依赖控制面                                        ↑ 由控制服务端调度
```

## 为什么要收到服务器

旧方案(下方"历史方案")的控制逻辑全写在 workflow 里,是一条**去中心化的链**:`relay` job 触发下一轮,`guard` 挡住 schedule 重复播种。四个硬伤:

1. **端口靠盲抢**。随机挑端口 + TCP 预检 / grep 日志,有竞态窗口,且没有任何一方知道全局占用。
2. **链断即全断**。`relay` 失败、被限流、run 被清掉,整个池子就死到下一个 15 分钟 schedule,最坏空窗 15 分钟。
3. **状态不可见**。服务器不知道有几个节点在线、哪个端口对应哪个 IP、某节点是活着还是僵尸。唯一的真相来源是 mihomo 的健康检查——那是数据面在猜控制面的状态。
4. **19 节点 + 1 relay 占满 20 并发**,1 个名额纯浪费;整批同时到寿只能靠 `STAGGER_SECONDS` 硬凑错峰。

新方案把这四条一次性解决:端口由服务器直接分配(无竞态)、reconcile 循环声明式收敛(无链式依赖)、状态集中可见、**20 个名额全给节点**。仓库里也**不再需要 `secrets.PAT`** —— PAT 只存在于服务器上。

## 状态机

你看到的是 3 个状态,服务器内部要 4 个:job 被 dispatch 之后、runner 真正跑起来之前,节点还在 GitHub 队列里排队,没有能力跟服务器说话,但服务器必须记账,否则会重复派单。

| 服务器内部状态 | 节点看到的 | 含义 |
|---|---|---|
| `DISPATCHED` | (还没上线,看不到) | 已调 workflow_dispatch,等 runner 上线 |
| `PENDING` | **2 待使用** `standby` | runner 已注册,正在装 gost / 建隧道 |
| `IN_USE` | **1 使用中** `in_use` | ready 已上报,端口已通,在出口池里服务 |
| `DRAINING` | **3 待回收** `recycle` | 服务器已判死刑,等节点自己退出 |
| `DEAD` | (已退出) | 终态,端口与并发名额已归还 |

```
reconcile 决定补位          → 创建 lease(uuid) + DISPATCHED
POST /register              → DISPATCHED → PENDING    (此时分配端口)
POST /ready                 → PENDING    → IN_USE     (记 in_use_since)
warmup 超时 300s            → PENDING    → DRAINING   warmup_timeout
心跳超时 15s  (IN_USE)      → IN_USE     → DEAD       lost + cancel run
心跳超时 30s  (PENDING)     → PENDING    → DEAD       lost_in_warmup
dispatch 超时 300s          → DISPATCHED → DEAD       no_show
软寿命 600s 到 且 水位够    → IN_USE     → DRAINING   rotate
硬寿命 1800s 到(无条件)   → IN_USE     → DRAINING   max_age
POST /bye                   → 任意       → DEAD       bye:<reason>
DRAINING 超 60s 不退        → DRAINING   → DEAD       drain_timeout + cancel run
```

### "ready 即使用中"和"不留空窗"怎么调和

这两条看起来冲突:如果 ready 就立刻算使用中,就没有热备了,每次回收都会实打实少一个节点。

解法不是改定义,而是**把"使用中"当成池子而不是单个租约**。数据面本来就是 mihomo 的 load-balance 组,节点可替换,不存在客户端独占某个节点。所以:

- ready → `IN_USE` → 立刻进池,**定义不变**
- 回收不做一对一 make-before-break,改成**水位保护**:只有 `count(IN_USE) > N_MIN` 时才允许因软寿命到期而回收
- 到寿但水位不足 → 顺延到下个 tick,顺延的上限就是 `HARD_LIFETIME`

于是 `HARD_LIFETIME` 有了明确语义:它不是正常轮换周期(那是 `SOFT_LIFETIME`),而是**宽限期的安全阀**,保证 IP 绝不会因为池子长期吃紧而无限期不换。软硬两层,不冲突。

`PENDING` 的真实含义也清楚了:它不是热备,是**在路上(in-flight warmup)**。

## 容量账本:这里有个死锁

GitHub 免费账号整账号 20 并发。稳态下"在路上"的节点数可由 Little's law 直接算出:

```
dispatch 速率 λ = N_TARGET / SOFT_LIFETIME
在路上的数量   = λ × T_warmup = N_TARGET × T_warmup / SOFT_LIFETIME
```

`T_warmup ≈ 45s`(dispatch 延迟 1-3s + GitHub 调度排队 5-30s + runner 启动 5-15s + 装 gost 3-10s + 建隧道 2-5s):

| N_TARGET | 软寿命 | 在路上 | inflight 峰值 | 结论 |
|---|---|---|---|---|
| 19 | 300s | 2.9 | ~22 | 超限,必然死锁 |
| 16 | 300s | 2.4 | ~18.4 | 只剩 0.6 个余量,启动会告警 |
| 14 | 300s | 2.1 | ~17 | 可行 |
| **16** | **600s** | **1.2** | **~17.2** | **当前默认**,余量充足且省一半分钟数 |

**死锁长什么样**:inflight 打满 → 新 dispatch 的 job 在队列里等名额 → 服务器因水位保护不敢回收老节点(在等新节点 ready)→ 新 job 永远等不到名额 → 全池冻结。

三条防线:

1. `MAX_INFLIGHT=19`,硬性不超发,给账号里其它 workflow 留 1 个名额。
2. 参数定档 `N_TARGET=16` + `SOFT_LIFETIME=600`,峰值 17.2,余量 1.8。
3. **反死锁逃生阀**:若 inflight 打满且有租约卡在 PENDING/DISPATCHED 超过 `STUCK_PENDING=90s`,无条件强制回收最老的在用节点来腾名额。

启动时 `config.py` 会做两级检查:数学上必然出事的组合**直接拒绝启动**(比如 19+300s),余量偏薄的组合**放行但告警**并给出建议档位。

另外节点退出前会主动打 `POST /bye`,名额归还从"等 `DRAIN_TIMEOUT` 60s 超时"变成约 0 秒,等于凭空多出一个名额的余量。

## 节点侧 API(只有 4 个)

```
POST /v1/register   {lease_id, run_id, runner_ip, rebind}
  → 200 {port, heartbeat_interval, state:"standby"}
  → 409 lease_unknown / lease_dead / no_free_port      节点立刻 exit 0

POST /v1/ready      {lease_id, port}
  → 200 {state:"in_use", soft_deadline_in, hard_deadline_in}
  → 409 port_mismatch

POST /v1/heartbeat  {lease_id, run_id, port, node_phase}      每 5s
  → 200 {action:"keep",    state:"standby"|"in_use"}
  → 200 {action:"recycle", reason}                     ← 这就是状态 3
  → 409 {action:"recycle", reason:"unknown_lease"}

POST /v1/bye        {lease_id, reason}  → 204
```

三个要点:

- **`lease_id` 是把 dispatch 和 runner 缝起来的唯一手段。** GitHub 的 `workflow_dispatch` 返回 204 且**不给 run_id**,所以必须服务器先生成 `lease_id` 作为 inputs 传下去;`run_id` 反过来由节点上报,服务器留着强杀用。
- **心跳自描述**:每次带全量身份,服务器因此可以在重启后靠心跳重建状态。
- **回收指令搭心跳的车下发**,不用第二个轮询接口,决定到节点退出的延迟 ≤ 5s。

运维接口(`/v1/admin/*`,独立 `ADMIN_TOKEN`,Caddy 只放行本机):`state` 全量快照、`ports`、`pause`、`target`、`drain/{lease_id}`、`rotate-all`。

## 服务器崩溃自愈

内存状态会在重启后丢光,而此时可能有 16 个节点还在线上跑。用 SQLite(WAL)持久化 lease,重启后:

1. 加载所有非 `DEAD` 的 lease,标记 `unconfirmed`
2. 进入 **20s 冷启动收敛窗口**:这期间**既不 dispatch 也不回收**
3. 心跳陆续回来,凭 `lease_id` 认领,端口按节点自报的实际值重新登记
4. 窗口结束时仍 `unconfirmed` 的,判 `orphan_after_restart` + cancel run
5. 恢复正常 reconcile

关键在第 2 步。少了它,服务器一重启就会看到 `in_use=0`,立刻 dispatch 一整批,而老节点还在跑——瞬间打爆 20 并发。**冷启动必须先观察再行动**,所以 `COLD_START_WINDOW` 必须大于心跳超时(配置校验会强制这一点)。

## 部署

### 前置条件

| 项 | 要求 |
|---|---|
| 服务器 | Debian / Ubuntu,有公网 IP,root 权限 |
| 域名 | 一条 A 记录指向服务器(Caddy 签 HTTPS 证书要用) |
| GitHub 仓库 | **建议公开**(Actions 免费不限量);私有仓库 16 个并行 runner 会很快吃光额度 |
| 数据面 | 服务器上已经跑着 mihomo,且 load-balance 组已列入 `127.0.0.1:20001..20060` |

> 数据面(mihomo + hy2)的安装脚本**不在本仓库**,它属于上层 `frp-hy2-pool/server` 项目。本仓库只负责出口节点池:gost relay(穿透层)+ 控制服务端(调度层)。如果服务器上还没有 mihomo,先把那部分装好再往下走。

### 端口规划

| 端口 | 协议 | 用途 | 监听者 | 是否需放行公网 |
|---|---|---|---|---|
| 443 | UDP | hy2 客户端入口 | hysteria2 | 是 |
| 443 | TCP | 控制面 HTTPS | Caddy | 是 |
| 80 | TCP | Caddy 申请证书(ACME) | Caddy | 是 |
| 8443 | UDP | gost relay,GHA 节点拨入 | gost-relay | 是 |
| 20001-20060 | TCP | 出口端口池 | 仅 127.0.0.1 | **否** |
| 8787 | TCP | 控制服务端 | 仅 127.0.0.1 | **否** |

443 的 UDP(hy2)和 TCP(Caddy)是两个不同的套接字,可以共存,不冲突。

### 第 1 步:把仓库推到 GitHub

必须先做。安装脚本会检查 workflow 是否已被 GitHub 登记——**没登记过的 workflow 不接受 dispatch**,这是最容易踩的坑。

```bash
git add -A
git commit -m "server-allocated exit node pool"
git push -u origin main
```

推完去仓库的 Actions 页面确认列表里出现了 **exit node**。没出现就不要继续。

### 第 2 步:建 PAT

GitHub → Settings → Developer settings → Personal access tokens。

- Classic:勾 `repo` + `workflow`
- Fine-grained:Actions 读写、Contents 只读、Metadata 只读

这个 PAT 只会存在服务器的 `/etc/exit-node-control.env`(权限 600),**不要**放进仓库 Secrets。

### 第 3 步:DNS

把 `ctl.你的域名` 的 A 记录指向服务器 IP,等生效(`dig +short ctl.你的域名` 能返回正确 IP)。

### 第 4 步:装 gost relay(穿透层)

```bash
sudo TOKEN=你的强口令 RELAY_PORT=8443 bash install_server_gost.sh
ufw allow 8443/udp        # 或对应的云厂商安全组
systemctl status gost-relay
```

### 第 5 步:先用单节点验证 gost 这条路走得通(别跳过)

**这一步比看起来重要。** 旧的 `gost-node.yml` 因为 heredoc 缩进错误导致整个 YAML 无法被 GitHub 解析,也就是说 **gost relay+quic 这条链路从来没有真正跑起来过**。先手动验证再让 16 个 runner 上去,否则你会在 16 份心跳日志里排查一个本来就没通的隧道。

按下方"历史方案 → gost 版 → 第 2 步"那段,在任意一台机器(本地或另一台 VPS)手动跑一个 gost 节点绑到 20051,然后在服务器上验证:

```bash
curl -x socks5h://127.0.0.1:20051 https://api.ipify.org      # 应返回那台机器的 IP
```

返回正确 IP 才继续。返回不了就是穿透层的问题,和控制服务端无关。

### 第 6 步:装控制服务端

脚本要读 `control-server/app`,所以在服务器上也要有这份代码:

```bash
git clone https://github.com/你的用户名/frpc-nodes.git
cd frpc-nodes

# 首次部署用 2 个节点跑通全链路,别一上来就 16 个
sudo DOMAIN=ctl.你的域名 \
     GH_OWNER=你的用户名 \
     GH_REPO=frpc-nodes \
     N_TARGET=2 N_MIN=1 MAX_INFLIGHT=5 \
     bash install_control_server.sh
```

`GH_TOKEN` 不在命令行给,脚本会用不回显的方式提示你输入(避免落进 shell history)。

脚本做的事:装依赖 → **起飞前检查 PAT / 仓库 / workflow** → 建 venv → 写 `/etc/exit-node-control.env` → 起 systemd(只监听 127.0.0.1)→ 配 Caddy 自动 HTTPS → 校验 Caddyfile。

结束时会打印随机生成的 `NODE_TOKEN` 和 `ADMIN_TOKEN`,**记下来**。

### 第 7 步:配置仓库 Secrets

仓库 → Settings → Secrets and variables → Actions:

| Secret | 值 |
|---|---|
| `CONTROL_URL` | `https://ctl.你的域名`(注意不要带结尾斜杠) |
| `NODE_TOKEN` | 第 6 步打印的值 |
| `FRP_SERVER_ADDR` | 服务器公网 IP |
| `GOST_TOKEN` | 与第 4 步的 `TOKEN` 一致 |

`PAT` 这个 Secret 现在可以删掉了。

### 第 8 步:验证

不用手动跑 workflow。控制服务端一启动,reconcile 循环就会自己把池子补到 `N_TARGET`。

```bash
# 1) 调度日志:应该看到 排队派发 → dispatch ok → register → ready -> 使用中
journalctl -u exit-node-control -f

# 2) 健康检查(公网可访问,无需鉴权)
curl -s https://ctl.你的域名/healthz | jq

# 3) 全量状态。运维接口公网返回 403,必须走 SSH 隧道
ssh -L 8787:127.0.0.1:8787 你的服务器
curl -s -H "Authorization: Bearer 你的ADMIN_TOKEN" \
  http://127.0.0.1:8787/v1/admin/state | jq '{by_state, active_ports, inflight}'

# 4) 服务器上确认端口真的被反向绑定了
ss -ltnp | grep ':200'

# 5) 走真实出口验证(端口号取自上面的 active_ports)
curl -x socks5h://127.0.0.1:20001 https://api.ipify.org
```

顺利的话时间线大约是:启动 → 2s 内 dispatch → 20~60s 后第一个节点 `ready` 进池。

### 第 9 步:放大到 16

前面全部正常,再调容量:

```bash
sudo sed -i 's/^N_TARGET=.*/N_TARGET=16/; s/^N_MIN=.*/N_MIN=12/; s/^MAX_INFLIGHT=.*/MAX_INFLIGHT=19/' \
  /etc/exit-node-control.env
sudo systemctl restart exit-node-control
```

重启有 20s 冷启动收敛窗口,已在线的节点不会被误杀。也可以不重启,临时调目标值(重启后失效):

```bash
curl -s -X POST -H "Authorization: Bearer 你的ADMIN_TOKEN" \
  -H 'Content-Type: application/json' -d '{"n_target":16}' \
  http://127.0.0.1:8787/v1/admin/target
```

### 排障对照表

| 现象 | 原因 | 处理 |
|---|---|---|
| 日志刷 `dispatch 失败` | PAT 权限不足 / workflow 未登记 / 分支名不对 | 起飞前检查本该拦住;核对 `GH_REF` 与默认分支 |
| 节点一直停在 `pending`,最后 `warmup_timeout` | gost 隧道建不起来 | 回到第 5 步单独验证穿透层;查 8443/udp 是否放行 |
| 节点日志 `注册被拒绝 HTTP=409` | 该租约已被服务器判死(常见于排队过久) | 正常自愈行为,服务器会补新的 |
| 节点日志 `心跳失败 HTTP=000` | 节点连不上 `CONTROL_URL` | 检查域名解析、443/tcp 放行、Caddy 证书是否签下来 |
| `/healthz` 返回 503 | reconcile 循环卡住 | 看 `journalctl -u exit-node-control` 有无异常栈 |
| `in_use` 长期为 0 但 `dispatched` 在涨 | runner 起不来或 Secrets 缺失 | 去 Actions 页面看具体 job 的日志 |
| 启动日志有 `配置提醒:并发余量偏薄` | 容量参数太挤 | 按提示降 `N_TARGET` 或加大 `SOFT_LIFETIME` |

日常运维:

```bash
systemctl restart exit-node-control      # 重启(有收敛窗口保护)
# 强制换一整批 IP(受水位保护,逐批退出,不会一次全空)
curl -s -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  http://127.0.0.1:8787/v1/admin/rotate-all
# 维护期暂停调度(不回收也不补位)
curl -s -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' -d '{"paused":true}' \
  http://127.0.0.1:8787/v1/admin/pause
```

## 参数

都在 `/etc/exit-node-control.env`,改完 `systemctl restart exit-node-control`(有冷启动收敛窗口,在线节点不会被误杀)。

| 参数 | 默认 | 说明 |
|---|---|---|
| `N_TARGET` | 16 | 目标在线节点数 |
| `N_MIN` | 12 | 水位保护下限,低于它不做自愿轮换 |
| `MAX_INFLIGHT` | 19 | 占用并发名额的租约上限,给账号留 1 个余量 |
| `SOFT_LIFETIME` | 600 | 使用中软寿命,正常轮换周期 |
| `HARD_LIFETIME` | 1800 | 使用中硬寿命,宽限期安全阀,保证 IP 一定轮换 |
| `HB_INTERVAL` | 5 | 心跳间隔 |
| `HB_TIMEOUT_IN_USE` | 15 | 在用节点失联判定(= 丢 3 个心跳) |
| `HB_TIMEOUT_PENDING` | 30 | 建隧道阶段失联判定,给装 gost 留余量 |
| `WARMUP_TIMEOUT` | 300 | 待使用硬超时 |
| `DISPATCH_TIMEOUT` | 300 | 已派发但 runner 始终没上线 |
| `DRAIN_TIMEOUT` | 60 | 待回收但节点赖着不退,升级强杀 |
| `STUCK_PENDING` | 90 | 反死锁逃生阀的触发线 |
| `PORT_COOLDOWN` | 10 | 端口归还前的冷却,防跨代连接串台 |
| `COLD_START_WINDOW` | 20 | 重启后的观察期,必须 > `HB_TIMEOUT_IN_USE` |
| `BASE_PORT` / `POOL_SIZE` | 20000 / 60 | 端口池,**必须与 `install_server_gost.sh` 一致** |
| `OIDC_ENABLED` | 0 | 见下方"鉴权" |

## 鉴权

两层,第二层可选:

1. **Bearer 共享密钥(必须)**。`NODE_TOKEN` 放 GHA Secret。配合服务器签发的不可猜 `lease_id`,构成双因子:光有 token 猜不到 lease_id,光有 lease_id 过不了 token。
2. **GitHub OIDC(可选,默认关)**。runner 用 `id-token: write` 换短命 JWT,服务器验签并校验 `repository` claim。它解决共享密钥的根本弱点:长期有效、泄露后无法感知。

workflow **始终**会带上 `X-GHA-OIDC` 头,所以打开它不需要改 workflow,只在服务器上设:

```
OIDC_ENABLED=1
OIDC_REPOSITORY=你的用户名/你的仓库名
```

默认关是因为 OIDC 依赖外部 JWKS 拉取:如果 GitHub 的 JWKS 端点抖动而校验又是强制的,整个池子会补不进新节点。默认关、可随时打开,是风险和收益的合理切分。

## 已知行为

- **池子在 `N_TARGET-1 ↔ N_TARGET` 之间小幅浮动**。补位是在回收发生后才触发的,所以每次轮换会有约一个 `T_warmup`(20~60s)的窗口少一个节点(16 → 15)。这不是空窗——15 个出口仍在服务,只是容量掉 6%。想要恒定 16,把 `N_TARGET` 设成 17 并确认告警可接受即可。
- **DISPATCHED 阶段的 job 无法 cancel**,因为 `workflow_dispatch` 不返回 run_id。如果一个 job 在 GitHub 队列里排到超过 `DISPATCH_TIMEOUT`,服务器会先归还名额;该 job 后来真的跑起来时,`/register` 会返回 409,它立刻 `exit 0`。自愈,但会浪费一点分钟数。
- **费用**:16 个并行 runner,`SOFT_LIFETIME=600` 时约 96 次轮换/小时。私有仓库会很快吃光免费额度,**建议放公开仓库**(Actions 免费不限量),但绝不能把密钥写进代码。
- GitHub runner 是 Azure 数据中心 IP,不是住宅 IP。
- 长时间占用 runner 跑代理属灰色用法,可能触发 GitHub 滥用检测,自行评估。

## 代码结构

```
control-server/app/
  config.py     环境变量 + 两级配置校验(拒绝 / 告警)
  models.py     Lease 状态机数据模型,内部 5 态 → 节点可见 3 态的映射
  store.py      SQLite(WAL)持久化 + 端口池(free / held / cooling)
  ghclient.py   dispatch / cancel run / 清理历史 run
  auth.py       Bearer + 可选 OIDC + 按 IP 滑动窗口限流
  scheduler.py  reconcile 循环:扫超时 → 反死锁 → 轮换 → 补位
  main.py       FastAPI 路由(节点 4 个 + 运维 6 个)
.github/workflows/exit-node.yml   节点侧,1 run = 1 节点,无 matrix / guard / relay
install_control_server.sh         控制服务端一键安装
install_server_gost.sh            gost relay(穿透层,不变)
```

`reconcile` 全程持 `store.lock` 且**不 await 网络**——dispatch 和 cancel 都走后台 worker 队列,否则 GitHub API 的延迟会把节点心跳排在后面。

---

# 历史方案(已停用,仅作手动兜底保留)

> 下面两套 workflow 的 `schedule` 触发器已注释掉。它们每 15 分钟播种 19 个 job,会和新调度抢同一批 20 个账号级并发名额,两边都补不满。保留 `workflow_dispatch` 以便应急手动使用。

## frpc 版

把 GitHub Actions 的 runner 当 frpc 节点:每个 runner 一个独立出口 IP,接入服务器的 frps,组成出口池。配合 `../server` 的 mihomo + hy2,客户端随机走这些 IP 出网,并且 IP 会持续轮换。

> GitHub runner 是 Azure 数据中心 IP,不是住宅 IP。目标站点若封 Azure 段,请改用 `../node` 的住宅节点。

## 这版怎么消除"换批空窗"

旧版每个节点端口写死(node1→20001),两批一重叠就撞端口,只能靠互斥锁串行 → 换批必有空窗。新版改成:

- **动态抢空端口**:每个 runner 启动时在端口池里随机挑一个,先用 TCP 预检(连得上=被占用,换一个),再用 `frpc status` 确认抢到了才留下。两批可以安全重叠,不再撞端口。
- **服务端开大端口池**:frps 开 `POOL_SIZE`(默认 60)个端口,mihomo 把这 60 个全列进 load-balance 组,健康检查间隔短(默认 30s)→ **新端口自动纳管、死端口自动剔除**,无需手工维护节点列表。
- **错峰退出 + 滚动接力**:19 个节点不再同时死,而是按 `instance` 依次多活几秒。relay 提前触发下一轮,下一轮的 job 排队等本轮节点错峰腾名额时逐个补位 → 滚动替换,池子基本不空。

### 绕不开的硬限制

GitHub 免费账号**同一时刻最多 20 个并发 job**(整账号共享)。所以无法让"完整一批新节点"和"完整一批旧节点"同时在线(那是 40 个 job)。本设计用"错峰退出 + 逐个补位"在 20 名额内尽量贴合,空窗压到最小,但不是数学意义的零。要更稳就把 `RUN_SECONDS` 调大(换批没那么频繁,空窗占比更低)。

## 关键参数(在 `frpc-node.yml` 的 `env` 里)

| 参数 | 默认 | 说明 |
|---|---|---|
| `POOL_SIZE` | 60 | 端口池大小,**必须和服务器 install_server.sh 的 POOL_SIZE 一致** |
| `RUN_SECONDS` | 300 | 单个 runner 基础寿命(5 分钟)。调小=换 IP 更勤但空窗更频繁 |
| `STAGGER_SECONDS` | 8 | 错峰步长,节点退出分散开的间隔 |
| `BASE_PORT` | 20000 | 端口池起点,与服务器一致 |
| matrix `instance` | 1..19 | 节点数,19 + 1 relay = 20 占满并发上限 |

## 部署步骤

### 1. 服务器(开大端口池)

```bash
cd ../server
sudo TOKEN=你的强token HY2_PASSWORD=你的hy2密码 HY2_PORT=443 \
     POOL_SIZE=60 HEALTH_INTERVAL=30 STRATEGY=round-robin \
     bash install_server.sh
```

`POOL_SIZE` 要 ≥ 峰值在线节点数,给新旧重叠留余量(19 节点用 60 很宽裕)。改完确认 mihomo 起来:`systemctl status mihomo`。

### 2. 新建 GitHub 仓库并推送

把本目录(含 `.github/workflows/frpc-node.yml`)推到一个新仓库:

```cmd
cd /d d:\desktop\zip_main_local\frp-hy2-pool\node-github-action
git init
git add .
git commit -m "frpc dynamic-port rolling pool"
git branch -M main
git remote add origin https://github.com/你的用户名/frpc-nodes.git
git push -u origin main
```

### 3. 配置仓库 Secrets

仓库 → Settings → Secrets and variables → Actions:

| Secret | 值 |
|---|---|
| `FRP_SERVER_ADDR` | frp 服务器公网 IP |
| `FRP_TOKEN` | 与服务器 `TOKEN` 一致 |
| `PAT` | 有 `repo` + `workflow` 权限的 Personal Access Token |

### 4. 启动

Actions → "frpc exit nodes" → Run workflow 手动跑一次,之后靠 relay 自接力。链路若意外全断,`schedule` 看门狗(每 15 分钟)会在确认没有任何在跑/排队的 run 时重新播种。

## 验证

服务器上:

```bash
journalctl -u frps -f          # 看 exit-200xx 端口不断有节点上线/下线
ss -ltnp | grep -c ':200'      # 当前在线的出口端口数,应稳定在 ~19 上下
```

客户端多次请求,出口 IP 既在多个节点间轮询,又随时间不断换新:

```bash
curl -x socks5h://127.0.0.1:1080 https://api.ipify.org
```

## 注意

- **费用**:19 个并行 runner + 5 分钟一轮,Actions 分钟数消耗快。私有仓库会很快吃光免费额度,**建议放公开仓库**(Actions 免费不限量),但公开仓库绝不能把密钥写进代码,全部放 Secrets。
- **换批瞬间**:可能有极少数请求落到刚退出的节点上失败,客户端侧做重试即可(`round-robin` 只用当前在线节点,不会整体中断)。
- 长时间占用 runner 跑代理属灰色用法,可能触发 GitHub 滥用检测,自行评估。


---

# gost 版(relay+quic,取代 frpc)

frp 是 TCP-over-TCP,会把 TLS ClientHello 拆段,被 Akamai 这类风控识别(实测同一出口 IP,frp 路径被挡、hy2 能过)。gost 版把服务器↔节点这段换成 **relay+quic**,并在节点本地起 socks5 直连出网,规避拆段问题。工作流文件:`.github/workflows/gost-node.yml`。

## 链路

```
客户端 --hy2--> 服务器 mihomo --(127.0.0.1:200xx)--> gost relay --quic--> GHA节点 gost(socks5) --> 目标
                                          ↑ 取代 frps            ↑ 取代 frpc
```

mihomo 完全不动(仍连 `127.0.0.1:20001..60`),只换穿透层。

## 部署步骤

### 1. 服务器装 gost relay(取代 frps)

```bash
cd ../server   # frp-hy2-pool/server
sudo TOKEN=你的强口令 RELAY_PORT=8443 bash install_server_gost.sh
# 防火墙放行 8443/udp
```

脚本会停掉旧 frps、起 gost relay(`relay+quic://:8443?bind=true`),mihomo 不动。

### 2. 先用单节点验证能不能过 Akamai(别急着开 19 个 runner)

随便找台机器(本地/VPS)当节点,**用配置文件**(CLI 的 `rtcp://:端口/...` 紧凑写法会把绑定地址拼成 `0.0.0.0::端口` 非法地址,bind 必失败):

```bash
mkdir -p /etc/gost
cat > /etc/gost/node.yaml <<'EOF'
services:
- name: socks
  addr: "127.0.0.1:1080"
  handler: { type: socks5 }
  listener: { type: tcp }
- name: reverse
  addr: "127.0.0.1:20051"        # 在服务器上绑定的地址(loopback,给 mihomo 用)
  handler: { type: rtcp }
  listener: { type: rtcp, chain: relay }
  forwarder:
    nodes:
    - { name: socks-local, addr: "127.0.0.1:1080" }
chains:
- name: relay
  hops:
  - name: hop
    nodes:
    - name: server
      addr: "服务器IP:8443"
      connector:
        type: relay
        auth: { username: node, password: 你的强口令 }
      dialer: { type: quic }
EOF
gost -C /etc/gost/node.yaml
```

看到 `bind on 127.0.0.1:20051/tcp OK` 就成了。

在服务器上测出口和指纹:

```bash
curl -x socks5h://127.0.0.1:7890 https://api.ipify.org           # 应返回节点 IP
curl -x socks5h://127.0.0.1:7890 -s -o /dev/null -w "%{http_code}\n" \
  "https://tools.usps.com/go/TrackConfirmAction?tLabels=9400111899223818218407"
```

- **USPS 返回 200/302** → gost 路径过了,继续第 3 步上 GHA。
- **仍被挡** → 说明拆段不是唯一原因(可能 socks 出口仍拆 ClientHello),先别铺开,回来一起看抓包。

### 3. 配置 GHA 并启动

仓库 Secrets:

| Secret | 值 |
|---|---|
| `FRP_SERVER_ADDR` | 服务器公网 IP(复用) |
| `GOST_TOKEN` | 与服务器 `install_server_gost.sh` 的 `TOKEN` 一致 |
| `PAT` | 有 `repo`+`workflow` 权限的 PAT(复用) |

Actions → "gost exit nodes" → Run workflow。

## 注意

- `relay+quic` 节点侧默认跳过证书校验(连服务器自签证书)。若日志报 TLS/证书错误,在 `-F` 末尾加 `&secure=false`。
- gost 换的是"服务器↔节点"传输;**节点到目标那一跳仍是节点本地 socks5 新建的 TCP**。能不能彻底解决拆段,以第 2 步单节点实测为准——这就是为什么要先验证再铺开。
