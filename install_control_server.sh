#!/usr/bin/env bash
# 控制服务端一键安装(Debian/Ubuntu)。
#
# 它做四件事:
#   1. 装 Python 依赖到独立 venv
#   2. 写 /etc/exit-node-control.env(参数定档见下方注释)
#   3. 起 systemd 服务,只监听 127.0.0.1
#   4. 配 Caddy 做 HTTPS 反代
#
# 为什么必须上 HTTPS:控制面 API 得公网可达(GitHub runner 要连它),NODE_TOKEN 和
# lease_id 明文过公网等于把整个池子交出去。脚本默认不允许裸 HTTP。
set -euo pipefail

APP_DIR="/opt/exit-node-control"
ENV_FILE="/etc/exit-node-control.env"
DATA_DIR="/var/lib/exit-node-control"
SERVICE="exit-node-control"
LISTEN_PORT="${LISTEN_PORT:-8787}"

die() { echo "错误: $*" >&2; exit 1; }
[ "$(id -u)" = "0" ] || die "请用 root 运行"

# ---------------------------------------------------------------- 参数收集
DOMAIN="${DOMAIN:-}"
GH_TOKEN="${GH_TOKEN:-}"
GH_OWNER="${GH_OWNER:-}"
GH_REPO="${GH_REPO:-}"
GH_REF="${GH_REF:-main}"

ask() {  # ask <变量名> <提示> [默认值]
  local var="$1" prompt="$2" def="${3:-}" cur val
  cur="${!var:-}"
  if [ -n "$cur" ]; then return 0; fi
  if [ -n "$def" ]; then
    read -rp "${prompt} [${def}]: " val || true
    val="${val:-$def}"
  else
    read -rp "${prompt}: " val || true
  fi
  printf -v "$var" '%s' "$val" 2>/dev/null || eval "$var=\$val"
}

ask DOMAIN   "控制面域名(必须已解析到本机,用于签 HTTPS 证书)"
ask GH_OWNER "GitHub 用户名/组织名"
ask GH_REPO  "仓库名"
ask GH_REF   "dispatch 的分支" "main"
if [ -z "${GH_TOKEN}" ]; then
  read -rsp "GitHub PAT(需要 repo + workflow 权限,输入不回显): " GH_TOKEN; echo
fi

[ -n "$DOMAIN" ]   || die "域名不能为空"
[ -n "$GH_OWNER" ] || die "GH_OWNER 不能为空"
[ -n "$GH_REPO" ]  || die "GH_REPO 不能为空"
[ -n "$GH_TOKEN" ] || die "GH_TOKEN 不能为空"

NODE_TOKEN="${NODE_TOKEN:-$(openssl rand -hex 32)}"
ADMIN_TOKEN="${ADMIN_TOKEN:-$(openssl rand -hex 32)}"

# 容量参数可在命令行覆盖。建议首次部署用 N_TARGET=2 跑通全链路,确认出口 IP 真的能用之后
# 再调到 16 —— 一上来就开 16 个 runner,出问题时日志会被 16 份心跳刷得很难看。
N_TARGET="${N_TARGET:-16}"
N_MIN="${N_MIN:-12}"
MAX_INFLIGHT="${MAX_INFLIGHT:-19}"
SOFT_LIFETIME="${SOFT_LIFETIME:-600}"
HARD_LIFETIME="${HARD_LIFETIME:-1800}"
BASE_PORT="${BASE_PORT:-20000}"
POOL_SIZE="${POOL_SIZE:-60}"

# ---------------------------------------------------------------- 最小依赖
# 起飞前检查要用 curl + jq,所以先把这几个装上,重活留到检查通过之后
echo "==> 安装基础工具"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq curl jq openssl ca-certificates >/dev/null

# ---------------------------------------------------------------- 起飞前检查
# 这三项任何一项不对,服务都能正常启动,但 reconcile 会一直 dispatch 失败并退避 ——
# 表现是"池子永远是空的"却没有明显报错。宁可在这里直接失败。
echo "==> 检查 GitHub 配置"
GH_WORKFLOW_FILE="exit-node.yml"
gh_api() { curl -sS -m 15 -w '\n%{http_code}' -H "Authorization: Bearer ${GH_TOKEN}" \
             -H "Accept: application/vnd.github+json" \
             -H "X-GitHub-Api-Version: 2022-11-28" "https://api.github.com$1"; }

out="$(gh_api "/repos/${GH_OWNER}/${GH_REPO}")" || die "无法访问 GitHub API,检查服务器出网"
code="${out##*$'\n'}"; body="${out%$'\n'*}"
case "$code" in
  200) ;;
  401) die "PAT 无效或已过期(HTTP 401)" ;;
  404) die "仓库 ${GH_OWNER}/${GH_REPO} 不存在,或 PAT 无权访问(HTTP 404)" ;;
  *)   die "访问仓库失败 HTTP ${code}: $(printf '%s' "$body" | head -c 200)" ;;
esac
IS_PRIVATE="$(printf '%s' "$body" | jq -r '.private')"
echo "    仓库可访问,private=${IS_PRIVATE}"
if [ "$IS_PRIVATE" = "true" ]; then
  echo "    !! 私有仓库的 Actions 分钟数有限,16 个并行 runner 会很快吃光免费额度。"
  echo "       建议改成公开仓库(Actions 免费不限量),密钥全部放 Secrets。"
fi

out="$(gh_api "/repos/${GH_OWNER}/${GH_REPO}/actions/workflows/${GH_WORKFLOW_FILE}")"
code="${out##*$'\n'}"
if [ "$code" != "200" ]; then
  die "在仓库里找不到 .github/workflows/${GH_WORKFLOW_FILE}(HTTP ${code})。
     请先把本仓库(含该 workflow)push 到 ${GH_REF} 分支,并在 Actions 页面确认
     它已出现,再回来运行本脚本。GitHub 只有登记过该 workflow 才接受 dispatch。"
fi
WF_STATE="$(printf '%s' "${out%$'\n'*}" | jq -r '.state')"
echo "    workflow ${GH_WORKFLOW_FILE} 已登记,state=${WF_STATE}"
[ "$WF_STATE" = "active" ] || die "workflow 状态是 ${WF_STATE},请在 Actions 页面启用它"

# ---------------------------------------------------------------- 依赖
echo "==> 安装系统依赖"
apt-get install -y -qq python3 python3-venv python3-pip gnupg \
  debian-keyring debian-archive-keyring apt-transport-https >/dev/null

if ! command -v caddy >/dev/null 2>&1; then
  echo "==> 安装 Caddy(HTTPS 反代 + 自动证书)"
  curl -fsSL https://dl.cloudsmith.io/public/caddy/stable/gpg.key \
    | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  curl -fsSL https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt \
    > /etc/apt/sources.list.d/caddy-stable.list
  apt-get update -qq
  apt-get install -y -qq caddy >/dev/null
fi

# ---------------------------------------------------------------- 代码 + venv
echo "==> 部署代码到 ${APP_DIR}"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/control-server"
[ -d "${SRC_DIR}/app" ] || die "找不到 ${SRC_DIR}/app,请在仓库根目录运行本脚本"

mkdir -p "${APP_DIR}" "${DATA_DIR}"
cp -r "${SRC_DIR}/app" "${SRC_DIR}/requirements.txt" "${APP_DIR}/"

if [ ! -x "${APP_DIR}/venv/bin/python" ]; then
  python3 -m venv "${APP_DIR}/venv"
fi
"${APP_DIR}/venv/bin/pip" install -q --upgrade pip
"${APP_DIR}/venv/bin/pip" install -q -r "${APP_DIR}/requirements.txt"

id -u exitctl >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin exitctl
chown -R exitctl:exitctl "${APP_DIR}" "${DATA_DIR}"

# ---------------------------------------------------------------- 环境变量
echo "==> 写入 ${ENV_FILE}"
cat > "${ENV_FILE}" <<EOF
# ============ GitHub ============
GH_TOKEN=${GH_TOKEN}
GH_OWNER=${GH_OWNER}
GH_REPO=${GH_REPO}
GH_WORKFLOW=exit-node.yml
GH_REF=${GH_REF}

# ============ 鉴权 ============
# NODE_TOKEN 要同步配到仓库 Secrets 的 NODE_TOKEN
NODE_TOKEN=${NODE_TOKEN}
ADMIN_TOKEN=${ADMIN_TOKEN}
# 打开 OIDC 第二因子(workflow 侧无需改动):
# OIDC_ENABLED=1
# OIDC_REPOSITORY=${GH_OWNER}/${GH_REPO}
OIDC_ENABLED=0
OIDC_AUDIENCE=exit-node-pool

# ============ 端口池(必须与 mihomo 的 load-balance 组范围一致) ============
BASE_PORT=${BASE_PORT}
POOL_SIZE=${POOL_SIZE}
PORT_COOLDOWN=10

# ============ 容量 ============
# 由 Little's law 反推:稳态在途 = N_TARGET * T_warmup / SOFT_LIFETIME
#                            = 16 * 45 / 600 ≈ 1.2
# 峰值 inflight ≈ 16 + 1.2 + 少量 draining ≈ 18,MAX_INFLIGHT=19 留出余量。
# 想把 N_TARGET 往上加,就必须同步加大 SOFT_LIFETIME,否则会撞 20 并发上限并死锁。
N_TARGET=${N_TARGET}
N_MIN=${N_MIN}
MAX_INFLIGHT=${MAX_INFLIGHT}

# ============ 时间参数(秒) ============
HB_INTERVAL=5
HB_TIMEOUT_IN_USE=15
HB_TIMEOUT_PENDING=30
WARMUP_TIMEOUT=300
DISPATCH_TIMEOUT=300
DRAIN_TIMEOUT=60
SOFT_LIFETIME=${SOFT_LIFETIME}
HARD_LIFETIME=${HARD_LIFETIME}
STUCK_PENDING=90

# ============ 调度 ============
RECONCILE_TICK=2
COLD_START_WINDOW=20
DISPATCH_MIN_INTERVAL=1
DISPATCH_BURST=4
DISPATCH_BACKOFF=15

# ============ 杂项 ============
DB_PATH=${DATA_DIR}/state.db
JANITOR_INTERVAL=300
JANITOR_KEEP=600
LOG_LEVEL=INFO
EOF
chmod 600 "${ENV_FILE}"

# ---------------------------------------------------------------- systemd
echo "==> 配置 systemd 服务"
cat > "/etc/systemd/system/${SERVICE}.service" <<EOF
[Unit]
Description=GitHub Actions 出口 IP 池 · 控制服务端
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=exitctl
Group=exitctl
EnvironmentFile=${ENV_FILE}
WorkingDirectory=${APP_DIR}
# 只监听回环:公网入口一律走 Caddy 的 HTTPS
ExecStart=${APP_DIR}/venv/bin/uvicorn app.main:app --host 127.0.0.1 --port ${LISTEN_PORT} \\
          --no-access-log --proxy-headers --forwarded-allow-ips 127.0.0.1
Restart=always
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${DATA_DIR}

[Install]
WantedBy=multi-user.target
EOF

# ---------------------------------------------------------------- Caddy
echo "==> 配置 Caddy 反代 ${DOMAIN}"
mkdir -p /etc/caddy/sites
# 运维接口一律不走公网:公网访问直接 403,要用就 SSH 隧道到 127.0.0.1:${LISTEN_PORT}。
# (故意不写 "not remote_ip 127.0.0.1" 这类条件 —— 经 Caddy 反代进来的请求 remote_ip
#  永远是客户端公网 IP,那个条件恒真,写了只是让人误以为有本机放行的口子。)
cat > "/etc/caddy/sites/${SERVICE}.caddy" <<EOF
${DOMAIN} {
    encode zstd gzip
    @admin path /v1/admin/*
    respond @admin 403
    reverse_proxy 127.0.0.1:${LISTEN_PORT}
}
EOF
if ! grep -q "import sites/\*.caddy" /etc/caddy/Caddyfile 2>/dev/null; then
  # 前置换行:防止原文件末尾没有换行时把 import 拼到上一行尾部
  printf '\nimport sites/*.caddy\n' >> /etc/caddy/Caddyfile
fi
caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile \
  || die "Caddyfile 校验失败,请检查 /etc/caddy/sites/${SERVICE}.caddy"

systemctl daemon-reload
systemctl enable --now "${SERVICE}"
systemctl reload caddy 2>/dev/null || systemctl restart caddy

sleep 3
echo
echo "======================================================================"
systemctl is-active --quiet "${SERVICE}" \
  && echo "控制服务端已启动" \
  || { echo "启动失败,日志如下:"; journalctl -u "${SERVICE}" -n 40 --no-pager; exit 1; }

cat <<EOF

下一步:在 GitHub 仓库 Settings -> Secrets and variables -> Actions 添加

  CONTROL_URL       = https://${DOMAIN}
  NODE_TOKEN        = ${NODE_TOKEN}
  FRP_SERVER_ADDR   = <本机公网 IP>        (若已存在则复用)
  GOST_TOKEN        = <与 install_server_gost.sh 的 TOKEN 一致>

仓库里**不再需要** secrets.PAT —— PAT 现在只存在于这台服务器上。

运维接口只监听本机,用 SSH 隧道访问:
  ssh -L 8787:127.0.0.1:${LISTEN_PORT} <本机>
  curl -H "Authorization: Bearer ${ADMIN_TOKEN}" http://127.0.0.1:8787/v1/admin/state | jq

常用命令:
  journalctl -u ${SERVICE} -f                          # 看调度日志
  curl -s https://${DOMAIN}/healthz | jq               # 健康检查(无需鉴权)
  systemctl restart ${SERVICE}                         # 重启(有 20s 冷启动收敛窗口,在线节点不会被误杀)

ADMIN_TOKEN 已写入 ${ENV_FILE},请自行留存。
======================================================================
EOF
