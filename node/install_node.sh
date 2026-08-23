#!/usr/bin/env bash
# VPS/住宅节点一键安装 frpc(socks5 出口)· 自动抢空端口版
#
# 一键用法(同一条命令可直接粘到每台 VPS,无需分配 NODE_ID):
#   curl -fsSL <本脚本URL> | sudo SERVER_ADDR=服务器IP TOKEN=你的frp令牌 bash
# 或下载后:
#   sudo SERVER_ADDR=服务器IP TOKEN=你的frp令牌 bash install_node.sh
#
# 想固定端口(老用法)也行:额外传 NODE_ID=1 就会用 BASE_PORT+1,不再自动抢。
set -euo pipefail

FRP_VERSION="${FRP_VERSION:-0.61.1}"
SERVER_ADDR="${SERVER_ADDR:?必须设置 SERVER_ADDR=服务器公网IP}"
TOKEN="${TOKEN:-CHANGE_ME_FRP_TOKEN}"
FRP_PORT="${FRP_PORT:-7000}"
BASE_PORT="${BASE_PORT:-20000}"          # 端口池起点,需与服务器一致
POOL_SIZE="${POOL_SIZE:-60}"             # 端口池大小,需与服务器 install_server.sh 一致
NODE_ID="${NODE_ID:-}"                    # 留空=自动抢空端口;填数字=固定用 BASE_PORT+NODE_ID
MAX_TRY="${MAX_TRY:-25}"                  # 自动抢端口最大尝试次数

case "$(uname -m)" in
  x86_64|amd64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  armv7l) ARCH="arm" ;;
  *) echo "不支持的架构: $(uname -m)"; exit 1 ;;
esac

command -v curl >/dev/null || { echo "缺少 curl"; exit 1; }
command -v tar  >/dev/null || { echo "缺少 tar"; exit 1; }

WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT

echo "==> 安装 frpc v${FRP_VERSION} (${ARCH})"
curl -fsSL "https://github.com/fatedier/frp/releases/download/v${FRP_VERSION}/frp_${FRP_VERSION}_linux_${ARCH}.tar.gz" \
  -o "$WORK/frp.tar.gz"
tar -xzf "$WORK/frp.tar.gz" -C "$WORK"
install -m 0755 "$WORK/frp_${FRP_VERSION}_linux_${ARCH}/frpc" /usr/local/bin/frpc
mkdir -p /etc/frp

# 生成一份带指定 remotePort 的 frpc 配置
write_conf() {
  local rport="$1" tag="$2"
  cat > /etc/frp/frpc.toml <<EOF
serverAddr = "${SERVER_ADDR}"
serverPort = ${FRP_PORT}
auth.method = "token"
auth.token = "${TOKEN}"

[[proxies]]
name = "${tag}"
type = "tcp"
remotePort = ${rport}

[proxies.plugin]
type = "socks5"
EOF
}

# 试抢一个端口:前台短跑 frpc,看日志判断是否抢到(成功 / 端口被占)
try_port() {
  local rport="$1"
  local tag="node-$(hostname | tr -cd 'a-zA-Z0-9' | tail -c 6)-${rport}"
  write_conf "$rport" "$tag"
  local log="$WORK/probe.log"; : > "$log"
  /usr/local/bin/frpc -c /etc/frp/frpc.toml >"$log" 2>&1 &
  local pid=$!
  for _ in $(seq 1 10); do
    sleep 1
    if grep -qE "start proxy success|login to server success.*proxy" "$log" 2>/dev/null \
       && grep -q "start proxy success" "$log" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
      return 0
    fi
    if grep -qiE "port already used|already in use|proxy.*already exists|remote port.*not allowed" "$log" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
      return 1
    fi
    kill -0 "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null || true; return 1; }
  done
  kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true
  # 没明确成功也没明确失败:保守当失败,换端口
  return 1
}

REMOTE_PORT=""
if [[ -n "$NODE_ID" ]]; then
  REMOTE_PORT=$((BASE_PORT + NODE_ID))
  echo "==> 固定端口模式 NODE_ID=${NODE_ID} -> remotePort=${REMOTE_PORT}"
  write_conf "$REMOTE_PORT" "node${NODE_ID}-socks"
else
  echo "==> 自动抢空端口(池 ${BASE_PORT}+1 .. ${BASE_PORT}+${POOL_SIZE},最多试 ${MAX_TRY} 次)"
  for _ in $(seq 1 "$MAX_TRY"); do
    cand=$((BASE_PORT + 1 + RANDOM % POOL_SIZE))
    echo "   试端口 ${cand} ..."
    if try_port "$cand"; then
      REMOTE_PORT="$cand"
      echo "   抢到端口 ${REMOTE_PORT}"
      break
    fi
  done
  [[ -n "$REMOTE_PORT" ]] || { echo "!! 试了 ${MAX_TRY} 次都没抢到空端口,端口池可能已满,加大 POOL_SIZE 或稍后重试"; exit 1; }
fi

echo "==> 注册 systemd 服务"
cat > /etc/systemd/system/frpc.service <<EOF
[Unit]
Description=frp client exit node (port ${REMOTE_PORT})
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/frpc -c /etc/frp/frpc.toml
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now frpc

echo ""
echo "================ 节点安装完成 ================"
echo " 出口端口   : ${REMOTE_PORT}  (服务器本地 socks5,mihomo 自动纳管)"
echo " 服务器     : ${SERVER_ADDR}:${FRP_PORT}"
echo " 模式       : $([[ -n "$NODE_ID" ]] && echo "固定(NODE_ID=${NODE_ID})" || echo "自动抢端口")"
echo " 查看状态   : systemctl status frpc"
echo " 看日志     : journalctl -u frpc -f"
echo "============================================="
