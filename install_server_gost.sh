#!/usr/bin/env bash
# 服务器端:安装 gost relay(取代 frps 做反向隧道入口)。mihomo 保持不动。
# gost 用 relay+quic 监听,GHA/VPS 节点用 rtcp 反向把本地 socks5 映射成服务器本地端口 20000+N,
# mihomo 仍然连 127.0.0.1:20001..20060,无需改 mihomo 配置。
#
# 用法:
#   sudo TOKEN=你的强口令 RELAY_PORT=8443 bash install_server_gost.sh
set -euo pipefail

GOST_VERSION="${GOST_VERSION:-3.2.6}"
TOKEN="${TOKEN:-CHANGE_ME_TOKEN}"          # 节点接入口令(relay 认证),节点侧要一致
RELAY_PORT="${RELAY_PORT:-8443}"           # gost relay 监听端口(QUIC/UDP)
RELAY_USER="${RELAY_USER:-node}"           # relay 用户名
STOP_FRPS="${STOP_FRPS:-yes}"              # 是否停掉旧的 frps(默认停,避免两套并存抢端口)

case "$(uname -m)" in
  x86_64|amd64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  armv7l) ARCH="armv7" ;;
  *) echo "不支持的架构: $(uname -m)"; exit 1 ;;
esac

command -v curl >/dev/null || { echo "缺少 curl"; exit 1; }
command -v tar  >/dev/null || { echo "缺少 tar"; exit 1; }

WORK="$(mktemp -d)"; trap 'rm -rf "$WORK"' EXIT

echo "==> [1/3] 安装 gost v${GOST_VERSION} (${ARCH})"
curl -fsSL "https://github.com/go-gost/gost/releases/download/v${GOST_VERSION}/gost_${GOST_VERSION}_linux_${ARCH}.tar.gz" \
  -o "$WORK/gost.tar.gz"
tar -xzf "$WORK/gost.tar.gz" -C "$WORK"
install -m 0755 "$WORK/gost" /usr/local/bin/gost

echo "==> [2/3] 写入 gost 配置文件 + systemd 服务 (relay+quic://:${RELAY_PORT})"
# 用配置文件而非 CLI:确保 bind 选项作用在 relay handler 的 metadata 上(CLI 的 ?bind=true 不生效)。
# relay+quic:QUIC 传输,内置 TLS(gost 自动生成自签证书),节点用 quic 拨入。
mkdir -p /etc/gost
cat > /etc/gost/config.yaml <<EOF
services:
- name: relay
  addr: ":${RELAY_PORT}"
  handler:
    type: relay
    auth:
      username: ${RELAY_USER}
      password: ${TOKEN}
    metadata:
      bind: true
  listener:
    type: quic
EOF

cat > /etc/systemd/system/gost-relay.service <<EOF
[Unit]
Description=gost relay (reverse tunnel entry)
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/gost -C /etc/gost/config.yaml
Restart=on-failure
RestartSec=3
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
EOF

echo "==> [3/3] 启动服务"
if [[ "$STOP_FRPS" == "yes" ]]; then
  systemctl disable --now frps 2>/dev/null || true
  echo "    已停用旧 frps"
fi
systemctl daemon-reload
systemctl enable --now gost-relay

echo ""
echo "================ gost relay 安装完成 ================"
echo " relay 端口   : ${RELAY_PORT} (UDP, QUIC)"
echo " relay 用户   : ${RELAY_USER}"
echo " relay 口令   : ${TOKEN}"
echo " 传输         : relay+quic (bind=true 允许节点反向绑定端口)"
echo "----------------------------------------------------"
echo " 防火墙放行   : ${RELAY_PORT}/udp"
echo " mihomo 不用动 : 仍连 127.0.0.1:20001..20060"
echo " 查看状态     : systemctl status gost-relay"
echo " 看日志       : journalctl -u gost-relay -f"
echo "===================================================="
