#!/usr/bin/env python3
"""root helper：仅允许在配置的端口池内启停 Hysteria2 systemd 实例。"""
from __future__ import annotations

import grp
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

CONTROL_CONFIG = Path("/etc/exit-node-hy2.json")
SYSTEMCTL = "/usr/bin/systemctl"


def fail(message: str, code: int = 1) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(code)


def run(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(args), check=check, text=True, capture_output=True, timeout=10
    )


def load_control() -> dict:
    try:
        data = json.loads(CONTROL_CONFIG.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"无法读取 helper 配置: {exc}")
    required = {"base_port", "pool_size", "cert", "key", "data_dir", "service_group"}
    if not required.issubset(data):
        fail("helper 配置缺少字段")
    return data


def validate_port(value: object, config: dict) -> int:
    if not isinstance(value, int):
        fail("port 必须是整数")
    low = int(config["base_port"]) + 1
    high = int(config["base_port"]) + int(config["pool_size"])
    if not low <= value <= high:
        fail(f"port 不在允许范围 {low}..{high}")
    return value


def validate_secret(value: object, name: str) -> str:
    if not isinstance(value, str) or not 20 <= len(value) <= 256:
        fail(f"{name} 长度非法")
    if any(ord(char) < 32 for char in value):
        fail(f"{name} 含控制字符")
    return value


def unit(port: int) -> str:
    return f"hysteria-session@{port}.service"


def config_path(config: dict, port: int) -> Path:
    return Path(config["data_dir"]) / f"{port}.yaml"


def tcp_ready(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            return True
    except OSError:
        return False


def write_config(config: dict, port: int, password: str, obfs_password: str) -> Path:
    data_dir = Path(config["data_dir"])
    gid = grp.getgrnam(str(config["service_group"])).gr_gid
    data_dir.mkdir(mode=0o750, parents=True, exist_ok=True)
    os.chown(data_dir, 0, gid)
    os.chmod(data_dir, 0o750)
    content = (
        f"listen: {json.dumps(':' + str(port))}\n"
        "tls:\n"
        f"  cert: {json.dumps(str(config['cert']))}\n"
        f"  key: {json.dumps(str(config['key']))}\n"
        "auth:\n"
        "  type: password\n"
        f"  password: {json.dumps(password)}\n"
        "obfs:\n"
        "  type: salamander\n"
        "  salamander:\n"
        f"    password: {json.dumps(obfs_password)}\n"
        "disableUDP: true\n"
        "outbounds:\n"
        "  - name: runner\n"
        "    type: socks5\n"
        "    socks5:\n"
        f"      addr: {json.dumps('127.0.0.1:' + str(port))}\n"
    )
    fd, temporary = tempfile.mkstemp(prefix=f"_{port}-", suffix=".yaml", dir=data_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.chown(temporary, 0, gid)
        os.chmod(temporary, 0o640)
        target = config_path(config, port)
        os.replace(temporary, target)
        return target
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def stop(config: dict, port: int, *, remove: bool) -> None:
    run(SYSTEMCTL, "stop", unit(port), check=False)
    for _ in range(20):
        state = run(SYSTEMCTL, "is-active", unit(port), check=False).stdout.strip()
        if state not in {"active", "activating", "deactivating", "reloading"}:
            if remove:
                config_path(config, port).unlink(missing_ok=True)
            return
        time.sleep(0.25)
    fail("Hysteria2 实例未能确认停止，配置和端口保持隔离")


def start(config: dict, port: int, password: str, obfs_password: str) -> None:
    if not tcp_ready(port):
        fail("目标 runner SOCKS5 端口尚未可用")
    stop(config, port, remove=True)
    write_config(config, port, password, obfs_password)
    result = run(SYSTEMCTL, "start", unit(port), check=False)
    if result.returncode != 0:
        stop(config, port, remove=True)
        fail("Hysteria2 systemd 实例启动失败")
    for _ in range(20):
        active = run(SYSTEMCTL, "is-active", unit(port), check=False)
        if active.stdout.strip() == "active":
            return
        time.sleep(0.25)
    stop(config, port, remove=True)
    fail("Hysteria2 实例未在 5 秒内进入 active")


def ensure(config: dict, port: int) -> None:
    path = config_path(config, port)
    if not path.is_file() or path.stat().st_mode & 0o027:
        fail("现有 HY2 配置不存在或权限不安全")
    result = run(SYSTEMCTL, "is-active", unit(port), check=False)
    if result.stdout.strip() != "active":
        run(SYSTEMCTL, "start", unit(port))


def main() -> int:
    if os.geteuid() != 0:
        fail("helper 必须由 root 执行")
    if len(sys.argv) != 1:
        fail("helper 不接受命令行参数")
    raw = sys.stdin.buffer.read(4097)
    if len(raw) > 4096:
        fail("请求过大")
    try:
        request = json.loads(raw)
    except json.JSONDecodeError:
        fail("请求不是合法 JSON")
    config = load_control()
    port = validate_port(request.get("port"), config)
    action = request.get("action")
    if action == "start":
        start(
            config,
            port,
            validate_secret(request.get("password"), "password"),
            validate_secret(request.get("obfs_password"), "obfs_password"),
        )
    elif action == "stop":
        stop(config, port, remove=True)
    elif action == "ensure":
        ensure(config, port)
    else:
        fail("不支持的 action")
    print(json.dumps({"ok": True, "action": action, "port": port}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
