#!/usr/bin/env python3
"""申请独占 GitHub 出口并在本机启动 Hysteria2 SOCKS5。"""
from __future__ import annotations

import argparse
import getpass
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

DEFAULT_SERVER = "https://www.kungfu.bj.cn"


def token_from(args: argparse.Namespace) -> str:
    token = args.token or os.environ.get("EXIT_NODE_CLIENT_TOKEN", "")
    if not token:
        token = getpass.getpass("CLIENT_TOKEN（输入不回显）: ").strip()
    if not token:
        raise SystemExit("CLIENT_TOKEN 不能为空")
    return token


def api(server: str, token: str, method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        server.rstrip("/") + path,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "exit-node-client/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:500]
        raise RuntimeError(f"控制服务返回 HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"无法访问控制服务: {exc.reason}") from exc


def hysteria_binary(value: str | None) -> str:
    candidate = value or os.environ.get("HYSTERIA_BIN") or shutil.which("hysteria")
    if not candidate and os.name == "nt":
        candidate = shutil.which("hysteria.exe")
    if not candidate:
        raise SystemExit("找不到 Hysteria2 客户端，请用 --hysteria 指定 hysteria.exe")
    return candidate


def yaml_config(info: dict, socks_port: int) -> str:
    return (
        f"server: {json.dumps(str(info['hy2_host']) + ':' + str(info['hy2_port']))}\n"
        f"auth: {json.dumps(info['password'])}\n"
        "tls:\n"
        f"  sni: {json.dumps(info['sni'])}\n"
        "  insecure: false\n"
        "obfs:\n"
        "  type: salamander\n"
        "  salamander:\n"
        f"    password: {json.dumps(info['obfs_password'])}\n"
        "socks5:\n"
        f"  listen: {json.dumps('127.0.0.1:' + str(socks_port))}\n"
        "  disableUDP: true\n"
        "lazy: false\n"
    )


def wait_socks(process: subprocess.Popen, port: int, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"Hysteria2 客户端提前退出，exit={process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.25)
    raise RuntimeError("本地 SOCKS5 未在 10 秒内开始监听")


def release(server: str, token: str, session_id: str) -> dict:
    return api(server, token, "POST", "/v1/client/release", {"session_id": session_id})


def _windows_identity() -> str:
    user = os.environ.get("USERNAME") or getpass.getuser()
    domain = os.environ.get("USERDOMAIN")
    return f"{domain}\\{user}" if domain else user


def _icacls(path: Path, permission: str) -> None:
    result = subprocess.run(
        [
            "icacls",
            str(path),
            "/inheritance:r",
            "/grant:r",
            f"{_windows_identity()}:{permission}",
        ],
        text=True,
        capture_output=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if result.returncode != 0:
        raise RuntimeError(f"无法收紧 Windows ACL: {result.stderr.strip()[:300]}")


def secure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        _icacls(path, "(OI)(CI)F")
    else:
        os.chmod(path, 0o700)


def write_private(path: Path, content: str) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
        if os.name == "nt":
            _icacls(path, "F")
        else:
            os.chmod(path, 0o600)
    except Exception:
        path.unlink(missing_ok=True)
        raise


def cmd_start(args: argparse.Namespace) -> int:
    if args.dry_run:
        print(f"DRY_RUN: 将向 {args.server}/v1/client/acquire 申请 {args.ttl}s 独占节点")
        print(f"DRY_RUN: 将启动本地 SOCKS5 127.0.0.1:{args.socks_port}")
        print("DRY_RUN: 退出时调用 /v1/client/release，异常时由服务端 TTL 兜底")
        return 0

    token = token_from(args)
    binary = hysteria_binary(args.hysteria)
    request_id = uuid.uuid4().hex
    info: dict | None = None
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            info = api(
                args.server,
                token,
                "POST",
                "/v1/client/acquire",
                {"request_id": request_id, "ttl_seconds": args.ttl},
            )
            break
        except RuntimeError as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(2)
    if info is None:
        raise RuntimeError(f"申请节点失败: {last_error}")

    session_id = info["session_id"]
    process: subprocess.Popen | None = None
    config_path: Path | None = None
    old_handlers: dict[int, object] = {}

    def interrupt(_signum, _frame) -> None:
        raise KeyboardInterrupt

    signals = [signal.SIGTERM]
    if hasattr(signal, "SIGHUP"):
        signals.append(signal.SIGHUP)
    if hasattr(signal, "SIGBREAK"):
        signals.append(signal.SIGBREAK)
    for item in signals:
        old_handlers[item] = signal.getsignal(item)
        signal.signal(item, interrupt)

    try:
        # acquire 成功后的所有本地文件操作都在清理域内，任何异常都会主动 release。
        runtime = Path(args.runtime_dir)
        secure_directory(runtime)
        config_path = runtime / f"_session-{session_id}.yaml"
        log_path = runtime / f"_hysteria-{session_id}.log"
        write_private(config_path, yaml_config(info, args.socks_port))

        with log_path.open("ab") as log_file:
            process = subprocess.Popen(
                [binary, "client", "--config", str(config_path), "--disable-update-check"],
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
            wait_socks(process, args.socks_port)
            print(f"SESSION_ID={session_id}")
            print(f"SOCKS5=socks5h://127.0.0.1:{args.socks_port}")
            print(f"RUNNER_IP={info.get('runner_ip') or '未知'}")
            print(f"EXPIRES_AT={info['expires_at']}")
            print(f"LOG={log_path}")
            print("按 Ctrl+C 释放该节点；释放后 runner 将被销毁并自动补充新 IP。")
            while process.poll() is None:
                time.sleep(1)
            raise RuntimeError(f"Hysteria2 客户端退出，exit={process.returncode}")
    except KeyboardInterrupt:
        print("正在释放独占节点……")
    finally:
        # 清理期间忽略第二次终止信号，确保服务端 release 一定执行到。
        for item in old_handlers:
            signal.signal(item, signal.SIG_IGN)
        try:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        print("本地 Hysteria2 进程未能回收", file=sys.stderr)
        except Exception as exc:
            print(f"清理本地 Hysteria2 失败: {exc}", file=sys.stderr)
        try:
            state = release(args.server, token, session_id)
            print(f"RELEASE_STATE={state.get('state', 'unknown')}")
        except Exception as exc:
            print(f"主动释放失败（服务端 TTL 仍会兜底）: {exc}", file=sys.stderr)
        finally:
            if config_path is not None:
                config_path.unlink(missing_ok=True)
            for item, handler in old_handlers.items():
                signal.signal(item, handler)
    return 0


def cmd_release(args: argparse.Namespace) -> int:
    token = token_from(args)
    print(json.dumps(release(args.server, token, args.session_id), ensure_ascii=False, indent=2))
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    token = token_from(args)
    result = api(args.server, token, "GET", f"/v1/client/sessions/{args.session_id}")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--server", default=DEFAULT_SERVER)
    root.add_argument("--token", help="不建议写命令行；省略时安全输入或读取 EXIT_NODE_CLIENT_TOKEN")
    sub = root.add_subparsers(dest="command", required=True)

    start = sub.add_parser("start", help="申请节点并启动本地 SOCKS5")
    start.add_argument("--socks-port", type=int, required=True)
    start.add_argument("--ttl", type=int, default=600)
    start.add_argument("--hysteria")
    start.add_argument("--runtime-dir", default="_runtime")
    start.add_argument("--dry-run", action="store_true")
    start.set_defaults(func=cmd_start)

    release_parser = sub.add_parser("release", help="手工释放会话")
    release_parser.add_argument("session_id")
    release_parser.set_defaults(func=cmd_release)

    status_parser = sub.add_parser("status", help="查询会话状态")
    status_parser.add_argument("session_id")
    status_parser.set_defaults(func=cmd_status)
    return root


def main() -> int:
    args = parser().parse_args()
    if getattr(args, "socks_port", 1) not in range(1024, 65536):
        raise SystemExit("--socks-port 必须在 1024..65535")
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
