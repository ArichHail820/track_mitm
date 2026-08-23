"""通过最小权限 root helper 管理每个客户端会话的 Hysteria2 实例。"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from dataclasses import dataclass

from .config import Settings

log = logging.getLogger("hy2")


class Hy2Error(RuntimeError):
    pass


@dataclass(frozen=True)
class Hy2Credentials:
    password: str
    obfs_password: str


class Hy2Controller:
    def __init__(self, settings: Settings) -> None:
        self.s = settings

    async def start(self, port: int, credentials: Hy2Credentials) -> None:
        await self._call(
            {
                "action": "start",
                "port": port,
                "password": credentials.password,
                "obfs_password": credentials.obfs_password,
            }
        )

    async def stop(self, port: int) -> None:
        await self._call({"action": "stop", "port": port})

    async def ensure(self, port: int) -> None:
        await self._call({"action": "ensure", "port": port})

    async def _call(self, payload: dict) -> dict:
        process = await asyncio.create_subprocess_exec(
            "sudo",
            "-n",
            self.s.hy2_helper,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        raw = json.dumps(payload, separators=(",", ":")).encode()
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(raw), timeout=self.s.hy2_command_timeout
            )
        except asyncio.TimeoutError as exc:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                log.error("HY2 helper 进程组终止后仍未回收 pid=%s", process.pid)
            raise Hy2Error("HY2 helper 执行超时") from exc
        if process.returncode != 0:
            detail = stderr.decode(errors="replace").strip()[:500]
            raise Hy2Error(f"HY2 helper 失败({process.returncode}): {detail}")
        try:
            result = json.loads(stdout.decode() or "{}")
        except json.JSONDecodeError as exc:
            raise Hy2Error("HY2 helper 返回了非法 JSON") from exc
        if not result.get("ok"):
            raise Hy2Error(str(result.get("error") or "HY2 helper 未确认成功"))
        log.info("HY2 helper action=%s port=%s 完成", payload["action"], payload["port"])
        return result
