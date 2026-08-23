"""客户端独占会话：原子分配 runner、启停 HY2、释放即销毁 runner。"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import logging

from .config import Settings
from .hy2 import Hy2Controller, Hy2Credentials, Hy2Error
from .models import CLIENT_LIVE_STATES, ClientSession, ClientState, State, now
from .scheduler import Scheduler
from .store import Store

log = logging.getLogger("sessions")


class NoAvailableNode(RuntimeError):
    pass


class SessionUnavailable(RuntimeError):
    pass


class SessionManager:
    def __init__(
        self,
        settings: Settings,
        store: Store,
        scheduler: Scheduler,
        hy2: Hy2Controller,
    ) -> None:
        self.s = settings
        self.store = store
        self.scheduler = scheduler
        self.hy2 = hy2
        self._stop = asyncio.Event()
        self._worker: asyncio.Task | None = None
        self._helper_lock = asyncio.Lock()
        self._acquire_lock = asyncio.Lock()

    async def start(self) -> None:
        await self._restore_active()
        self._worker = asyncio.create_task(self._loop(), name="client-sessions")

    async def stop(self) -> None:
        self._stop.set()
        if self._worker is not None:
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
            self._worker = None

    def _credentials(self, session_id: str) -> Hy2Credentials:
        """确定性派生让相同 request_id 的重试可拿回同一凭据，数据库无需存明文。"""
        key = self.s.client_token.encode()

        def derive(label: str) -> str:
            digest = hmac.new(key, f"{label}:{session_id}".encode(), hashlib.sha256).digest()
            return base64.urlsafe_b64encode(digest).decode().rstrip("=")

        return Hy2Credentials(password=derive("auth"), obfs_password=derive("obfs"))

    async def acquire(
        self, request_id: str, ttl: int
    ) -> tuple[ClientSession, Hy2Credentials, str]:
        # 把 helper 启动也纳入幂等串行区，重复请求会等首个请求提交 ACTIVE。
        async with self._acquire_lock:
            async with self.store.lock:
                existing = self.store.get_session_by_request(request_id)
                if existing is not None:
                    original_ttl = round(existing.expires_at - existing.created_at)
                    if original_ttl != ttl:
                        raise SessionUnavailable("相同 request_id 的 ttl_seconds 与首次请求不一致")
                    lease = self.store.get(existing.lease_id)
                    if existing.state is ClientState.ACTIVE and existing.expires_at > now():
                        return (
                            existing,
                            self._credentials(existing.id),
                            lease.runner_ip if lease and lease.runner_ip else "",
                        )
                    if (
                        existing.state is ClientState.STARTING
                        and lease is not None
                        and lease.state is State.IN_USE
                        and not lease.unconfirmed
                    ):
                        # 进程曾在 helper 启动/HTTP 响应窗口崩溃；用派生凭据恢复同一会话。
                        session = existing
                        runner_ip = lease.runner_ip or ""
                    elif existing.state is ClientState.STARTING:
                        raise NoAvailableNode("原申请正在等待 runner 重连，请稍后用相同 request_id 重试")
                    else:
                        raise SessionUnavailable("request_id 已被使用且会话不再可恢复")
                else:
                    candidates = self.store.available_leases(ttl)
                    if not candidates:
                        raise NoAvailableNode("当前没有可完整覆盖所需时长的空闲节点")
                    lease = candidates[0]
                    session = self.store.create_session(lease, ttl, request_id)
                    runner_ip = lease.runner_ip or ""

            credentials = self._credentials(session.id)
            try:
                async with self._helper_lock:
                    await self.hy2.start(session.port, credentials)
            except Exception as exc:
                await self._abort_start(session.id, f"hy2_start_failed:{type(exc).__name__}")
                raise SessionUnavailable("独立 HY2 实例启动失败") from exc

            activation_error: Exception | None = None
            async with self.store.lock:
                current = self.store.get_session(session.id)
                lease = self.store.get(session.lease_id)
                if (
                    current is None
                    or current.state is not ClientState.STARTING
                    or lease is None
                    or lease.state is not State.IN_USE
                ):
                    should_abort = True
                else:
                    current.state = ClientState.ACTIVE
                    try:
                        self.store.persist_session(current)
                    except Exception as exc:
                        current.state = ClientState.STARTING
                        activation_error = exc
                        should_abort = True
                    else:
                        session = current
                        should_abort = False
            if should_abort:
                await self._abort_start(
                    session.id,
                    "activation_persist_failed" if activation_error else "runner_changed_during_start",
                )
                raise SessionUnavailable("节点在 HY2 启动提交期间已不可用") from activation_error

            log.info(
                "客户端会话已激活 session=%s lease=%s port=%d ip=%s ttl=%d",
                session.id[:8], session.lease_id[:8], session.port, runner_ip, ttl,
            )
            return session, credentials, runner_ip

    async def release(self, session_id: str, reason: str = "client_release") -> ClientSession:
        async with self.store.lock:
            session = self.store.get_session(session_id)
            if session is None:
                raise KeyError(session_id)
            if session.state in (ClientState.RELEASED, ClientState.FAILED):
                return session
            self.store.request_session_release(session, reason)
            lease = self.store.get(session.lease_id)
            if lease is not None and lease.state not in (State.DRAINING, State.DEAD):
                self.scheduler.drain(lease, reason)
        await self._stop_hy2(session_id)
        async with self.store.lock:
            return self.store.get_session(session_id) or session

    async def _abort_start(self, session_id: str, reason: str) -> None:
        """只有 helper 明确确认 inactive 后，才允许将启动失败写成安全终态。"""
        async with self.store.lock:
            session = self.store.get_session(session_id)
            if session is None:
                return
            port = session.port
        try:
            async with self._helper_lock:
                await self.hy2.stop(port)
        except Exception:
            async with self.store.lock:
                session = self.store.get_session(session_id)
                if session is not None:
                    self.store.request_session_release(session, reason)
                    lease = self.store.get(session.lease_id)
                    if lease is not None and lease.state not in (State.DRAINING, State.DEAD):
                        self.scheduler.drain(lease, reason)
            log.exception("启动失败后的 HY2 停止也失败，端口保持隔离 session=%s", session_id[:8])
            return

        async with self.store.lock:
            session = self.store.get_session(session_id)
            if session is None:
                return
            lease = self.store.get(session.lease_id)
            self.store.ports.mark_hy2_stopped(
                session.port,
                lease_still_holds=lease is not None and lease.state is not State.DEAD,
            )
            session.state = ClientState.FAILED
            session.reason = reason
            session.released_at = now()
            session.hy2_stopped_at = now()
            self.store.persist_session(session)

    async def _stop_hy2(self, session_id: str) -> None:
        async with self.store.lock:
            session = self.store.get_session(session_id)
            if session is None or session.hy2_stopped_at is not None:
                return
            port = session.port
        async with self._helper_lock:
            await self.hy2.stop(port)
        async with self.store.lock:
            session = self.store.get_session(session_id)
            if session is not None and session.hy2_stopped_at is None:
                lease = self.store.get(session.lease_id)
                self.store.ports.mark_hy2_stopped(
                    session.port,
                    lease_still_holds=lease is not None and lease.state is not State.DEAD,
                )
                session.hy2_stopped_at = now()
                self.store.persist_session(session)

    async def _restore_active(self) -> None:
        async with self.store.lock:
            active = [
                session for session in self.store.sessions()
                if session.state is ClientState.ACTIVE
            ]
        for session in active:
            try:
                async with self._helper_lock:
                    await self.hy2.ensure(session.port)
            except Hy2Error:
                log.exception("恢复 HY2 失败，回收会话 %s", session.id[:8])
                await self.release(session.id, "hy2_restore_failed")

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("客户端会话维护 tick 异常")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    async def _tick(self) -> None:
        t = now()
        async with self.store.lock:
            sessions = [s for s in self.store.sessions() if s.state in CLIENT_LIVE_STATES]
            actions: list[tuple[str, str]] = []
            for session in sessions:
                lease = self.store.get(session.lease_id)
                if session.state is ClientState.STARTING and t - session.created_at > 30:
                    actions.append((session.id, "start_timeout"))
                elif session.state is ClientState.ACTIVE and session.expires_at <= t:
                    actions.append((session.id, "client_timeout"))
                elif session.state is ClientState.ACTIVE and (
                    lease is None or lease.state is not State.IN_USE
                ):
                    actions.append((session.id, "runner_unavailable"))
                elif session.state is ClientState.RELEASING:
                    actions.append((session.id, session.reason or "releasing"))

        for session_id, reason in actions:
            try:
                await self.release(session_id, reason)
            except KeyError:
                continue
            except Hy2Error:
                # 入口停止失败时保留 RELEASING 和 quarantined，下一 tick 重试。
                log.exception("停止会话 HY2 失败，将重试 session=%s", session_id[:8])

        async with self.store.lock:
            t = now()
            for session in self.store.sessions():
                if session.state is not ClientState.RELEASING:
                    continue
                lease = self.store.get(session.lease_id)
                if (
                    session.hy2_stopped_at is not None
                    and lease is not None
                    and lease.state is not State.DEAD
                    and session.release_at is not None
                    and t - session.release_at > self.s.client_release_grace
                ):
                    self.scheduler.kill(lease, "client_release_grace", cancel_run=True)
                if session.hy2_stopped_at is not None and (
                    lease is None or lease.state is State.DEAD
                ):
                    session.state = ClientState.RELEASED
                    session.released_at = t
                    self.store.persist_session(session)
                    log.info("客户端会话已释放 session=%s", session.id[:8])
