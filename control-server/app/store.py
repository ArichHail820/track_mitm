"""租约存储 + 端口池。

存储策略:内存 dict 是工作副本,SQLite(WAL)是持久镜像。只在**状态迁移**时
写库,心跳的 last_hb 不落盘 —— 它本来就是易失信息,重启后靠节点心跳重新认领
(见 scheduler 的冷启动收敛窗口),没必要每秒钟拿 fsync 去换。

端口池:free 集合 + cooldown 队列。租约结束时端口先进冷却再归还,避免上一代
节点的 TCP 连接还没散场,新一代就绑同一端口,导致 mihomo 把两代流量混在一起。
"""
from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from typing import Iterable, Iterator

from .config import Settings
from .models import (
    CLIENT_LIVE_STATES,
    INFLIGHT_STATES,
    ClientSession,
    ClientState,
    Lease,
    State,
    now,
)

log = logging.getLogger("store")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS leases (
    id             TEXT PRIMARY KEY,
    state          TEXT    NOT NULL,
    run_id         TEXT,
    runner_ip      TEXT,
    port           INTEGER,
    created_at     REAL    NOT NULL,
    registered_at  REAL,
    in_use_since   REAL,
    last_hb        REAL,
    drain_at       REAL,
    dead_at        REAL,
    reason         TEXT,
    register_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_leases_state ON leases(state);
CREATE INDEX IF NOT EXISTS idx_leases_dead_at ON leases(dead_at);

CREATE TABLE IF NOT EXISTS client_sessions (
    id             TEXT PRIMARY KEY,
    request_id     TEXT    NOT NULL,
    lease_id       TEXT    NOT NULL,
    port           INTEGER NOT NULL,
    state          TEXT    NOT NULL,
    created_at     REAL    NOT NULL,
    expires_at     REAL    NOT NULL,
    release_at     REAL,
    released_at    REAL,
    hy2_stopped_at REAL,
    reason         TEXT
);
CREATE INDEX IF NOT EXISTS idx_client_sessions_state
    ON client_sessions(state);
CREATE INDEX IF NOT EXISTS idx_client_sessions_lease
    ON client_sessions(lease_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_client_sessions_request
    ON client_sessions(request_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_client_sessions_live_lease
    ON client_sessions(lease_id)
    WHERE state IN ('starting', 'active', 'releasing');
"""

_COLS = (
    "id", "state", "run_id", "runner_ip", "port", "created_at", "registered_at",
    "in_use_since", "last_hb", "drain_at", "dead_at", "reason", "register_count",
)
_SESSION_COLS = (
    "id", "request_id", "lease_id", "port", "state", "created_at", "expires_at",
    "release_at", "released_at", "hy2_stopped_at", "reason",
)


class PortPool:
    """端口分配器。所有方法都在 Store 的锁保护下调用,自身不加锁。"""

    def __init__(self, settings: Settings) -> None:
        self._all = set(settings.all_ports)
        self._cooldown_secs = settings.port_cooldown
        self._free: set[int] = set()
        self._held: set[int] = set()
        self._cooling: dict[int, float] = {}
        self._quarantined: set[int] = set()

    def rebuild(self, held: Iterable[int]) -> None:
        """按当前活跃租约重建占用情况(冷启动 / 一致性修复用)。"""
        self._held = {p for p in held if p in self._all}
        self._cooling = {}
        self._free = self._all - self._held - self._quarantined

    def acquire(self) -> int | None:
        self._tick()
        if not self._free:
            return None
        # 取最小值而不是随机:让端口分布稳定可读,排障时好认
        port = min(self._free)
        self._free.discard(port)
        self._held.add(port)
        return port

    def release(self, port: int | None) -> None:
        if port is None or port not in self._all:
            return
        self._held.discard(port)
        if port in self._quarantined:
            # HY2 未确认停止时绝不能复用同号端口，否则旧凭据会跨代接入新 runner。
            self._cooling.pop(port, None)
            self._free.discard(port)
            return
        self._cooling[port] = now() + self._cooldown_secs

    def quarantine(self, port: int | None) -> None:
        if port is None or port not in self._all:
            return
        self._quarantined.add(port)
        self._cooling.pop(port, None)
        self._free.discard(port)

    def mark_hy2_stopped(self, port: int, *, lease_still_holds: bool) -> None:
        if port not in self._all:
            return
        self._quarantined.discard(port)
        if lease_still_holds:
            self._held.add(port)
            self._free.discard(port)
        else:
            self._held.discard(port)
            self._cooling[port] = now() + self._cooldown_secs

    def _tick(self) -> None:
        t = now()
        done = [p for p, ready in self._cooling.items() if ready <= t]
        for p in done:
            del self._cooling[p]
            if p not in self._held and p not in self._quarantined:
                self._free.add(p)

    def stats(self) -> dict:
        self._tick()
        return {
            "total": len(self._all),
            "held": len(self._held),
            "cooling": len(self._cooling),
            "quarantined": len(self._quarantined),
            "free": len(self._free),
        }

    def is_held_by_pool(self, port: int) -> bool:
        return port in self._held

    def force_hold(self, port: int) -> bool:
        """冷启动认领时用:把节点自报的端口强行标记为占用。"""
        if port not in self._all:
            return False
        self._cooling.pop(port, None)
        self._free.discard(port)
        self._held.add(port)
        return True


class Store:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.lock = asyncio.Lock()
        self.ports = PortPool(settings)
        self._leases: dict[str, Lease] = {}
        self._sessions: dict[str, ClientSession] = {}
        self._db: sqlite3.Connection | None = None

    # ---------- 生命周期 ----------

    def open(self) -> int:
        path = self.settings.db_path
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        # NORMAL:崩溃最多丢最后几毫秒的写,换来不阻塞事件循环的写入延迟。
        # 丢失的那点状态本来就能靠节点心跳重建,这个取舍是划算的。
        db.execute("PRAGMA synchronous=NORMAL")
        db.executescript(_SCHEMA)
        self._db = db
        adopted = self._load()
        self._load_sessions()
        return adopted

    def close(self) -> None:
        if self._db is not None:
            try:
                self._db.close()
            finally:
                self._db = None

    def _load(self) -> int:
        assert self._db is not None
        rows = self._db.execute(
            "SELECT * FROM leases WHERE state != ?", (State.DEAD.value,)
        ).fetchall()
        adopted = 0
        for r in rows:
            lease = Lease(
                id=r["id"],
                state=State(r["state"]),
                run_id=r["run_id"],
                runner_ip=r["runner_ip"],
                port=r["port"],
                created_at=r["created_at"],
                registered_at=r["registered_at"],
                in_use_since=r["in_use_since"],
                last_hb=r["last_hb"],
                drain_at=r["drain_at"],
                dead_at=r["dead_at"],
                reason=r["reason"],
                register_count=r["register_count"] or 0,
            )
            # 重启后一律标为待认领:服务器不能假设这些节点还活着,
            # 也不能假设它们已经死了。真相只能由心跳带回来。
            lease.unconfirmed = True
            # 心跳时间重置到"现在",这样收敛窗口内不会被 hb 超时误杀
            lease.last_hb = now()
            self._leases[lease.id] = lease
            adopted += 1
        self.ports.rebuild(
            l.port for l in self._leases.values() if l.port is not None and l.is_inflight
        )
        return adopted

    def _load_sessions(self) -> None:
        assert self._db is not None
        rows = self._db.execute(
            "SELECT * FROM client_sessions ORDER BY created_at DESC LIMIT 1000"
        ).fetchall()
        for r in rows:
            session = ClientSession(
                id=r["id"],
                request_id=r["request_id"],
                lease_id=r["lease_id"],
                port=r["port"],
                state=ClientState(r["state"]),
                created_at=r["created_at"],
                expires_at=r["expires_at"],
                release_at=r["release_at"],
                released_at=r["released_at"],
                hy2_stopped_at=r["hy2_stopped_at"],
                reason=r["reason"],
            )
            # STARTING 凭据可由 session_id 确定性恢复；保持状态让相同 request_id 重试。
            self._sessions[session.id] = session
            if session.hy2_stopped_at is None:
                self.ports.quarantine(session.port)
            self.persist_session(session)

    # ---------- 持久化 ----------

    def persist(self, lease: Lease) -> None:
        if self._db is None:
            return
        vals = (
            lease.id, lease.state.value, lease.run_id, lease.runner_ip, lease.port,
            lease.created_at, lease.registered_at, lease.in_use_since, lease.last_hb,
            lease.drain_at, lease.dead_at, lease.reason, lease.register_count,
        )
        placeholders = ",".join("?" * len(_COLS))
        try:
            self._db.execute(
                f"INSERT INTO leases ({','.join(_COLS)}) VALUES ({placeholders}) "
                "ON CONFLICT(id) DO UPDATE SET "
                + ",".join(f"{c}=excluded.{c}" for c in _COLS if c != "id"),
                vals,
            )
        except sqlite3.Error:
            # 持久化失败不能拖垮控制面:内存状态仍然正确,最坏是重启后少认领几条
            log.exception("持久化 lease %s 失败", lease.id)

    def persist_session(self, session: ClientSession) -> None:
        """会话分配必须持久化成功，否则不能向客户端宣告占用成功。"""
        if self._db is None:
            return
        vals = (
            session.id, session.request_id, session.lease_id, session.port,
            session.state.value,
            session.created_at, session.expires_at, session.release_at,
            session.released_at, session.hy2_stopped_at, session.reason,
        )
        placeholders = ",".join("?" * len(_SESSION_COLS))
        try:
            self._db.execute(
                f"INSERT INTO client_sessions ({','.join(_SESSION_COLS)}) "
                f"VALUES ({placeholders}) ON CONFLICT(id) DO UPDATE SET "
                + ",".join(
                    f"{column}=excluded.{column}"
                    for column in _SESSION_COLS if column != "id"
                ),
                vals,
            )
        except sqlite3.Error:
            log.exception("持久化客户端会话 %s 失败", session.id)
            raise

    def purge_db(self, keep: int = 500) -> None:
        if self._db is None:
            return
        try:
            self._db.execute(
                "DELETE FROM leases WHERE state = ? AND id NOT IN "
                "(SELECT id FROM leases WHERE state = ? ORDER BY dead_at DESC LIMIT ?)",
                (State.DEAD.value, State.DEAD.value, keep),
            )
            self._db.execute(
                "DELETE FROM client_sessions WHERE state IN (?, ?) AND id NOT IN "
                "(SELECT id FROM client_sessions WHERE state IN (?, ?) "
                "ORDER BY released_at DESC LIMIT ?)",
                (
                    ClientState.RELEASED.value, ClientState.FAILED.value,
                    ClientState.RELEASED.value, ClientState.FAILED.value, keep,
                ),
            )
        except sqlite3.Error:
            log.exception("清理历史 lease 失败")

    # ---------- 查询 ----------

    def get(self, lease_id: str) -> Lease | None:
        return self._leases.get(lease_id)

    def all(self) -> list[Lease]:
        return list(self._leases.values())

    def of(self, *states: State) -> list[Lease]:
        want = set(states)
        return [l for l in self._leases.values() if l.state in want]

    def count(self, *states: State) -> int:
        want = set(states)
        return sum(1 for l in self._leases.values() if l.state in want)

    def inflight(self) -> list[Lease]:
        return [l for l in self._leases.values() if l.state in INFLIGHT_STATES]

    def count_inflight(self) -> int:
        return sum(1 for l in self._leases.values() if l.state in INFLIGHT_STATES)

    def iter_active(self) -> Iterator[Lease]:
        # 复制一份再迭代:reconcile 过程中会改状态
        for l in list(self._leases.values()):
            if l.state is not State.DEAD:
                yield l

    def port_owner(self, port: int) -> Lease | None:
        for l in self._leases.values():
            if l.port == port and l.state in INFLIGHT_STATES:
                return l
        return None

    def active_ports(self) -> list[int]:
        return sorted(l.port for l in self._leases.values()
                      if l.port is not None and l.state is State.IN_USE)

    def get_session(self, session_id: str) -> ClientSession | None:
        return self._sessions.get(session_id)

    def get_session_by_request(self, request_id: str) -> ClientSession | None:
        return next(
            (session for session in self._sessions.values() if session.request_id == request_id),
            None,
        )

    def sessions(self) -> list[ClientSession]:
        return list(self._sessions.values())

    def live_session_for_lease(self, lease_id: str) -> ClientSession | None:
        return next(
            (
                session for session in self._sessions.values()
                if session.lease_id == lease_id and session.state in CLIENT_LIVE_STATES
            ),
            None,
        )

    def has_live_session(self, lease_id: str) -> bool:
        return self.live_session_for_lease(lease_id) is not None

    def available_leases(self, ttl: float) -> list[Lease]:
        """返回能完整覆盖请求 TTL 和回收余量的空闲 runner。"""
        required_remaining = ttl + self.settings.drain_timeout + 30.0
        return sorted(
            (
                lease for lease in self._leases.values()
                if lease.state is State.IN_USE
                and not lease.unconfirmed
                and lease.port is not None
                and not self.has_live_session(lease.id)
                and self.settings.hard_lifetime - lease.in_use_age() >= required_remaining
            ),
            key=lambda lease: lease.in_use_since or lease.created_at,
        )

    # ---------- 变更 ----------

    def create(self) -> Lease:
        lease = Lease()
        self._leases[lease.id] = lease
        self.persist(lease)
        return lease

    def create_session(self, lease: Lease, ttl: float, request_id: str) -> ClientSession:
        if lease.port is None or self.has_live_session(lease.id):
            raise RuntimeError("runner 已被占用或没有端口")
        if self.get_session_by_request(request_id) is not None:
            raise RuntimeError("request_id 已存在")
        session = ClientSession(
            request_id=request_id,
            lease_id=lease.id,
            port=lease.port,
            expires_at=now() + ttl,
        )
        self._sessions[session.id] = session
        self.ports.quarantine(session.port)
        try:
            self.persist_session(session)
        except Exception:
            self._sessions.pop(session.id, None)
            self.ports.mark_hy2_stopped(session.port, lease_still_holds=True)
            raise
        return session

    def request_session_release(self, session: ClientSession, reason: str) -> None:
        if session.state not in CLIENT_LIVE_STATES:
            return
        session.state = ClientState.RELEASING
        session.release_at = session.release_at or now()
        session.reason = reason
        self.persist_session(session)

    def request_release_for_lease(self, lease_id: str, reason: str) -> None:
        session = self.live_session_for_lease(lease_id)
        if session is not None:
            self.request_session_release(session, reason)

    def prune_memory(self) -> int:
        """把过了保留期的 DEAD 租约从内存里丢掉,防止长跑进程无限膨胀。"""
        t = now()
        cutoff = self.settings.dead_retention
        gone = [
            lid for lid, l in self._leases.items()
            if l.state is State.DEAD and l.dead_at is not None and t - l.dead_at > cutoff
        ]
        for lid in gone:
            del self._leases[lid]
        session_gone = [
            sid for sid, session in self._sessions.items()
            if session.state in (ClientState.RELEASED, ClientState.FAILED)
            and session.released_at is not None
            and t - session.released_at > cutoff
        ]
        for sid in session_gone:
            del self._sessions[sid]
        return len(gone) + len(session_gone)
