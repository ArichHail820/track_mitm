"""租约(lease)状态机的数据模型。

一个 lease 代表"一次出口节点的完整生命周期",从服务器决定 dispatch 一个
GitHub Actions run 开始,到该 runner 退出为止。lease_id 是把 dispatch 和
runner 缝起来的唯一手段 —— GitHub 的 workflow_dispatch API 返回 204 且不
给 run_id,所以必须由服务器先生成 ID 再作为 inputs 传下去。
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


class State(str, Enum):
    """服务器视角的 runner 内部状态。"""

    DISPATCHED = "dispatched"  # 已调 dispatch,runner 可能还在 GitHub 队列里排队
    PENDING = "pending"        # runner 已注册,正在装 gost / 建隧道
    IN_USE = "in_use"          # ready 已上报,隧道可分配给客户端
    DRAINING = "draining"      # 服务器已判死刑,等节点自己退出
    DEAD = "dead"              # 终态,端口与并发名额已归还


class ClientState(str, Enum):
    """浏览器侧独占会话状态，与 runner 生命周期严格分离。"""

    STARTING = "starting"      # 已原子占用 runner，正在启动独立 HY2 实例
    ACTIVE = "active"          # 客户端可通过该 HY2 端口使用 runner
    RELEASING = "releasing"    # 已停入口或正在停入口，等待 runner 完全退出
    RELEASED = "released"      # 正常终态
    FAILED = "failed"          # HY2 启动失败等异常终态


CLIENT_LIVE_STATES: tuple[ClientState, ...] = (
    ClientState.STARTING,
    ClientState.ACTIVE,
    ClientState.RELEASING,
)


#: 尚未结束、仍占用 GitHub 并发名额的状态
INFLIGHT_STATES: tuple[State, ...] = (
    State.DISPATCHED,
    State.PENDING,
    State.IN_USE,
    State.DRAINING,
)

#: 节点可见的三态。DISPATCHED 阶段节点还没跟服务器说过话,所以节点视角只有 3 个状态。
NODE_VISIBLE_STATE: dict[State, str] = {
    State.PENDING: "standby",   # 2 待使用
    State.IN_USE: "in_use",     # 1 使用中
    State.DRAINING: "recycle",  # 3 待回收
    State.DEAD: "recycle",
}


def now() -> float:
    """统一时间源。用墙钟而非 monotonic,因为状态要跨进程重启持久化。"""
    return time.time()


def new_lease_id() -> str:
    return uuid.uuid4().hex


@dataclass
class Lease:
    id: str = field(default_factory=new_lease_id)
    state: State = State.DISPATCHED
    run_id: str | None = None
    runner_ip: str | None = None
    port: int | None = None

    created_at: float = field(default_factory=now)
    registered_at: float | None = None
    in_use_since: float | None = None
    last_hb: float | None = None
    drain_at: float | None = None
    dead_at: float | None = None

    reason: str | None = None
    register_count: int = 0

    #: 仅内存字段:服务器重启后加载出来的租约,等节点心跳回来认领
    unconfirmed: bool = False
    #: 是否已经尝试过 cancel run,避免重复打 GitHub API
    cancel_requested: bool = False

    # ---------- 派生属性 ----------

    @property
    def is_inflight(self) -> bool:
        return self.state in INFLIGHT_STATES

    def age(self, t: float | None = None) -> float:
        return (t if t is not None else now()) - self.created_at

    def in_use_age(self, t: float | None = None) -> float:
        if self.in_use_since is None:
            return 0.0
        return (t if t is not None else now()) - self.in_use_since

    def hb_age(self, t: float | None = None) -> float:
        """距上次心跳多久。从未心跳过则退化为 lease 年龄。"""
        ref = self.last_hb if self.last_hb is not None else self.created_at
        return (t if t is not None else now()) - ref

    def node_state(self) -> str:
        return NODE_VISIBLE_STATE.get(self.state, "recycle")

    def snapshot(self, t: float | None = None) -> dict:
        t = t if t is not None else now()
        return {
            "lease_id": self.id,
            "state": self.state.value,
            "node_state": self.node_state(),
            "run_id": self.run_id,
            "runner_ip": self.runner_ip,
            "port": self.port,
            "age": round(self.age(t), 1),
            "in_use_age": round(self.in_use_age(t), 1) if self.in_use_since else None,
            "hb_age": round(self.hb_age(t), 1) if self.last_hb else None,
            "reason": self.reason,
            "register_count": self.register_count,
            "unconfirmed": self.unconfirmed,
        }


@dataclass
class ClientSession:
    """一个浏览器独占会话；凭据不持久化到 SQLite。"""

    id: str = field(default_factory=new_lease_id)
    request_id: str = ""
    lease_id: str = ""
    port: int = 0
    state: ClientState = ClientState.STARTING
    created_at: float = field(default_factory=now)
    expires_at: float = 0.0
    release_at: float | None = None
    released_at: float | None = None
    hy2_stopped_at: float | None = None
    reason: str | None = None

    @property
    def is_live(self) -> bool:
        return self.state in CLIENT_LIVE_STATES

    def snapshot(self, t: float | None = None) -> dict:
        t = t if t is not None else now()
        return {
            "session_id": self.id,
            "lease_id": self.lease_id,
            "port": self.port,
            "state": self.state.value,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "ttl_remaining": round(max(0.0, self.expires_at - t), 1),
            "release_at": self.release_at,
            "released_at": self.released_at,
            "reason": self.reason,
        }
