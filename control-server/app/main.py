"""FastAPI 应用:节点接口 + 运维接口。

节点侧只需要 4 个调用,节点脚本因此可以极简 —— 不再有旧方案里"随机挑端口、
TCP 预检、grep 日志、失败重试 60 次"那一整套盲抢逻辑,端口由服务器直接分配。

  POST /v1/register   -> 拿端口(服务器分配,无竞态)
  POST /v1/ready      -> 隧道通了,进入"使用中"
  POST /v1/heartbeat  -> 每 5s 一次,回收指令搭这趟车下发
  POST /v1/bye        -> 主动告别,立刻归还名额
"""
from __future__ import annotations

import contextlib
import logging
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .auth import Authenticator, admin_auth_dep, client_auth_dep, node_auth_dep
from .config import Settings, load_settings
from .ghclient import GitHubClient
from .hy2 import Hy2Controller
from .models import ClientState, Lease, State, now
from .scheduler import Scheduler
from .sessions import NoAvailableNode, SessionManager, SessionUnavailable
from .store import Store

log = logging.getLogger("api")


# --------------------------------------------------------------------- 请求模型

class RegisterIn(BaseModel):
    lease_id: str = Field(min_length=8, max_length=64)
    run_id: str | None = Field(default=None, max_length=32)
    runner_ip: str | None = Field(default=None, max_length=64)
    rebind: bool = False


class ReadyIn(BaseModel):
    lease_id: str = Field(min_length=8, max_length=64)
    port: int


class HeartbeatIn(BaseModel):
    lease_id: str = Field(min_length=8, max_length=64)
    run_id: str | None = Field(default=None, max_length=32)
    port: int | None = None
    node_phase: str = Field(default="pending", max_length=16)


class ByeIn(BaseModel):
    lease_id: str = Field(min_length=8, max_length=64)
    reason: str = Field(default="node_exit", max_length=64)


class AcquireIn(BaseModel):
    request_id: str = Field(min_length=16, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
    ttl_seconds: int | None = Field(default=None, ge=30)


class ReleaseIn(BaseModel):
    session_id: str = Field(min_length=16, max_length=64)


class TargetIn(BaseModel):
    n_target: int | None = None


class PauseIn(BaseModel):
    paused: bool


# --------------------------------------------------------------------- 辅助

def _recycle(reason: str, http: int = 200) -> JSONResponse:
    """统一的"状态 3"应答。节点看到 action=recycle 就结束任务。"""
    return JSONResponse(
        status_code=http,
        content={"action": "recycle", "state": "recycle", "reason": reason},
    )


def _ctx(request: Request) -> tuple[Settings, Store, Scheduler]:
    st = request.app.state
    return st.settings, st.store, st.scheduler


def _sessions(request: Request) -> SessionManager:
    return request.app.state.sessions


# --------------------------------------------------------------------- 节点路由

node_router = APIRouter(prefix="/v1", dependencies=[Depends(node_auth_dep)])


@node_router.post("/register")
async def register(body: RegisterIn, request: Request) -> Any:
    settings, store, sched = _ctx(request)
    async with store.lock:
        lease = store.get(body.lease_id)
        if lease is None:
            # 未知租约一定要让节点立刻退出:否则被扫到接口的人可以无限申请端口
            log.warning("register 未知租约 %s", body.lease_id[:8])
            raise HTTPException(status.HTTP_409_CONFLICT, "lease_unknown")
        if lease.state is State.DEAD:
            raise HTTPException(status.HTTP_409_CONFLICT, f"lease_dead:{lease.reason}")
        if lease.state is State.DRAINING:
            return _recycle(lease.reason or "draining")
        if lease.state is State.IN_USE:
            raise HTTPException(status.HTTP_409_CONFLICT, "lease_already_in_use")

        if body.run_id:
            lease.run_id = body.run_id
        if body.runner_ip:
            lease.runner_ip = body.runner_ip
        lease.last_hb = now()
        lease.unconfirmed = False

        # 已是 PENDING 且不要求换端口 -> 幂等重放,原样返回
        if lease.state is State.PENDING and not body.rebind:
            store.persist(lease)
            return _register_ok(settings, lease)

        if body.rebind:
            if lease.register_count >= 3:
                sched.drain(lease, "bind_failed")
                return _recycle("bind_failed")
            log.info("lease=%s 请求换端口(原 %s)", lease.id[:8], lease.port)
            store.ports.release(lease.port)
            lease.port = None

        port = store.ports.acquire()
        if port is None:
            # 端口池被冷却占满。判死并归还名额,让 reconcile 稍后重新派单。
            sched.kill(lease, "no_free_port", cancel_run=True)
            raise HTTPException(status.HTTP_409_CONFLICT, "no_free_port")

        lease.port = port
        lease.state = State.PENDING
        lease.registered_at = now()
        lease.register_count += 1
        lease.reason = None
        store.persist(lease)
        log.info("register lease=%s ip=%s run=%s -> port=%d (第 %d 次)",
                 lease.id[:8], lease.runner_ip, lease.run_id, port, lease.register_count)
        return _register_ok(settings, lease)


def _register_ok(s: Settings, lease: Lease) -> dict:
    return {
        "action": "keep",
        "state": lease.node_state(),          # standby = 2 待使用
        "port": lease.port,
        "heartbeat_interval": s.hb_interval,
        "warmup_deadline_in": round(
            max(0.0, s.warmup_timeout - lease.age()), 1
        ),
        "server_time": round(now(), 3),
    }


@node_router.post("/ready")
async def ready(body: ReadyIn, request: Request) -> Any:
    settings, store, _ = _ctx(request)
    async with store.lock:
        lease = store.get(body.lease_id)
        if lease is None:
            raise HTTPException(status.HTTP_409_CONFLICT, "lease_unknown")
        if lease.state in (State.DEAD, State.DRAINING):
            return _recycle(lease.reason or "draining")
        if lease.port != body.port:
            # 节点绑的不是我们分配的端口,拒绝入池 —— 否则会和别的租约撞车
            log.warning("ready 端口不符 lease=%s 分配=%s 上报=%s",
                        lease.id[:8], lease.port, body.port)
            raise HTTPException(status.HTTP_409_CONFLICT, "port_mismatch")

        if lease.state is State.PENDING:
            lease.state = State.IN_USE
            lease.in_use_since = now()
            lease.last_hb = now()
            lease.unconfirmed = False
            store.persist(lease)
            log.info("ready -> 使用中 lease=%s port=%d ip=%s",
                     lease.id[:8], lease.port, lease.runner_ip)

        return {
            "action": "keep",
            "state": lease.node_state(),      # in_use = 1 使用中
            "heartbeat_interval": settings.hb_interval,
            "soft_deadline_in": round(
                max(0.0, settings.soft_lifetime - lease.in_use_age()), 1
            ),
            "hard_deadline_in": round(
                max(0.0, settings.hard_lifetime - lease.in_use_age()), 1
            ),
        }


@node_router.post("/heartbeat")
async def heartbeat(body: HeartbeatIn, request: Request) -> Any:
    settings, store, sched = _ctx(request)
    async with store.lock:
        lease = store.get(body.lease_id)
        if lease is None:
            # DB 被清空 / 伪造的 lease_id。不认领未知租约:宁可让这个节点退出重开,
            # 也不接受一个来源不明的成员进池。
            log.warning("heartbeat 未知租约 %s(phase=%s),令其回收",
                        body.lease_id[:8], body.node_phase)
            return _recycle("unknown_lease", http=status.HTTP_409_CONFLICT)

        lease.last_hb = now()
        if body.run_id and not lease.run_id:
            lease.run_id = body.run_id

        if lease.unconfirmed:
            _adopt(store, sched, lease, body)

        if lease.state is State.DEAD:
            return _recycle(lease.reason or "dead", http=status.HTTP_409_CONFLICT)
        if lease.state is State.DRAINING:
            return _recycle(lease.reason or "recycle")

        resp: dict[str, Any] = {
            "action": "keep",
            "state": lease.node_state(),
            "heartbeat_interval": settings.hb_interval,
        }
        if lease.state is State.IN_USE:
            resp["soft_deadline_in"] = round(
                max(0.0, settings.soft_lifetime - lease.in_use_age()), 1
            )
        return resp


def _adopt(store: Store, sched: Scheduler, lease: Lease, body: HeartbeatIn) -> None:
    """服务器重启后的认领:真相由节点带回来。

    这是崩溃自愈的核心。服务器不假设这些租约还活着,也不假设它们已经死了 ——
    在收敛窗口内等心跳,谁回来谁算活着,窗口结束还没回来的才判死。
    """
    lease.unconfirmed = False
    reported = body.port

    if reported is not None:
        other = store.port_owner(reported)
        if other is not None and other.id != lease.id:
            # 同一端口被两个活跃租约声明。留下先建的,回收后建的。
            loser = other if other.created_at > lease.created_at else lease
            sched.drain(loser, "port_conflict_after_restart")
            log.warning("认领时端口 %d 冲突,回收较新的 lease=%s", reported, loser.id[:8])
            if loser is lease:
                return
        if store.ports.force_hold(reported):
            lease.port = reported
        else:
            log.warning("节点自报端口 %s 不在池内,回收 lease=%s", reported, lease.id[:8])
            sched.drain(lease, "port_out_of_pool")
            return

    if body.node_phase == "in_use" and lease.state is State.PENDING:
        lease.state = State.IN_USE
        lease.in_use_since = lease.in_use_since or now()

    store.persist(lease)
    log.info("认领 lease=%s state=%s port=%s ip=%s",
             lease.id[:8], lease.state.value, lease.port, lease.runner_ip)


# 注意返回类型必须标成 Response:标 None 会被 FastAPI 推断成 NoneType 作为响应模型,
# 而类对象是真值,于是它会断言"204 不允许有响应体"并在注册路由时直接崩掉。
@node_router.post("/bye", status_code=status.HTTP_204_NO_CONTENT, response_class=Response)
async def bye(body: ByeIn, request: Request) -> Response:
    _, store, sched = _ctx(request)
    async with store.lock:
        lease = store.get(body.lease_id)
        if lease is not None:
            # runner 已停 gost，但 GitHub job 尚未必 completed；进入确认队列后再归还名额。
            sched.kill(lease, f"bye:{body.reason}", cancel_run=True)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --------------------------------------------------------------------- 客户端独占会话路由

client_router = APIRouter(prefix="/v1/client", dependencies=[Depends(client_auth_dep)])


@client_router.post("/acquire", status_code=status.HTTP_201_CREATED)
async def client_acquire(body: AcquireIn, request: Request) -> Any:
    settings, _, _ = _ctx(request)
    ttl = body.ttl_seconds or settings.client_ttl_default
    if ttl > settings.client_ttl_max:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"ttl_seconds 不能超过 {settings.client_ttl_max}",
        )
    try:
        session, credentials, runner_ip = await _sessions(request).acquire(
            body.request_id, ttl
        )
    except NoAvailableNode as exc:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            headers={"Retry-After": "5"},
            content={"detail": "no_available_node", "message": str(exc)},
        )
    except SessionUnavailable as exc:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc)) from exc
    return {
        "session_id": session.id,
        "state": session.state.value,
        "hy2_host": settings.public_host,
        "hy2_port": session.port,
        "password": credentials.password,
        "obfs_password": credentials.obfs_password,
        "sni": settings.public_host,
        "runner_ip": runner_ip,
        "expires_at": session.expires_at,
        "ttl_seconds": round(session.expires_at - session.created_at),
    }


@client_router.get("/sessions/{session_id}")
async def client_session_state(session_id: str, request: Request) -> Any:
    _, store, _ = _ctx(request)
    async with store.lock:
        session = store.get_session(session_id)
        if session is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "session_unknown")
        return session.snapshot()


@client_router.post("/release", status_code=status.HTTP_202_ACCEPTED)
async def client_release(body: ReleaseIn, request: Request) -> Any:
    try:
        session = await _sessions(request).release(body.session_id)
    except KeyError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "session_unknown") from exc
    return session.snapshot()


# --------------------------------------------------------------------- 运维路由

admin_router = APIRouter(prefix="/v1/admin", dependencies=[Depends(admin_auth_dep)])


@admin_router.get("/state")
async def state(request: Request) -> Any:
    _, store, sched = _ctx(request)
    async with store.lock:
        snap = sched.snapshot()
        snap["leases"] = [
            l.snapshot() for l in sorted(store.all(), key=lambda x: x.created_at)
            if l.state is not State.DEAD
        ]
        snap["recent_dead"] = [
            l.snapshot() for l in sorted(
                (x for x in store.all() if x.state is State.DEAD),
                key=lambda x: x.dead_at or 0, reverse=True,
            )[:20]
        ]
        snap["client_sessions"] = [
            session.snapshot()
            for session in sorted(store.sessions(), key=lambda item: item.created_at)
            if session.state not in (ClientState.RELEASED, ClientState.FAILED)
        ]
        return snap


@admin_router.get("/ports")
async def ports(request: Request) -> Any:
    """当前真正在服务的端口。可以喂给外部工具做 mihomo 配置校对。"""
    _, store, _ = _ctx(request)
    async with store.lock:
        return {
            "in_use": store.active_ports(),
            "pool": store.ports.stats(),
            "allocated": [
                session.snapshot()
                for session in store.sessions()
                if session.state in (
                    ClientState.STARTING, ClientState.ACTIVE, ClientState.RELEASING
                )
            ],
        }


@admin_router.post("/pause")
async def pause(body: PauseIn, request: Request) -> Any:
    _, _, sched = _ctx(request)
    sched.paused = body.paused
    log.warning("调度已%s", "暂停" if body.paused else "恢复")
    return {"paused": sched.paused}


@admin_router.post("/target")
async def target(body: TargetIn, request: Request) -> Any:
    settings, _, sched = _ctx(request)
    if body.n_target is None:
        sched.target_override = None
    else:
        if not (0 < body.n_target <= settings.max_inflight):
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f"n_target 必须在 1..{settings.max_inflight} 之间",
            )
        sched.target_override = body.n_target
    log.warning("目标在线数调整为 %d", sched.n_target)
    return {"n_target": sched.n_target}


@admin_router.post("/drain/{lease_id}")
async def drain_one(lease_id: str, request: Request) -> Any:
    _, store, sched = _ctx(request)
    async with store.lock:
        lease = store.get(lease_id)
        if lease is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "lease_unknown")
        sched.drain(lease, "manual")
        return lease.snapshot()


@admin_router.post("/rotate-all")
async def rotate_all(request: Request) -> Any:
    """把当前所有在用节点标记回收,强制换一整批 IP。

    reconcile 的水位保护会让它们按批次逐步退出,不会一次全空。
    """
    _, store, sched = _ctx(request)
    async with store.lock:
        settings, _, _ = _ctx(request)
        n = 0
        for l in store.of(State.IN_USE):
            # 把 in_use_since 拨到刚过软寿命(而不是硬寿命)的位置,让它们走
            # 阶段 3 的正常轮换路径,从而继续受水位保护、逐批退出
            l.in_use_since = now() - settings.soft_lifetime - 1
            n += 1
        return {"marked": n}


# --------------------------------------------------------------------- 应用

def create_app() -> FastAPI:
    settings = load_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s",
    )

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        store = Store(settings)
        adopted = store.open()
        gh = GitHubClient(settings)
        sched = Scheduler(settings, store, gh)
        sched.begin_cold_start(adopted)
        sessions = SessionManager(settings, store, sched, Hy2Controller(settings))

        app.state.settings = settings
        app.state.store = store
        app.state.gh = gh
        app.state.scheduler = sched
        app.state.sessions = sessions
        app.state.auth = Authenticator(settings)

        await sched.start()
        await sessions.start()
        for w in settings.warnings():
            log.warning("配置提醒:%s", w)
        log.info(
            "控制服务端就绪:目标 %d 在线 / 水位下限 %d / 名额上限 %d / "
            "软寿命 %.0fs / 硬寿命 %.0fs / 端口池 %d-%d",
            settings.n_target, settings.n_min, settings.max_inflight,
            settings.soft_lifetime, settings.hard_lifetime,
            settings.base_port + 1, settings.base_port + settings.pool_size,
        )
        try:
            yield
        finally:
            await sessions.stop()
            await sched.stop()
            await gh.aclose()
            store.close()
            log.info("控制服务端已停止")

    app = FastAPI(
        title="GitHub Actions 出口 IP 池 · 控制服务端",
        version="1.0.0",
        lifespan=lifespan,
        docs_url=None,       # 不暴露交互文档,减少公网攻击面
        redoc_url=None,
        openapi_url=None,
    )
    app.include_router(node_router)
    app.include_router(client_router)
    app.include_router(admin_router)

    @app.get("/healthz")
    async def healthz(request: Request) -> Any:
        _, store, sched = _ctx(request)
        stale = (
            sched.last_tick_at > 0
            and now() - sched.last_tick_at > settings.reconcile_tick * 10
        )
        async with store.lock:
            body = {
                "ok": not stale,
                "in_use": store.count(State.IN_USE),
                "available": len(store.available_leases(settings.client_ttl_default)),
                "client_active": sum(
                    1 for session in store.sessions()
                    if session.state is ClientState.ACTIVE
                ),
                "inflight": store.count_inflight(),
                "paused": sched.paused,
                "last_tick_age": round(now() - sched.last_tick_at, 1) if sched.last_tick_at else None,
            }
        return JSONResponse(status_code=200 if not stale else 503, content=body)

    return app


app = create_app()
