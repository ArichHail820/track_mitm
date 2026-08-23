"""reconcile 调度循环 —— 整个系统唯一的调度入口。

这是新架构和旧 workflow 最本质的区别:**声明式收敛,不是链式触发**。

旧方案里,下一轮节点由上一轮的 relay job 触发,是一条链:relay 失败 / 被限流 /
run 被清掉,整个池子就死到下一个 15 分钟 schedule。这里改成每 2 秒无条件跑一次
"看实际状态、算与期望的差、补差",幂等、无状态依赖、随便重启。

每个 tick 的四个阶段(顺序不能换):
  1. 扫超时  —— 只做状态迁移,把死的、过期的清出账本
  2. 反死锁  —— 名额打满且有 PENDING 卡住时的逃生阀
  3. 轮换    —— 软寿命到期 + 水位保护
  4. 补位    —— 按容量余量 dispatch

补位必须由这个循环统一发起,**绝不能让被回收的节点去触发下一个** —— 那就退化
成旧方案的链式接力了。
"""
from __future__ import annotations

import asyncio
import logging
import time

from .config import Settings
from .ghclient import GitHubClient
from .models import Lease, State, now
from .store import Store

log = logging.getLogger("sched")


class Scheduler:
    def __init__(self, settings: Settings, store: Store, gh: GitHubClient) -> None:
        self.s = settings
        self.store = store
        self.gh = gh

        # 运行期可调
        self.paused = False
        self.target_override: int | None = None

        self._stop = asyncio.Event()

        self._cold_start_until = 0.0
        self._last_dispatch = 0.0
        self._dispatch_blocked_until = 0.0
        self._cancel_queue: asyncio.Queue[str] = asyncio.Queue()
        self._dispatch_queue: asyncio.Queue[str] = asyncio.Queue()
        self._workers: list[asyncio.Task] = []

        self.stats = {
            "ticks": 0,
            "dispatched": 0,
            "dispatch_errors": 0,
            "drained": 0,
            "killed": 0,
            "started_at": now(),
        }
        self.last_tick_at: float = 0.0

    # ------------------------------------------------------------------ 生命周期

    @property
    def n_target(self) -> int:
        return self.target_override if self.target_override is not None else self.s.n_target

    def begin_cold_start(self, adopted: int) -> None:
        """冷启动收敛窗口。

        少了这一步,服务器一重启就会看到 in_use=0,立刻 dispatch 一整批新节点,
        而老节点还在线上跑 —— 瞬间打爆 20 并发。**冷启动必须先观察再行动。**
        """
        if adopted > 0:
            self._cold_start_until = now() + self.s.cold_start_window
            log.warning(
                "冷启动:从数据库认领 %d 条待确认租约,收敛窗口 %.0fs 内不派发也不回收",
                adopted, self.s.cold_start_window,
            )

    async def start(self) -> None:
        self._workers = [
            asyncio.create_task(self._loop(), name="reconcile"),
            asyncio.create_task(self._dispatch_loop(), name="dispatcher"),
            asyncio.create_task(self._cancel_loop(), name="canceller"),
            asyncio.create_task(self._janitor_loop(), name="janitor"),
        ]

    async def stop(self) -> None:
        self._stop.set()
        for t in self._workers:
            t.cancel()
        for t in self._workers:
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("worker %s 退出异常", t.get_name())
        self._workers = []

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                async with self.store.lock:
                    await self.reconcile()
            except asyncio.CancelledError:
                raise
            except Exception:
                # reconcile 抛异常绝不能让循环退出 —— 那等于控制面静默死亡
                log.exception("reconcile 异常,本 tick 跳过")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.s.reconcile_tick)
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------ 状态迁移原子操作

    def drain(self, lease: Lease, reason: str) -> None:
        """判死刑:标 DRAINING,等节点下次心跳拿到 recycle 后自己退出。

        服务器故意**不**主动 cancel —— 让节点优雅关掉 gost 再退,既省 API 配额
        又避免连接被硬切。只有失联或赖着不走才升级为强杀。
        """
        if lease.state in (State.DRAINING, State.DEAD):
            return
        lease.state = State.DRAINING
        lease.drain_at = now()
        lease.reason = reason
        self.store.persist(lease)
        self.stats["drained"] += 1
        log.info("drain lease=%s port=%s ip=%s reason=%s",
                 lease.id[:8], lease.port, lease.runner_ip, reason)

    def kill(self, lease: Lease, reason: str, *, cancel_run: bool = False) -> None:
        """终态:归还端口(进冷却)与并发名额。"""
        if lease.state is State.DEAD:
            return
        lease.state = State.DEAD
        lease.dead_at = now()
        lease.reason = reason
        self.store.ports.release(lease.port)
        self.store.persist(lease)
        self.stats["killed"] += 1
        log.info("kill  lease=%s port=%s ip=%s reason=%s",
                 lease.id[:8], lease.port, lease.runner_ip, reason)
        if cancel_run and lease.run_id and not lease.cancel_requested:
            lease.cancel_requested = True
            self._cancel_queue.put_nowait(lease.run_id)

    async def _cancel_loop(self) -> None:
        """异步强杀队列。放到后台是为了不让 GitHub API 的延迟卡住 reconcile。"""
        while not self._stop.is_set():
            try:
                run_id = await self._cancel_queue.get()
            except asyncio.CancelledError:
                raise
            try:
                ok = await self.gh.cancel_run(run_id)
                log.info("cancel run %s -> %s", run_id, "ok" if ok else "failed")
            except Exception:
                log.exception("cancel run %s 异常", run_id)

    # ------------------------------------------------------------------ 主逻辑

    async def reconcile(self) -> None:
        t = now()
        self.stats["ticks"] += 1
        self.last_tick_at = t

        # 冷启动收敛窗口:什么都不做,只等心跳把真相带回来
        if t < self._cold_start_until:
            return
        if self._cold_start_until:
            self._finish_cold_start(t)

        self._phase1_timeouts(t)

        if self.paused:
            return

        in_use = self.store.count(State.IN_USE)
        pending = self.store.count(State.PENDING)
        dispatched = self.store.count(State.DISPATCHED)
        inflight = self.store.count_inflight()

        in_use = self._phase2_break_deadlock(t, in_use, inflight, pending)
        in_use = self._phase3_rotate(t, in_use)
        self._phase4_refill(t, in_use, pending, dispatched)

        self.store.prune_memory()

    def _finish_cold_start(self, t: float) -> None:
        """收敛窗口结束:还没心跳回来认领的,判定为已死。"""
        self._cold_start_until = 0.0
        orphans = [l for l in self.store.iter_active() if l.unconfirmed]
        for l in orphans:
            # DISPATCHED 的孤儿可能只是还在 GitHub 队列里排队,给它走正常的
            # dispatch_timeout,别急着杀
            if l.state is State.DISPATCHED and l.age(t) < self.s.dispatch_timeout:
                l.unconfirmed = False
                continue
            self.kill(l, "orphan_after_restart", cancel_run=True)
        log.warning("冷启动收敛结束:清理 %d 条未认领租约,当前在线 %d",
                    len(orphans), self.store.count(State.IN_USE))

    # ---------- 阶段 1:扫超时 ----------

    def _phase1_timeouts(self, t: float) -> None:
        s = self.s
        for l in self.store.iter_active():
            if l.state is State.DISPATCHED:
                # runner 始终没上线:可能被 GitHub 挤掉了,也可能 dispatch 落到了
                # 一个立刻失败的 run。归还名额,让补位阶段重新派单。
                if l.age(t) > s.dispatch_timeout:
                    self.kill(l, "no_show")

            elif l.state is State.PENDING:
                if l.hb_age(t) > s.hb_timeout_pending:
                    self.kill(l, "lost_in_warmup", cancel_run=True)
                elif l.age(t) > s.warmup_timeout:
                    # 注册成功但隧道死活建不起来。节点还活着,给它优雅退出的机会。
                    self.drain(l, "warmup_timeout")

            elif l.state is State.IN_USE:
                if l.hb_age(t) > s.hb_timeout_in_use:
                    # 失联就没法通知它退出了,标 DRAINING 毫无意义 —— 直接判死
                    # 并 cancel run,免得僵尸 job 白占名额、白烧 Actions 分钟数。
                    self.kill(l, "lost", cancel_run=True)
                elif l.in_use_age(t) > s.hard_lifetime:
                    # 硬寿命:无条件,不看水位。这是保证 IP 一定会轮换的安全阀。
                    self.drain(l, "max_age")

            elif l.state is State.DRAINING:
                if l.drain_at is not None and t - l.drain_at > s.drain_timeout:
                    self.kill(l, "drain_timeout", cancel_run=True)

    # ---------- 阶段 2:反死锁 ----------

    def _phase2_break_deadlock(
        self, t: float, in_use: int, inflight: int, pending: int
    ) -> int:
        """打破"新 job 等名额、老节点等新 job ready"的互等死局。

        死锁场景:inflight 打满 -> 新 dispatch 的 job 在 GitHub 队列里等名额 ->
        服务器因水位保护不敢回收老节点(它在等新节点 ready)-> 新 job 永远等不到
        名额 -> 全池冻结。这里无条件牺牲最老的一个在用节点来腾名额。
        """
        if inflight < self.s.max_inflight:
            return in_use
        stuck = [
            l for l in self.store.of(State.PENDING, State.DISPATCHED)
            if l.age(t) > self.s.stuck_pending
        ]
        if not stuck or in_use == 0:
            return in_use
        victim = self._oldest_in_use()
        if victim is None:
            return in_use
        log.warning(
            "反死锁触发:inflight=%d 已满且 %d 个租约卡住 >%.0fs,强制回收 lease=%s",
            inflight, len(stuck), self.s.stuck_pending, victim.id[:8],
        )
        self.drain(victim, "break_deadlock")
        return in_use - 1

    # ---------- 阶段 3:软寿命轮换 ----------

    def _phase3_rotate(self, t: float, in_use: int) -> int:
        """按软寿命轮换,但受水位保护。

        这里是"ready 即使用中"和"零空窗"两个要求的调和点:因为数据面是 mihomo
        的 load-balance 组、节点本身可替换,所以不需要一对一的 make-before-break,
        只要保证池子规模不掉到 N_MIN 以下。到寿但水位不足就顺延到下个 tick,
        顺延的上限是 HARD_LIFETIME。
        """
        n_min = min(self.s.n_min, self.n_target - 1)
        if in_use <= n_min:
            return in_use
        for l in self._in_use_oldest_first():
            if in_use <= n_min:
                break
            if l.in_use_age(t) > self.s.soft_lifetime:
                self.drain(l, "rotate")
                in_use -= 1
        return in_use

    # ---------- 阶段 4:补位 ----------

    def _phase4_refill(self, t: float, in_use: int, pending: int, dispatched: int) -> None:
        """只做记账和入队,**不在这里 await 网络**。

        reconcile 全程持 store.lock,如果在锁内等 GitHub API(动辄数百毫秒、还要
        按 1s 节流),节点心跳就会被排在后面。所以派发交给后台 worker,这里只负责
        原子地占掉名额(创建 DISPATCHED 租约)。
        """
        if t < self._dispatch_blocked_until:
            return

        # 期望缺口:目标在线数减去"已在线 + 在路上"
        gap = self.n_target - (in_use + pending + dispatched)
        # 容量余量:不能突破 GitHub 并发上限
        room = self.s.max_inflight - self.store.count_inflight()
        want = min(gap, room, self.s.dispatch_burst)
        if want <= 0:
            return

        free_ports = self.store.ports.stats()["free"]
        if free_ports <= 0:
            log.warning("端口池无可用端口(均在冷却中),本 tick 不派发")
            return
        want = min(want, free_ports)

        for _ in range(want):
            lease = self.store.create()
            self._dispatch_queue.put_nowait(lease.id)
            log.info("排队派发 lease=%s (缺口 %d, 余量 %d)", lease.id[:8], gap, room)

    async def _dispatch_loop(self) -> None:
        """派发 worker:按 dispatch_min_interval 节流,失败则退避并归还名额。"""
        while not self._stop.is_set():
            lease_id = await self._dispatch_queue.get()

            wait = self.s.dispatch_min_interval - (time.monotonic() - self._last_dispatch)
            if wait > 0:
                await asyncio.sleep(wait)

            async with self.store.lock:
                lease = self.store.get(lease_id)
                # reconcile 可能已经把它超时清掉了
                if lease is None or lease.state is not State.DISPATCHED:
                    continue

            try:
                await self.gh.dispatch(lease_id)
            except Exception as e:
                async with self.store.lock:
                    cur = self.store.get(lease_id)
                    if cur is not None:
                        self.kill(cur, "dispatch_failed")
                    self.stats["dispatch_errors"] += 1
                    self._dispatch_blocked_until = now() + self.s.dispatch_backoff
                    # 退避期内已排队的请求全部作废,让下个 tick 重新按实际缺口决策
                    self._drain_dispatch_queue()
                log.error("dispatch 失败,%.0fs 内暂停派发: %s", self.s.dispatch_backoff, e)
                continue

            self._last_dispatch = time.monotonic()
            self.stats["dispatched"] += 1
            log.info("dispatch ok lease=%s", lease_id[:8])

    def _drain_dispatch_queue(self) -> None:
        while True:
            try:
                lease_id = self._dispatch_queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            lease = self.store.get(lease_id)
            if lease is not None and lease.state is State.DISPATCHED:
                self.kill(lease, "dispatch_aborted")

    # ---------- 工具 ----------

    def _in_use_oldest_first(self) -> list[Lease]:
        return sorted(
            self.store.of(State.IN_USE),
            key=lambda l: l.in_use_since or l.created_at,
        )

    def _oldest_in_use(self) -> Lease | None:
        xs = self._in_use_oldest_first()
        return xs[0] if xs else None

    # ------------------------------------------------------------------ janitor

    async def _janitor_loop(self) -> None:
        # 启动后先等一会儿,别和冷启动收敛抢资源
        await asyncio.sleep(30)
        while not self._stop.is_set():
            try:
                n = await self.gh.cleanup_completed(self.s.janitor_keep)
                if n:
                    log.info("janitor 删除 %d 条已完成 run", n)
                async with self.store.lock:
                    self.store.purge_db()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("janitor 异常")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.s.janitor_interval)
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------ 观测

    def snapshot(self) -> dict:
        t = now()
        by_state = {st.value: self.store.count(st) for st in State}
        return {
            "paused": self.paused,
            "cold_start_remaining": max(0.0, round(self._cold_start_until - t, 1)),
            "dispatch_blocked_for": max(0.0, round(self._dispatch_blocked_until - t, 1)),
            "n_target": self.n_target,
            "n_min": self.s.n_min,
            "max_inflight": self.s.max_inflight,
            "inflight": self.store.count_inflight(),
            "by_state": by_state,
            "active_ports": self.store.active_ports(),
            "ports": self.store.ports.stats(),
            "gh_rate_remaining": self.gh.rate_remaining,
            "last_tick_age": round(t - self.last_tick_at, 1) if self.last_tick_at else None,
            "stats": dict(self.stats),
        }
