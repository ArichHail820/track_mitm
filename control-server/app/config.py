"""控制服务端配置。全部通过环境变量注入,启动时做一致性校验,配置错误直接拒绝启动。

设计取向:宁可启动失败也不要带着一份自相矛盾的容量参数跑起来 —— 那会直接
撞上 GitHub 的 20 并发上限并把整个池子锁死。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _s(name: str, default: str | None = None, *, required: bool = False) -> str:
    v = os.environ.get(name, default)
    if required and not v:
        raise RuntimeError(f"缺少必需的环境变量 {name}")
    return v or ""


def _i(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise RuntimeError(f"环境变量 {name}={raw!r} 不是合法整数") from e


def _f(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as e:
        raise RuntimeError(f"环境变量 {name}={raw!r} 不是合法数字") from e


def _b(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    # ---------- GitHub ----------
    gh_token: str            # PAT,权限只需 actions:write + contents:read(删 run 需要 repo)
    gh_owner: str
    gh_repo: str
    gh_workflow: str         # workflow 文件名
    gh_ref: str              # dispatch 的分支
    gh_api: str

    # ---------- 鉴权 ----------
    node_token: str          # 节点侧 Bearer 共享密钥
    admin_token: str         # 运维接口密钥
    client_token: str        # 本地客户端 acquire/release 共享密钥
    oidc_enabled: bool       # 是否额外校验 GitHub OIDC(第二因子)
    oidc_audience: str
    oidc_repository: str     # 期望的 repository claim,形如 owner/repo

    # ---------- 端口池 ----------
    base_port: int
    pool_size: int
    port_cooldown: float     # 端口归还前的冷却,避免跨代连接串台

    # ---------- 客户端独占会话 / HY2 ----------
    public_host: str
    client_ttl_default: int
    client_ttl_max: int
    client_release_grace: float
    hy2_helper: str
    hy2_command_timeout: float

    # ---------- 容量 ----------
    n_target: int            # 目标在线(使用中)节点数
    n_min: int               # 水位保护下限,低于它不做自愿轮换
    max_inflight: int        # 占用 GitHub 并发名额的租约上限

    # ---------- 时间参数(秒) ----------
    hb_interval: float       # 下发给节点的心跳间隔
    hb_timeout_in_use: float
    hb_timeout_pending: float
    warmup_timeout: float    # 待使用(PENDING)硬超时
    dispatch_timeout: float  # 已派发但 runner 始终没上线
    drain_timeout: float     # 待回收但节点不退出
    soft_lifetime: float     # 使用中软寿命,正常轮换周期
    hard_lifetime: float     # 使用中硬寿命,宽限期安全阀
    stuck_pending: float     # PENDING 卡住多久算触发反死锁
    t_warmup_est: float      # 从 dispatch 到 ready 的经验耗时,仅用于容量校验

    # ---------- 调度 ----------
    reconcile_tick: float
    cold_start_window: float
    dispatch_min_interval: float
    dispatch_burst: int
    dispatch_backoff: float

    # ---------- 杂项 ----------
    db_path: str
    janitor_interval: float
    janitor_keep: float      # 完成多久的 run 才允许删
    dead_retention: float    # DEAD 租约在内存里保留多久
    rate_limit_window: float
    rate_limit_max: int
    log_level: str

    # 运行期可调(不放 frozen 语义里,交给 scheduler 的可变状态)
    all_ports: tuple[int, ...] = field(default=())

    def validate(self) -> None:
        errs: list[str] = []

        # 端口池要同时容纳"在用的"和"正在冷却的"。冷却占用 = 归还速率 * 冷却时长,
        # 归还速率约等于轮换速率 N_TARGET / SOFT_LIFETIME。
        cooling = self.n_target / max(self.soft_lifetime, 1.0) * self.port_cooldown
        need = self.max_inflight + cooling
        if self.pool_size < need:
            errs.append(
                f"POOL_SIZE({self.pool_size}) 不足:需要 >= MAX_INFLIGHT({self.max_inflight})"
                f" + 冷却占用(~{cooling:.1f}) = {need:.1f},否则会出现注册时拿不到端口的租约"
            )
        if not (0 < self.n_min < self.n_target):
            errs.append(f"必须满足 0 < N_MIN({self.n_min}) < N_TARGET({self.n_target})")
        if self.n_target > self.max_inflight:
            errs.append(
                f"N_TARGET({self.n_target}) 不能大于 MAX_INFLIGHT({self.max_inflight})"
            )
        if self.max_inflight > 20:
            errs.append(
                f"MAX_INFLIGHT({self.max_inflight}) > 20,超出 GitHub 免费账号并发上限"
            )
        # 补位需要余量:目标 + 稳态在途 <= 上限。
        # 稳态在途由 Little's law 得出:λ = N_TARGET / SOFT_LIFETIME,在途 = λ * T_warmup。
        # T_warmup 实测 20~60s(dispatch 延迟 + GitHub 排队 + runner 启动 + 装 gost + 建隧道)。
        est_inflight = self.n_target * self.t_warmup_est / max(self.soft_lifetime, 1.0)
        if self.n_target + est_inflight > self.max_inflight:
            errs.append(
                f"容量不足:N_TARGET({self.n_target}) + 稳态在途(~{est_inflight:.1f}) "
                f"> MAX_INFLIGHT({self.max_inflight})。请降低 N_TARGET 或加大 SOFT_LIFETIME"
            )
        if self.hard_lifetime < self.soft_lifetime:
            errs.append(
                f"HARD_LIFETIME({self.hard_lifetime}) 必须 >= SOFT_LIFETIME({self.soft_lifetime})"
            )
        if self.hb_timeout_in_use < self.hb_interval * 2:
            errs.append(
                f"HB_TIMEOUT_IN_USE({self.hb_timeout_in_use}) 至少要是心跳间隔的 2 倍,"
                " 否则单次丢包就会误杀节点"
            )
        if self.cold_start_window < self.hb_timeout_in_use:
            errs.append(
                f"COLD_START_WINDOW({self.cold_start_window}) 必须 > "
                f"HB_TIMEOUT_IN_USE({self.hb_timeout_in_use}),否则服务器一重启就把在线节点全判死"
            )
        if self.stuck_pending >= self.warmup_timeout:
            errs.append(
                f"STUCK_PENDING({self.stuck_pending}) 应小于 WARMUP_TIMEOUT({self.warmup_timeout}),"
                " 否则反死锁逃生阀永远不会触发"
            )
        if self.oidc_enabled and not self.oidc_repository:
            errs.append("OIDC_ENABLED=1 时必须设置 OIDC_REPOSITORY=owner/repo")
        if not self.public_host:
            errs.append("PUBLIC_HOST 不能为空")
        if not (30 <= self.client_ttl_default <= self.client_ttl_max):
            errs.append(
                f"必须满足 30 <= CLIENT_TTL_DEFAULT({self.client_ttl_default}) "
                f"<= CLIENT_TTL_MAX({self.client_ttl_max})"
            )
        # acquire 只会选择剩余硬寿命足够的 runner；这里先保证单次会话理论上能装下。
        if self.client_ttl_max + self.drain_timeout + 30 > self.hard_lifetime:
            errs.append(
                f"CLIENT_TTL_MAX({self.client_ttl_max}) + DRAIN_TIMEOUT({self.drain_timeout}) "
                f"+ 30s 安全余量不能超过 HARD_LIFETIME({self.hard_lifetime})"
            )
        if self.client_release_grace < 0:
            errs.append("CLIENT_RELEASE_GRACE 不能为负数")

        if errs:
            raise RuntimeError("配置校验失败:\n  - " + "\n  - ".join(errs))

    def warnings(self) -> list[str]:
        """能跑但值得提醒的配置。

        和 validate() 的分工:validate 只拦"数学上必然出事"的组合,这里提醒
        "余量太薄、抖动一下就会出事"的组合。不把它做成硬拦截,是因为余量多少
        属于运维取舍,不该由程序替用户决定。
        """
        warns: list[str] = []
        est = self.n_target * self.t_warmup_est / max(self.soft_lifetime, 1.0)
        margin = self.max_inflight - (self.n_target + est)
        if margin < 1.5:
            warns.append(
                f"并发余量偏薄:峰值 inflight 约 {self.n_target + est:.1f},"
                f"距 MAX_INFLIGHT({self.max_inflight}) 仅 {margin:.1f} 个名额。"
                f"GitHub 排队一抖动就可能触发反死锁强制回收。"
                f"建议 N_TARGET 降到 {max(1, int(self.max_inflight - est - 1.5))} "
                f"或 SOFT_LIFETIME 加大到 "
                f"{self.n_target * self.t_warmup_est / max(self.max_inflight - self.n_target - 1.5, 0.1):.0f}s"
            )
        if self.max_inflight >= 20:
            warns.append(
                "MAX_INFLIGHT 已顶到账号并发上限 20:同账号其它仓库的 workflow 会挤占名额,"
                "建议留 1~2 个余量"
            )
        rotations_per_hour = self.n_target * 3600.0 / max(self.soft_lifetime, 1.0)
        if rotations_per_hour > 200:
            warns.append(
                f"轮换过于频繁:约 {rotations_per_hour:.0f} 次/小时,"
                f"意味着同量级的 dispatch + 删 run 调用。注意 PAT 的 5000 次/小时配额"
            )
        if self.hard_lifetime / max(self.soft_lifetime, 1.0) < 2:
            warns.append(
                f"HARD_LIFETIME({self.hard_lifetime:.0f}s) 相对 "
                f"SOFT_LIFETIME({self.soft_lifetime:.0f}s) 太近,水位保护的宽限空间很小,"
                "池子吃紧时会被硬寿命强行拉空"
            )
        return warns


def load_settings() -> Settings:
    base_port = _i("BASE_PORT", 20000)
    pool_size = _i("POOL_SIZE", 60)

    st = Settings(
        gh_token=_s("GH_TOKEN", required=True),
        gh_owner=_s("GH_OWNER", required=True),
        gh_repo=_s("GH_REPO", required=True),
        gh_workflow=_s("GH_WORKFLOW", "exit-node.yml"),
        gh_ref=_s("GH_REF", "main"),
        gh_api=_s("GH_API", "https://api.github.com").rstrip("/"),

        node_token=_s("NODE_TOKEN", required=True),
        admin_token=_s("ADMIN_TOKEN", required=True),
        client_token=_s("CLIENT_TOKEN", required=True),
        oidc_enabled=_b("OIDC_ENABLED", False),
        oidc_audience=_s("OIDC_AUDIENCE", "exit-node-pool"),
        oidc_repository=_s("OIDC_REPOSITORY", ""),

        base_port=base_port,
        pool_size=pool_size,
        port_cooldown=_f("PORT_COOLDOWN", 10.0),

        public_host=_s("PUBLIC_HOST", required=True),
        client_ttl_default=_i("CLIENT_TTL_DEFAULT", 600),
        client_ttl_max=_i("CLIENT_TTL_MAX", 1200),
        client_release_grace=_f("CLIENT_RELEASE_GRACE", 10.0),
        hy2_helper=_s("HY2_HELPER", "/usr/local/libexec/exit-node-hy2-control.py"),
        hy2_command_timeout=_f("HY2_COMMAND_TIMEOUT", 15.0),

        n_target=_i("N_TARGET", 16),
        n_min=_i("N_MIN", 12),
        max_inflight=_i("MAX_INFLIGHT", 19),

        hb_interval=_f("HB_INTERVAL", 5.0),
        hb_timeout_in_use=_f("HB_TIMEOUT_IN_USE", 15.0),
        hb_timeout_pending=_f("HB_TIMEOUT_PENDING", 30.0),
        warmup_timeout=_f("WARMUP_TIMEOUT", 300.0),
        dispatch_timeout=_f("DISPATCH_TIMEOUT", 300.0),
        drain_timeout=_f("DRAIN_TIMEOUT", 60.0),
        soft_lifetime=_f("SOFT_LIFETIME", 600.0),
        hard_lifetime=_f("HARD_LIFETIME", 1800.0),
        stuck_pending=_f("STUCK_PENDING", 90.0),
        t_warmup_est=_f("T_WARMUP_EST", 45.0),

        reconcile_tick=_f("RECONCILE_TICK", 2.0),
        cold_start_window=_f("COLD_START_WINDOW", 20.0),
        dispatch_min_interval=_f("DISPATCH_MIN_INTERVAL", 1.0),
        dispatch_burst=_i("DISPATCH_BURST", 4),
        dispatch_backoff=_f("DISPATCH_BACKOFF", 15.0),

        db_path=_s("DB_PATH", "/var/lib/exit-node-control/state.db"),
        janitor_interval=_f("JANITOR_INTERVAL", 300.0),
        janitor_keep=_f("JANITOR_KEEP", 600.0),
        dead_retention=_f("DEAD_RETENTION", 900.0),
        rate_limit_window=_f("RATE_LIMIT_WINDOW", 30.0),
        rate_limit_max=_i("RATE_LIMIT_MAX", 90),
        log_level=_s("LOG_LEVEL", "INFO").upper(),

        all_ports=tuple(range(base_port + 1, base_port + 1 + pool_size)),
    )
    st.validate()
    return st
