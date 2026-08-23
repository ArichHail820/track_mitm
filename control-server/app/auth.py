"""鉴权与限流。

控制面 API 必须公网可达(GitHub runner 要连它),所以这层不能省。选型:

1. **Bearer 共享密钥(必须)** —— `NODE_TOKEN` 放 GHA Secret,所有 /v1 节点接口
   都要带。配合服务器签发的不可猜 lease_id,构成双因子:光有 token 猜不到
   lease_id,光有 lease_id 过不了 token。
2. **GitHub OIDC(可选,默认关)** —— runner 用 `id-token: write` 换一个短命 JWT,
   服务器验签并校验 `repository` claim。它解决共享密钥的根本弱点:长期有效、
   泄露后无法感知。workflow 里**始终**会带上这个头,所以打开它不需要改 workflow,
   只要在服务器设 `OIDC_ENABLED=1` + `OIDC_REPOSITORY=owner/repo` 即可。

先上共享密钥、把 OIDC 留成一个开关,是因为 OIDC 依赖外部 JWKS 拉取:如果
GitHub 的 JWKS 端点抖动而配置又是强制校验,整个池子会补不进新节点。默认关、
可随时打开,是风险和收益的合理切分。
"""
from __future__ import annotations

import asyncio
import hmac
import logging
import time
from collections import deque

from fastapi import Header, HTTPException, Request, status

from .config import Settings

log = logging.getLogger("auth")

GITHUB_ISSUER = "https://token.actions.githubusercontent.com"
GITHUB_JWKS = f"{GITHUB_ISSUER}/.well-known/jwks"


def _bearer(header: str | None) -> str | None:
    if not header:
        return None
    parts = header.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip()


class RateLimiter:
    """按源 IP 的滑动窗口限流。

    正常负载:16 个节点各自独立 IP,每个 5s 一次心跳 = 12 次/分钟,离限额很远。
    它防的是某个失控 runner 疯狂重试把 reconcile 拖死。
    """

    def __init__(self, window: float, limit: int) -> None:
        self.window = window
        self.limit = limit
        self._hits: dict[str, deque[float]] = {}
        self._last_gc = 0.0

    def check(self, key: str) -> bool:
        t = time.monotonic()
        if t - self._last_gc > self.window:
            self._gc(t)
            self._last_gc = t
        dq = self._hits.setdefault(key, deque())
        while dq and t - dq[0] > self.window:
            dq.popleft()
        if len(dq) >= self.limit:
            return False
        dq.append(t)
        return True

    def _gc(self, t: float) -> None:
        for k in [k for k, dq in self._hits.items() if not dq or t - dq[-1] > self.window * 3]:
            self._hits.pop(k, None)


class Authenticator:
    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self.limiter = RateLimiter(settings.rate_limit_window, settings.rate_limit_max)
        self._jwk_client = None
        if settings.oidc_enabled:
            self._init_oidc()

    def _init_oidc(self) -> None:
        try:
            from jwt import PyJWKClient  # type: ignore
        except ImportError as e:  # pragma: no cover
            raise RuntimeError(
                "OIDC_ENABLED=1 需要安装 PyJWT[crypto]:pip install 'PyJWT[crypto]'"
            ) from e
        self._jwk_client = PyJWKClient(GITHUB_JWKS, cache_keys=True, lifespan=3600)
        log.info("OIDC 校验已启用,期望 repository=%s audience=%s",
                 self.s.oidc_repository, self.s.oidc_audience)

    # ---------- 节点接口 ----------

    async def require_node(self, request: Request, authorization: str | None) -> None:
        client_ip = request.client.host if request.client else "unknown"
        if not self.limiter.check(client_ip):
            raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "请求过于频繁")

        token = _bearer(authorization)
        if not token or not hmac.compare_digest(token, self.s.node_token):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "节点凭证无效")

        if self.s.oidc_enabled:
            await self._verify_oidc(request.headers.get("x-gha-oidc"))

    async def _verify_oidc(self, raw: str | None) -> None:
        if not raw:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "缺少 X-GHA-OIDC")
        try:
            claims = await asyncio.to_thread(self._decode_oidc, raw)
        except HTTPException:
            raise
        except Exception as e:
            log.warning("OIDC 验签失败: %s", e)
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "OIDC 令牌无效") from e

        repo = claims.get("repository")
        if repo != self.s.oidc_repository:
            raise HTTPException(
                status.HTTP_403_FORBIDDEN, f"OIDC repository 不匹配: {repo!r}"
            )

    def _decode_oidc(self, raw: str) -> dict:
        import jwt  # type: ignore

        assert self._jwk_client is not None
        signing_key = self._jwk_client.get_signing_key_from_jwt(raw)
        return jwt.decode(
            raw,
            signing_key.key,
            algorithms=["RS256"],
            audience=self.s.oidc_audience,
            issuer=GITHUB_ISSUER,
            options={"require": ["exp", "iat", "aud", "iss"]},
        )

    # ---------- 运维接口 ----------

    def require_admin(self, authorization: str | None) -> None:
        token = _bearer(authorization)
        if not token or not hmac.compare_digest(token, self.s.admin_token):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "运维凭证无效")


# FastAPI 依赖:实例挂在 app.state.auth 上(见 main.py 的 lifespan)
async def node_auth_dep(
    request: Request, authorization: str | None = Header(default=None)
) -> None:
    await request.app.state.auth.require_node(request, authorization)


def admin_auth_dep(request: Request, authorization: str | None = Header(default=None)) -> None:
    request.app.state.auth.require_admin(authorization)
