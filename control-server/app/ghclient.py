"""GitHub REST API 客户端:派发 run、强杀 run、清理历史 run。

PAT 只存在于这台服务器上 —— 这是新架构相对旧 workflow 的一个净收益:
仓库里不再需要 secrets.PAT,guard / relay 两个 job 也一并消失。

配额估算(N_TARGET=16, SOFT_LIFETIME=600s):
  dispatch  ≈ 16 * 3600/600 = 96 次/小时
  删 run    ≈ 96 次/小时
  cancel    ≈ 偶发
合计约 200 次/小时,远低于 PAT 的 5000 次/小时。
"""
from __future__ import annotations

import asyncio
import logging

import httpx

from .config import Settings

log = logging.getLogger("github")


class GitHubError(RuntimeError):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__(f"GitHub API {status}: {detail}")
        self.status = status
        self.detail = detail


class GitHubClient:
    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self._repo = f"{settings.gh_owner}/{settings.gh_repo}"
        self._client = httpx.AsyncClient(
            base_url=settings.gh_api,
            timeout=httpx.Timeout(15.0, connect=8.0),
            headers={
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "Authorization": f"Bearer {settings.gh_token}",
                "User-Agent": "exit-node-control/1.0",
            },
        )
        self.rate_remaining: int | None = None

    async def aclose(self) -> None:
        await self._client.aclose()

    def _note_rate(self, resp: httpx.Response) -> None:
        raw = resp.headers.get("x-ratelimit-remaining")
        if raw is not None:
            try:
                self.rate_remaining = int(raw)
            except ValueError:
                pass
            if self.rate_remaining is not None and self.rate_remaining < 300:
                log.warning("GitHub API 配额偏低:剩余 %s", self.rate_remaining)

    async def _req(self, method: str, path: str, **kw) -> httpx.Response:
        resp = await self._client.request(method, path, **kw)
        self._note_rate(resp)
        return resp

    # ---------- 派发 ----------

    async def dispatch(self, lease_id: str) -> None:
        """触发一个 workflow run。

        注意:这个 API 返回 204 且**不返回 run_id**,所以 lease_id 必须由我们
        自己生成并作为 inputs 传下去,再由 runner 反过来上报 run_id。
        """
        path = (
            f"/repos/{self._repo}/actions/workflows/"
            f"{self.s.gh_workflow}/dispatches"
        )
        resp = await self._req(
            "POST", path,
            json={"ref": self.s.gh_ref, "inputs": {"lease_id": lease_id}},
        )
        if resp.status_code not in (201, 204):
            raise GitHubError(resp.status_code, resp.text[:300])

    # ---------- 查找 / 强杀 ----------

    async def find_run_id(self, lease_id: str) -> str | None:
        """workflow_dispatch 不返回 run_id，借助 run-name 中的 lease_id 反查。"""
        path = f"/repos/{self._repo}/actions/workflows/{self.s.gh_workflow}/runs"
        try:
            resp = await self._req(
                "GET", path, params={"event": "workflow_dispatch", "per_page": 100}
            )
        except httpx.HTTPError as exc:
            log.warning("按 lease %s 查找 run 失败: %s", lease_id[:8], exc)
            return None
        if resp.status_code != 200:
            log.warning("按 lease %s 查找 run 失败: HTTP %s", lease_id[:8], resp.status_code)
            return None
        expected = f"exit-node-{lease_id}"
        for run in resp.json().get("workflow_runs", []):
            if run.get("display_title") == expected:
                return str(run["id"])
        return None

    async def cancel_run(self, run_id: str) -> bool:
        """请求取消 run；返回只表示 GitHub 已接受，完成状态需另行确认。"""
        path = f"/repos/{self._repo}/actions/runs/{run_id}/cancel"
        try:
            resp = await self._req("POST", path)
        except httpx.HTTPError as e:
            log.warning("cancel run %s 网络失败: %s", run_id, e)
            return False
        if resp.status_code in (202, 409):
            # 409 = 已经结束了,视作成功
            return True
        log.warning("cancel run %s 失败: %s %s", run_id, resp.status_code, resp.text[:200])
        return False

    async def run_completed(self, run_id: str) -> bool | None:
        """True=已完成，False=仍运行，None=暂时无法确认。"""
        try:
            resp = await self._req("GET", f"/repos/{self._repo}/actions/runs/{run_id}")
        except httpx.HTTPError as exc:
            log.warning("查询 run %s 状态失败: %s", run_id, exc)
            return None
        if resp.status_code == 404:
            log.warning("查询 run %s 得到 404，保持隔离而不是假定已完成", run_id)
            return None
        if resp.status_code != 200:
            log.warning("查询 run %s 状态失败: HTTP %s", run_id, resp.status_code)
            return None
        return resp.json().get("status") == "completed"

    # ---------- 清理 ----------

    async def cleanup_completed(self, older_than_secs: float, limit: int = 50) -> int:
        """删掉已完成且超过保留期的 run,免得 Actions 页面被上千条记录淹掉。

        旧方案在 relay job 里做这件事,一旦 relay 挂了就永远不清理;放到服务器
        的 janitor 里更稳。
        """
        import datetime as _dt

        try:
            resp = await self._req(
                "GET", f"/repos/{self._repo}/actions/runs",
                params={"status": "completed", "per_page": 100},
            )
        except httpx.HTTPError as e:
            log.warning("列出历史 run 失败: %s", e)
            return 0
        if resp.status_code != 200:
            log.warning("列出历史 run 失败: %s", resp.status_code)
            return 0

        cutoff = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(seconds=older_than_secs)
        deleted = 0
        for run in resp.json().get("workflow_runs", []):
            if deleted >= limit:
                break
            updated = run.get("updated_at")
            if updated:
                try:
                    ts = _dt.datetime.fromisoformat(updated.replace("Z", "+00:00"))
                except ValueError:
                    continue
                if ts > cutoff:
                    continue
            try:
                d = await self._req("DELETE", f"/repos/{self._repo}/actions/runs/{run['id']}")
                if d.status_code in (204, 404):
                    deleted += 1
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.05)
        return deleted
