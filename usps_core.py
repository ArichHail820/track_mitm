"""USPS 抓取核心逻辑(纯逻辑, 不含浏览器 / 不含代理)。

把 `大规模server_ads_手动.py` 里与浏览器无关的部分抽出来, 方便不同的
"流量产生方式"(patchright 拦截 / mitmproxy 中间人 / curl_cffi 重放)复用:

    获取单号  ->  组装 URL  ->  (外部产生流量)  ->  解析(m / d)  ->  提交结果

环境变量:
    NUM_TYPE           'mysql' 或 'big'(也可在构造 UspsScraper 时传入)
    BATCH              每批单号数量, 默认 34
    DANHAO_HOST        big 单号服务地址, 默认 43.130.27.95:8082
    DANHAO_HOST_MYSQL  mysql 单号服务地址, 默认 43.128.111.219:8082
"""

import asyncio
import os
import re
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import aiohttp
from pyquery import PyQuery as pq


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


DANHAO_SERVER_HOST = _env('DANHAO_HOST', 'kungfu.bj.cn:8082')
DANHAO_SERVER_HOST_MYSQL = _env('DANHAO_HOST_MYSQL', '43.128.111.219:8082')
DANHAO_PER_BATCH = _env_int('BATCH', 34)


# ============================== 全局 HTTP Session ==============================

_global_http_session: Optional[aiohttp.ClientSession] = None


def get_global_session() -> aiohttp.ClientSession:
    """全局共享的 aiohttp session(带连接池, keep-alive)。"""
    global _global_http_session
    if _global_http_session is None or _global_http_session.closed:
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=30,
            ttl_dns_cache=300,
            force_close=False,
            enable_cleanup_closed=True,
        )
        _global_http_session = aiohttp.ClientSession(connector=connector)
    return _global_http_session


async def close_global_session():
    global _global_http_session
    if _global_http_session and not _global_http_session.closed:
        await _global_http_session.close()


# ============================== 核心抓取逻辑 ==============================

class UspsScraper:
    """USPS 抓取核心逻辑。一个实例对应一种 (mode, num_type) 组合。

    mode:     'd'(桌面端 tools.usps.com) 或 'm'(手机端 m.usps.com)
    num_type: 'big' 或 'mysql'
    """

    def __init__(self, mode: str = 'd', num_type: str = 'big'):
        self.mode = mode
        self.num_type = num_type
        self.data_ready_event_dict: Dict[str, asyncio.Event] = {}

    # -------------------- 单号服务 IO --------------------

    async def 获取单号(self, num: int = DANHAO_PER_BATCH) -> List[str]:
        if self.num_type == 'mysql':
            url = f'http://{DANHAO_SERVER_HOST_MYSQL}/get_mysql_usps_num?num={num}'
        else:
            url = f'http://{DANHAO_SERVER_HOST}/get_big_usps_num?num={num}'

        session = get_global_session()
        timeout = aiohttp.ClientTimeout(total=30)
        try:
            async with session.get(url, timeout=timeout) as response:
                response.raise_for_status()
                text = await response.text()
            danhao_ls: List[str] = []
            for i in text.split(','):
                if '|' in i:
                    danhao_ls.append(i.split('|')[0])
            return danhao_ls
        except Exception as e:
            print(f"获取单号失败: {e}")
            return []

    async def 提交到缓存(self, data) -> bool:
        if self.num_type == 'mysql':
            url = f'http://{DANHAO_SERVER_HOST_MYSQL}/set_mysql_usps_num_res'
        else:
            url = f'http://{DANHAO_SERVER_HOST}/set_big_usps_num_res'
        session = get_global_session()
        timeout = aiohttp.ClientTimeout(total=10)
        for _ in range(3):
            try:
                async with session.post(url, json=data, timeout=timeout) as resp:
                    await resp.text()
                    return True
            except Exception as e:
                print(f"提交缓存失败: {e}")
                await asyncio.sleep(1)
        return False

    async def 原始请求提交到缓存(self, actions):
        try:
            cache_ls = [{"num": a[0], "res": a} for a in actions]
            await self.提交到缓存(cache_ls)
        except Exception as e:
            print(f"原始请求提交失败: {e}")

    # -------------------- URL 组装 / 响应判定 --------------------

    def build_url(self, danhao_ls: List[str]) -> str:
        labels = ','.join(danhao_ls)
        if self.mode == 'm':
            return f'https://m.usps.com/m/TrackConfirmAction?tLabels={labels}'
        return (
            f'https://tools.usps.com/go/TrackConfirmAction?tRef=fullpage'
            f'&tLc=2&text28777=&tLabels={labels}&tABt=false'
        )

    def is_target_response_url(self, url: str) -> bool:
        """判断某个【响应】的 URL 是否是我们要解析数据的目标。

        注意桌面端: go/TrackConfirmAction 会 302 跳到 /tracking/?...tLabels=,
        真正带数据的响应在 /tracking/ 上, 所以这里匹配 /tracking/。
        """
        if self.mode == 'm':
            return 'm.usps.com/m/TrackConfirmAction' in url and 'tLabels' in url
        return 'tools.usps.com/tracking/' in url and 'tLabels' in url

    @staticmethod
    def extract_usps_labels(url: str) -> List[str]:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        labels_raw = query.get('tLabels', [''])[0]
        return [s.strip() for s in labels_raw.split(',') if s.strip()]

    def first_label(self, url: str) -> str:
        labels = self.extract_usps_labels(url)
        return labels[0] if labels else ''

    def parse(self, html: str) -> List[List[str]]:
        if not html:
            return []
        return self.parse_danhao_mobile(html) if self.mode == 'm' else self.parse_danhao(html)

    # -------------------- HTML 解析 --------------------

    def parse_danhao(self, html: str) -> List[List[str]]:
        doc = pq(html)
        d = []
        for container in doc('.track-bar-container').items():
            for summary in container('.product_summary').items():
                latest_date = summary('.tb-date').eq(0).text().strip()
                d.append([
                    summary('.tracking-number').text().strip(),
                    summary('.banner-header').text().strip(),
                    summary('.banner-content').text().replace('\n', '').replace('\r', ''),
                    latest_date,
                ])
        seen = []
        for item in d:
            if item not in seen:
                seen.append(item)
        return seen

    def parse_danhao_mobile(self, html: str) -> List[List[str]]:
        doc = pq(html)
        d = []
        for item in doc('li.ui-border-dotted-bottom').items():
            tracking_number = item('.tracking-number.hidden').text().strip()
            if not tracking_number:
                continue
            status_header = item('.package-note h3').text().strip().rstrip(':')
            span_text = re.sub(r'\s+', ' ', item('.package-note > span').text().strip()).strip()
            date_match = re.search(r'on\s+(.+?\d{4})\s+at\s+(\d+:\d+\s*[ap]m)', span_text)
            latest_date = f"{date_match.group(1)} at {date_match.group(2)}" if date_match else ''
            d.append([
                tracking_number,
                status_header,
                span_text.replace('\n', '').replace('\r', ''),
                latest_date,
            ])
        seen = []
        for item in d:
            if item not in seen:
                seen.append(item)
        return seen
