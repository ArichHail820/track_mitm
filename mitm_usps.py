"""USPS 抓取 - 命令行真实 Chrome + mitmproxy 自驱动方案。

核心思路(关键: 用命令行启动【真实 Chrome】, 不带任何自动化特征, 让 Akamai 的 JS
挑战被 Chrome 自动跑通, 从而拿到真实数据; mitmproxy 在 HTTP 层处理数据 + 自驱动):

    1. 本进程内编程启动 mitmproxy(默认 127.0.0.1:8080)。
    2. 用 subprocess 命令行启动本机 Chrome:
         chrome.exe --proxy-server=127.0.0.1:8080 --ignore-certificate-errors ... http://usps.local/next
       不连 CDP、不加 --enable-automation, 所以没有 navigator.webdriver 等自动化痕迹。
    3. 自驱动闭环(全部在 mitmproxy 里完成, 浏览器只负责产生真实流量):
         a. 浏览器请求虚拟地址 http://usps.local/next
            -> addon 在 request 钩子里取一批单号, 拼 USPS URL, 302 跳转过去。
         b. 浏览器访问 USPS:
            - 首次会遇到 Akamai 挑战页(parse 为空)-> addon 不干预, 让 Chrome 自己跑通挑战;
              挑战通过后 Chrome 会自动重新加载, 拿到真实数据页。
            - 数据页(parse 出数据)-> addon 解析并提交到单号服务, 同时把响应体改写成
              "跳回 http://usps.local/next", 于是进入下一轮。
       如此循环, 无需 CDP 控制浏览器导航。

环境变量:
    MODE                 'd'(桌面) 或 'm'(手机), 默认 'd'
    NUM_TYPE             'big' 或 'mysql', 默认 'big'
    MITM_PORT            mitmproxy 监听端口, 默认 8080
    RUN_SECONDS          运行时长(秒), <=0 表示一直跑直到 Ctrl+C, 默认 0
    HEADLESS             命令行 Chrome 是否无头(--headless=new), 默认 false(有头更易过验证)
    CHROME_PATH          chrome.exe 路径, 默认标准安装路径
    CHROME_USER_DATA_DIR Chrome 用户资料目录; 留空则用一次性临时目录
    BATCH                每批单号数量(见 usps_core), 默认 34

依赖:
    pip install mitmproxy pyquery aiohttp loguru
"""

import asyncio
import os
import platform
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time
from typing import Optional

from loguru import logger
from mitmproxy import http
from mitmproxy.options import Options
from mitmproxy.tools.dump import DumpMaster

from usps_core import UspsScraper, close_global_session

ssl._create_default_https_context = ssl._create_unverified_context

logger.remove()
logger.add(sys.stderr, format="{time:HH:mm:ss} | <level>{message}</level>")


# ============================== 配置 ==============================

def _env(name: str, default: str) -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    return _env(name, 'true' if default else 'false').lower() in ('1', 'true', 'yes', 'on')


MODE = _env('MODE', 'd').lower()
NUM_TYPE = _env('NUM_TYPE', 'big').lower()
MITM_PORT = _env_int('MITM_PORT', 8080)
RUN_SECONDS = _env_int('RUN_SECONDS', 0)
TABS = max(1, _env_int('TABS', 5))            # 并行标签页数量
HEADLESS = _env_bool('HEADLESS', False)
def _default_chrome_path() -> str:
    """按平台返回 Chrome 默认路径(可被 CHROME_PATH 环境变量覆盖)。"""
    if platform.system() == 'Windows':
        return r'C:\Program Files\Google\Chrome\Application\chrome.exe'
    for p in ('/usr/bin/google-chrome', '/usr/bin/google-chrome-stable',
              '/usr/bin/chromium-browser', '/usr/bin/chromium'):
        if os.path.exists(p):
            return p
    return '/usr/bin/google-chrome'


CHROME_PATH = _env('CHROME_PATH', _default_chrome_path())
CHROME_USER_DATA_DIR = _env('CHROME_USER_DATA_DIR', '')
DUMP_TARGET = _env_bool('DUMP_TARGET', False)  # 把目标响应 body 存盘, 调试用
SUCCESS_THRESHOLD = float(_env('SUCCESS_THRESHOLD', '0.6'))   # 成功率低于此值则优雅结束
SUCCESS_MIN_SAMPLES = _env_int('SUCCESS_MIN_SAMPLES', 20)     # 至少累计这么多有效样本才判定(避免冷启动误判)
STEALTH = _env_bool('STEALTH', True)         # 通过 mitmproxy 注入指纹伪装脚本 + 放宽 CSP
MAX_REFRESH = _env_int('MAX_REFRESH', 5)     # 解析为空时, 刷新重试当前批的最大次数

# 自驱动用的虚拟调度地址(不真实存在, 由 mitmproxy 拦截并 302 到下一批 USPS URL)
DISPATCH_HOST = 'usps.local'
DISPATCH_URL = f'http://{DISPATCH_HOST}/next'

# 数据页处理完后, 把响应体替换成这个, 让浏览器立刻跳回调度地址取下一批
_REDIRECT_TO_NEXT = (
    f'<!doctype html><html><head>'
    f'<meta http-equiv="refresh" content="0;url={DISPATCH_URL}">'
    f'</head><body>next</body></html>'
)
# 暂时取不到单号时, 让浏览器 3 秒后重试调度地址
_RETRY_HTML = (
    f'<!doctype html><html><head>'
    f'<meta http-equiv="refresh" content="3;url={DISPATCH_URL}">'
    f'</head><body>retry</body></html>'
)


# 指纹伪装脚本: 在每个页面最前面执行, 覆盖 CI/headless 最暴露的特征,
# 让 Akamai 的检测脚本读到"普通真人 Windows Chrome"的值。
STEALTH_JS = r"""
(function(){
  try{
    var def=function(o,k,v){try{Object.defineProperty(o,k,{get:function(){return v;},configurable:true});}catch(e){}};
    def(navigator,'webdriver',undefined);
    def(navigator,'hardwareConcurrency',8);
    def(navigator,'deviceMemory',8);
    def(navigator,'languages',['en-US','en']);
    // WebGL 厂商/型号伪装: 把 CI 的 SwiftShader/Mesa 伪装成常见独显
    var patch=function(proto){
      if(!proto||!proto.getParameter)return;
      var gp=proto.getParameter;
      proto.getParameter=function(p){
        if(p===37445)return 'Intel Inc.';
        if(p===37446)return 'Intel(R) Iris(TM) Graphics 6100';
        return gp.apply(this,arguments);
      };
    };
    try{if(window.WebGLRenderingContext)patch(WebGLRenderingContext.prototype);}catch(e){}
    try{if(window.WebGL2RenderingContext)patch(WebGL2RenderingContext.prototype);}catch(e){}
    try{if(!window.chrome)window.chrome={runtime:{}};}catch(e){}
    try{
      var q=navigator.permissions&&navigator.permissions.query;
      if(q){navigator.permissions.query=function(p){
        return (p&&p.name==='notifications')?Promise.resolve({state:Notification.permission}):q.apply(this,arguments);
      };}
    }catch(e){}
  }catch(e){}
})();
"""


def _inject_stealth(html: str) -> str:
    """把指纹伪装脚本插到 <head>(或 <html>)开头, 确保在站点脚本之前执行。"""
    tag = '<script>' + STEALTH_JS + '</script>'
    low = html.lower()
    for marker in ('<head', '<html'):
        i = low.find(marker)
        if i != -1:
            j = html.find('>', i)
            if j != -1:
                return html[:j + 1] + tag + html[j + 1:]
    return tag + html


def _safe_text(resp) -> Optional[str]:
    """安全读取响应文本; 二进制(如 CRX/图片)解码失败时返回 None, 不报错。"""
    if resp is None:
        return None
    try:
        return resp.text
    except Exception:
        return None


# ============================== mitmproxy addon ==============================

class USPSAddon:
    """中间人插件: 调度(取单号->跳转) + 解析提交 + 自驱动跳转。"""

    def __init__(self, scraper: UspsScraper):
        self.scraper = scraper
        self.total_success = 0
        self.total_challenge = 0
        self.total_empty = 0
        self.dispatched = 0
        self.total_requests = 0      # mitmproxy 收到的所有请求数(判断 Chrome 是否走了代理)
        self._dump_n = 0
        self.retry_counts = {}       # 首单号 -> 当前批已刷新重试次数

    async def request(self, flow: http.HTTPFlow):
        self.total_requests += 1
        # 拦截调度地址: 取一批单号, 302 跳到 USPS
        if flow.request.pretty_host == DISPATCH_HOST:
            danhao_ls = await self.scraper.获取单号()
            if not danhao_ls:
                flow.response = http.Response.make(
                    200, _RETRY_HTML.encode(), {"Content-Type": "text/html; charset=utf-8"}
                )
                logger.warning("暂无单号, 3s 后重试")
                return
            url = self.scraper.build_url(danhao_ls)
            self.dispatched += 1
            flow.response = http.Response.make(302, b'', {"Location": url})

    async def response(self, flow: http.HTTPFlow):
        try:
            if flow.response is None:
                return
            url = flow.request.pretty_url
            ct = (flow.response.headers.get('content-type', '') or '').lower()
            is_html = 'text/html' in ct

            # 放宽 CSP / XFO, 让注入的指纹伪装脚本能执行
            if STEALTH:
                for h in ('content-security-policy', 'content-security-policy-report-only',
                          'x-frame-options'):
                    if h in flow.response.headers:
                        del flow.response.headers[h]

            if self.scraper.is_target_response_url(url):
                body = _safe_text(flow.response)
                if body is None:
                    return
                parsed = self.scraper.parse(body)

                if DUMP_TARGET and not parsed:
                    self._dump_n += 1
                    fn = f'_dump_{self.scraper.mode}_{self._dump_n}.html'
                    try:
                        with open(fn, 'w', encoding='utf-8') as f:
                            f.write(body)
                        logger.info(f"📄 dump 空响应 -> {fn} (len={len(body)})")
                    except Exception:
                        pass

                if parsed:
                    self.total_success += 1
                    self.retry_counts.pop(self.scraper.first_label(url), None)
                    logger.info(
                        f"✅ 解析成功 {len(parsed)} 条 | 首条 {parsed[0]} | "
                        f"累计 ✅{self.total_success}/🛡️{self.total_challenge}/⚠️{self.total_empty}"
                    )
                    asyncio.create_task(self.scraper.原始请求提交到缓存(parsed))
                    flow.response.text = _REDIRECT_TO_NEXT
                    flow.response.headers["content-type"] = "text/html; charset=utf-8"
                    return

                # 挑战页/中间页: 让 Chrome 自己跑通挑战, 同时注入指纹伪装
                if any(m in body for m in
                       ('/_sec/verify', 'bm-verify', 'Object.defineProperty(document',
                        '_sec/cp_challenge', 'ISTL-REDIRECT-TO', "addEventListener('afterReady'")):
                    self.total_challenge += 1
                    logger.info(f"🛡️ 挑战页, 交给 Chrome 自动通过 (len={len(body)})")
                    if STEALTH and is_html and body:
                        flow.response.text = _inject_stealth(body)
                    return

                # 非挑战的空页: USPS 偶发没出数据, 刷新重试当前这批(不取新单号)
                key = self.scraper.first_label(url)
                cnt = self.retry_counts.get(key, 0) + 1
                if cnt <= MAX_REFRESH:
                    self.retry_counts[key] = cnt
                    logger.info(f"🔄 解析为空(len={len(body)}), 第 {cnt}/{MAX_REFRESH} 次刷新重试当前批")
                    refresh_html = (
                        f'<!doctype html><html><head>'
                        f'<meta http-equiv="refresh" content="1;url={url}">'
                        f'</head><body>retry</body></html>'
                    )
                    # 用 make(200) 整体替换: 原响应可能非 200(空/中断), 否则浏览器不会执行刷新
                    flow.response = http.Response.make(
                        200, refresh_html.encode(), {"Content-Type": "text/html; charset=utf-8"})
                else:
                    self.retry_counts.pop(key, None)
                    self.total_empty += 1   # 刷满上限仍没数据, 才算一次真失败
                    logger.info(f"⚠️ 刷新 {MAX_REFRESH} 次仍为空, 放弃该批, 取下一批")
                    flow.response = http.Response.make(
                        200, _REDIRECT_TO_NEXT.encode(), {"Content-Type": "text/html; charset=utf-8"})
                return

            # 其它 HTML(挑战相关页 / iframe)也注入指纹伪装
            if STEALTH and is_html:
                body = _safe_text(flow.response)
                if body and '<' in body:
                    flow.response.text = _inject_stealth(body)
        except Exception as e:
            logger.debug(f"addon.response 异常: {e}")


# ============================== 命令行 Chrome ==============================

def start_chrome(user_data_dir: str) -> subprocess.Popen:
    args = [
        CHROME_PATH,
        f'--proxy-server=http://127.0.0.1:{MITM_PORT}',
        '--ignore-certificate-errors',          # 信任 mitmproxy 的伪证书
        f'--user-data-dir={user_data_dir}',
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-popup-blocking',
        '--disable-blink-features=AutomationControlled',
        '--window-size=1920,1080',
        '--lang=en-US',
        '--accept-lang=en-US,en',
    ]
    if platform.system() == 'Windows':
        args.append('--start-minimized')
    else:
        # Linux / CI(root)必备: 关闭沙箱 + 避免 /dev/shm 过小导致崩溃; --log-level=3 压掉 dbus 等噪音
        args += ['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--log-level=3']
    if HEADLESS:
        args.append('--headless=new')
    if MODE == 'm':
        args.append('--user-agent=Emb/And/1.0')
    # 传 N 个调度地址 -> Chrome 开 N 个顶层标签页, 每个独立自驱动循环(共享同一 profile 的 cookie)
    args.extend([DISPATCH_URL] * TABS)
    logger.info(f"启动命令行 Chrome ... ({TABS} 个标签页)")
    logger.info(f"   CHROME_PATH={CHROME_PATH} (存在={os.path.exists(CHROME_PATH)}) | user_data_dir={user_data_dir}")
    return subprocess.Popen(args)


def stop_chrome(proc: Optional[subprocess.Popen], user_data_dir: str):
    if proc is not None:
        try:
            proc.terminate()
        except Exception:
            pass
    if platform.system() == 'Windows':
        # 先按进程树杀 launcher
        if proc is not None:
            try:
                subprocess.run(['taskkill', '/F', '/T', '/PID', str(proc.pid)],
                               capture_output=True)
            except Exception:
                pass
        # 再按命令行里的 user-data-dir 精确清理 relaunch 出来的浏览器进程(不误杀用户自己的 Chrome)
        if user_data_dir:
            try:
                ps = (
                    "Get-CimInstance Win32_Process -Filter \"Name='chrome.exe'\" | "
                    f"Where-Object {{ $_.CommandLine -like '*{user_data_dir}*' }} | "
                    "ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"
                )
                subprocess.run(['powershell', '-NoProfile', '-Command', ps], capture_output=True)
            except Exception:
                pass
    else:
        # Linux: kill launcher + 按 user-data-dir 匹配清理
        if proc is not None:
            try:
                proc.kill()
            except Exception:
                pass
        if user_data_dir:
            try:
                subprocess.run(['pkill', '-9', '-f', user_data_dir], capture_output=True)
            except Exception:
                pass


# ============================== 入口 ==============================

async def _main():
    if MODE not in ('m', 'd'):
        logger.error(f"非法 MODE={MODE}")
        return
    if NUM_TYPE not in ('mysql', 'big'):
        logger.error(f"非法 NUM_TYPE={NUM_TYPE}")
        return
    if not os.path.exists(CHROME_PATH):
        logger.error(f"找不到 Chrome: {CHROME_PATH} (可用 CHROME_PATH 指定)")
        return

    scraper = UspsScraper(mode=MODE, num_type=NUM_TYPE)
    addon = USPSAddon(scraper)

    # 代理环境变量诊断(PyCharm 等可能注入, 一般不影响 Chrome, 但打印出来便于排查)
    proxy_envs = {k: os.environ.get(k) for k in
                  ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy')
                  if os.environ.get(k)}
    if proxy_envs:
        logger.warning(f"⚠️ 检测到代理环境变量: {proxy_envs}")

    # 端口预检: 若 8080 已被占用(常见于 PyCharm 上次运行没杀干净), 直接报错退出
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(('127.0.0.1', MITM_PORT))
    except OSError:
        logger.error(
            f"❌ 端口 {MITM_PORT} 已被占用! 多半是上一次运行的 mitmproxy 没退干净"
            f"(PyCharm 点 Stop 常杀不干净)。\n"
            f"    解决: 结束残留的 python/mitmproxy 进程, 或改用 MITM_PORT 换一个端口。\n"
            f"    PowerShell: Get-Process python | Stop-Process -Force"
        )
        probe.close()
        return
    finally:
        try:
            probe.close()
        except Exception:
            pass

    opts = Options(listen_host='127.0.0.1', listen_port=MITM_PORT, ssl_insecure=True)
    master = DumpMaster(opts, with_termlog=False, with_dumper=False)
    master.addons.add(addon)
    mitm_task = asyncio.create_task(master.run())
    logger.info(f"🚀 mitmproxy 已启动 127.0.0.1:{MITM_PORT} | mode={MODE} num_type={NUM_TYPE} "
                f"tabs={TABS} headless={HEADLESS} run_seconds={RUN_SECONDS}")
    await asyncio.sleep(1.5)

    # 确认 mitmproxy 真的起来了(bind 失败等会让 task 提前结束)
    if mitm_task.done():
        logger.error(f"❌ mitmproxy 启动失败: {mitm_task.exception()}")
        await close_global_session()
        return

    use_temp_dir = not CHROME_USER_DATA_DIR
    user_data_dir = CHROME_USER_DATA_DIR or tempfile.mkdtemp(prefix='usps_chrome_')
    proc = start_chrome(user_data_dir)

    deadline = (time.monotonic() + RUN_SECONDS) if RUN_SECONDS > 0 else None
    # 用"实际流量活性"判断 Chrome 是否还在干活(launcher 进程退出≠浏览器关闭, 不能用 proc.poll)
    STALL_RESTART = 60          # 连续多少秒没有任何流量就重启 Chrome
    last_total = -1
    last_active = time.monotonic()
    start_ts = time.monotonic()
    warned_noproxy = False
    try:
        while True:
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                logger.info("⏰ 到达运行时长, 准备退出")
                break

            await asyncio.sleep(5)

            # 启动诊断: 若 15s 内 mitmproxy 完全没收到流量, 说明 Chrome 没走代理
            if not warned_noproxy and time.monotonic() - start_ts > 15:
                if addon.total_requests == 0:
                    logger.error(
                        "🚫 15s 内 mitmproxy 没收到任何 Chrome 流量! Chrome 没走代理。\n"
                        "    排查: 1) 是否已有 Chrome 在运行(先全部关掉再跑); "
                        "2) 系统/企业策略代理是否覆盖了 --proxy-server; "
                        "3) CHROME_PATH 是否正确。"
                    )
                else:
                    logger.info(f"🔎 mitmproxy 已收到 {addon.total_requests} 个请求, 代理正常")
                warned_noproxy = True

            # 成功率熔断: 累计足够"有效样本"(✅+⚠️)后, 成功率低于阈值则优雅结束
            #   成功率 = ✅ /(✅ + ⚠️); 🛡️挑战页是 Chrome 过验证的中间态, 不计入
            ok = addon.total_success
            bad = addon.total_empty
            samples = ok + bad
            if samples >= SUCCESS_MIN_SAMPLES:
                rate = ok / samples
                if rate < SUCCESS_THRESHOLD:
                    logger.warning(
                        f"📉 成功率 {rate:.1%} 低于阈值 {SUCCESS_THRESHOLD:.0%} "
                        f"(有效样本 ✅{ok}/⚠️{bad}), 优雅结束"
                    )
                    break

            total = (addon.dispatched + addon.total_success
                     + addon.total_challenge + addon.total_empty)
            if total != last_total:
                last_total = total
                last_active = time.monotonic()
                continue
            # 没有任何新活动
            if time.monotonic() - last_active > STALL_RESTART:
                logger.warning(f"⚠️ 超过 {STALL_RESTART}s 无任何流量, 重启 Chrome")
                stop_chrome(proc, user_data_dir)
                await asyncio.sleep(2)
                proc = start_chrome(user_data_dir)
                last_active = time.monotonic()
    except asyncio.CancelledError:
        pass
    finally:
        stop_chrome(proc, user_data_dir)
        master.shutdown()
        await asyncio.gather(mitm_task, return_exceptions=True)
        await close_global_session()
        if use_temp_dir:
            shutil.rmtree(user_data_dir, ignore_errors=True)
    logger.info(f"🏁 程序退出 (调度 {addon.dispatched} 批 | "
                f"✅{addon.total_success}/🛡️{addon.total_challenge}/⚠️{addon.total_empty})")


if __name__ == '__main__':
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        logger.info('🛑 收到 Ctrl+C, 退出')
