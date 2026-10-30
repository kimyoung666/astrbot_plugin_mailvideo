"""视频搜索与下载：基于 yt-dlp 做通用解析，兼容 mp4 直链 / m3u8 / 播放页。

设计原则：不针对具体站点写死规则。
- 先按 ``search_url_template`` 拼接搜索页地址，交给 yt-dlp 枚举视频条目；
- yt-dlp 枚举不到时，回退到 httpx 抓取搜索页 HTML，用启发式规则收集候选视频页；
- 下载时优先交给 yt-dlp（自动处理 m3u8 合并、播放页解析），失败再尝试直链下载。

本模块所有方法均为**同步阻塞**实现，调用方应放到线程中执行（如 ``asyncio.to_thread``）。
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urljoin, urlparse

from astrbot.api import logger

__all__ = ["VideoFetcher", "VideoFetchError"]


# ====================================================================== #
# 站点适配器（SPA / JSON API 站）
# ====================================================================== #
def _load_site_adapters_module():
    """按文件路径加载同目录的 ``site_adapters``。

    与 ``main.py`` 的动态加载约定保持一致：用唯一模块名 + 源文件 mtime
    版本戳，改完文件能自动重载，也不会和别的插件同名模块撞车。
    """
    import importlib.util
    import sys as _sys

    name = "astrbot_plugin_mailvideo_core.site_adapters"
    path = Path(__file__).resolve().parent / "site_adapters.py"
    try:
        stamp = path.stat().st_mtime_ns
    except OSError:
        stamp = None

    cached = _sys.modules.get(name)
    if cached is not None and getattr(cached, "__mf_source_stamp__", None) == stamp:
        return cached

    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:  # pragma: no cover
        raise ImportError(f"无法加载适配器模块: {path}")
    module = importlib.util.module_from_spec(spec)
    module.__mf_source_stamp__ = stamp
    _sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _build_site_adapters(fetcher, enabled: bool = True, melon_pages: int = 3):
    """构建适配器集合；加载失败时返回 None，插件照常走通用流程。"""
    if not enabled:
        return None
    try:
        module = _load_site_adapters_module()
        return module.SiteAdapters(fetcher, enabled=True, melon_pages=melon_pages)
    except Exception as e:  # pragma: no cover
        logger.warning(f"[mailvideo] 站点适配器加载失败，按通用流程运行: {e}")
        return None

_MEDIA_EXT = (".mp4", ".m3u8", ".flv", ".mkv", ".ts", ".avi", ".mov", ".webm", ".f4v")
_VIDEO_PATH_HINTS = (
    "video",
    "play",
    "vod",
    "detail",
    "watch",
    "movie",
    "film",
    "episode",
    "view",
    "show",
    "post",
    "archives",
    "mv",
    "v/",
    "/v",
    "index.php",
)
_SKIP_SCHEMES = ("javascript:", "mailto:", "tel:", "data:")

# HTTP 请求头分隔符（ffmpeg -headers 需要 CRLF 分隔）
CRLF = "\r\n"

# 播放页内联 JSON 里的媒体直链（m3u8/mp4 等）；站点常用 "\/" 转义斜杠
_PAGE_MEDIA_RE = re.compile(
    r"https?://[^\s\"'<>\\]+?\.(?:m3u8|mp4|flv|mkv|webm|ts)(?:\?[^\s\"'<>]*)?",
    re.IGNORECASE,
)
# 择优顺序：HLS 自适应流 > 直接 mp4
_PAGE_MEDIA_PRIORITY = (".m3u8", ".mp4", ".flv", ".webm", ".mkv", ".ts")


class VideoFetchError(Exception):
    """视频搜索/下载相关错误。"""


class VideoFetcher:
    #: 共享连接池：连接上限 = 并发线程数 × 该系数
    _POOL_OVERCOMMIT = 2
    #: keep-alive 空闲连接保活时长（秒）
    _KEEPALIVE_EXPIRY = 30.0

    def __init__(
        self,
        search_url_template: str = "{base}/search?wd={keyword}",
        cookie: str = "",
        proxy: str = "",
        ffmpeg_path: str = "",
        timeout: int = 30,
        download_timeout: int = 1800,
        hls_parallel_workers: int = 24,
        proxy_bypass_hosts: str = "",
        proxy_force_hosts: str = "",
        auto_probe_direct: bool = True,
        probe_timeout: int = 5,
        http2: bool = True,
        enable_site_adapters: bool = True,
        melon_search_pages: int = 3,
        segment_host_fallbacks: str = "",
        prefer_max_bitrate_mbps: float = 0.0,
    ) -> None:
        raw = (search_url_template or "{base}/search?wd={keyword}").strip()
        # 支持用 "||" 或换行分隔多个候选模板：按顺序尝试，第一个返回真实结果的模板生效。
        # 这样可以同时兼容 ``{base}/search?query={keyword}`` 与 ``{base}/search/{keyword}/`` 这类不同站点写法。
        self.search_templates: list[str] = [
            t.strip() for t in re.split(r"\s*(?:\|\||\n)\s*", raw) if t.strip()
        ] or ["{base}/search?wd={keyword}"]
        self.cookie = cookie or ""
        self.proxy = proxy or ""
        self.ffmpeg_path = ffmpeg_path or ""
        self.timeout = timeout
        # 单次下载的硬超时（秒）。长片 + 慢源需要留足余量。
        self.download_timeout = max(60, int(download_timeout or 1800))
        # 预检阶段解析出的时长（media_url -> 秒），供下载进度显示
        self._duration_cache: dict[str, float] = {}
        # 日志里下载进度的刷新间隔（秒）
        self.progress_interval = 3.0
        # ── 搜索阶段调优 ──────────────────────────────────────────── #
        # yt-dlp 枚举搜索页时单独用的 socket 超时：它常在「站点不支持通用
        # 解析」的搜索页上死等到默认 30s，把整个流程卡在发候选之前。这里
        # 用独立的短超时，配合并发竞速，废模板几乎不再占时间。
        self.search_socket_timeout = max(2, min(10, int(timeout)))
        # 竞速宽限期：HTML 先出结果时，再给 yt-dlp 这么长时间（秒）等它
        # 一起返回；yt-dlp 的标题质量更好，两者都成功时优先用它。
        self.search_race_grace = 0.8
        # 最近一次搜索的命中信息 {"template": ..., "method": ...}，供上层
        # 记住「哪个模板能用」，下次优先命中。
        self.last_search: dict = {}
        # HLS 并行下载线程数（0 = 禁用并行，走 ffmpeg 串行）
        self.hls_parallel_workers = max(0, int(hls_parallel_workers or 0))
        # 码率上限（Mbps）：master playlist 有多档清晰度时，选「不超过上限的
        # 最高一档」；0 = 老行为（一律最高画质）。QQ 端上传是整包上行，上行
        # 带宽才是瓶颈，下 2.5Mbps 的档比下 8Mbps 再压缩快得多。
        try:
            self.prefer_max_bitrate_mbps = max(0.0, float(prefer_max_bitrate_mbps or 0))
        except (TypeError, ValueError):
            self.prefer_max_bitrate_mbps = 0.0
        # 已解析过的播放列表（media_url -> (final_url, m3u8 文本)）。预检阶段
        # 拉过一次，下载阶段直接复用，省掉一次往返 + 一次 TLS 等待。
        self._playlist_cache: dict[str, tuple[str, str]] = {}
        # ── 媒体域名分流 ──────────────────────────────────────────── #
        # 站点页面请求恒走代理（多数站点直连被墙）；媒体下载（m3u8/密钥/
        # 分片/直链）按 host 逐个判定：能直连就直连，直连不通用代理。
        self.auto_probe_direct = bool(auto_probe_direct)
        self.probe_timeout = max(1, int(probe_timeout or 5))
        # 分片/直链下载走 HTTP/2 多路复用：一个连接并发多请求，减少握手与
        # 队头阻塞开销，对「多小分片」的 HLS 尤其明显。个别老 CDN 或代理对
        # h2 支持不佳时，创建 client 会自动回退 HTTP/1.1。
        self.http2 = bool(http2)
        self._bypass_hosts = self._parse_hosts(proxy_bypass_hosts)
        self._force_hosts = self._parse_hosts(proxy_force_hosts)
        # host -> True(直连) / False(走代理)，探测一次后长期复用
        self._host_route: dict[str, bool] = {}
        # ── 分片域名改写 ──────────────────────────────────────────── #
        # 站点换 CDN 时，m3u8 里常残留已失效的分片域名（实测同一个文件在
        # ts2.ywqishu.com 全超时、在 ts2.zblsjy.com 正常）。适配器探测到可用
        # 镜像后注册改写，分片/密钥请求统一换域名。
        self._host_rewrites: dict[str, str] = {}
        self.segment_host_fallbacks: list[str] = self._parse_hosts(segment_host_fallbacks)
        self._route_lock = threading.Lock()
        # host -> 探测单飞锁（同一域名并发首访只探一次）
        self._host_probe_locks: dict[str, threading.Lock] = {}
        # 共享连接池：按用途分桶，替代「每个分片新建一个 Client」
        self._shared_clients: dict[str, object] = {}
        self._client_lock = threading.Lock()
        # ── 站点适配器 ────────────────────────────────────────────── #
        # SPA + JSON API 站（页面是壳、数据走 JSON、播放地址前端拼接）用
        # 通用 HTML 规则永远抓不到，交给适配器短路处理；不适配则返回 None。
        self._site_adapters = _build_site_adapters(
            self, enable_site_adapters, melon_pages=melon_search_pages
        )

    # ============================================================== #
    # 工具
    # ============================================================== #
    def ffmpeg_available(self) -> bool:
        if self.ffmpeg_path and Path(self.ffmpeg_path).exists():
            return True
        return shutil.which("ffmpeg") is not None

    def _ffmpeg_location(self) -> Optional[str]:
        if self.ffmpeg_path:
            p = Path(self.ffmpeg_path)
            return str(p.parent) if p.is_file() else str(p)
        return None

    def _http_headers(self) -> dict:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
            )
        }
        if self.cookie:
            headers["Cookie"] = self.cookie
        return headers

    def _fill_template(self, template: str, base: str, keyword: str) -> str:
        base = (base or "").rstrip("/")
        return (
            template.replace("{base}", base)
            .replace("{keyword}", quote(keyword))
            .replace("{raw_keyword}", keyword)
        )

    def build_search_urls(self, base: str, keyword: str) -> list[str]:
        """按配置顺序拼出全部候选搜索 URL。"""
        return [self._fill_template(t, base, keyword) for t in self.search_templates]

    # ============================================================== #
    # 搜索
    # ============================================================== #
    def search(self, base: str, keyword: str, prefer_template: str = "") -> list[dict]:
        """返回候选视频条目列表：``[{"url": ..., "title": ...}, ...]``。

        每个候选模板内部由「yt-dlp 枚举」和「HTML 解析」**并发竞速**，谁先
        给出真实候选就用谁，废模板不再串行拖满超时。

        ``prefer_template`` 传入上次成功的模板时，会把它提到最前面先试，
        避免每次都在已知不可用的模板上白等一轮。

        命中信息记在 ``self.last_search``，调用方据此持久化模板偏好。
        """
        self.last_search = {}
        # ── 站点适配器优先：命中则短路，不适配（返回 None）才走通用流程 ──
        if self._site_adapters is not None:
            adapted = self._site_adapters.search(base, keyword)
            if adapted is not None:
                if adapted:
                    self.last_search = {
                        "template": "adapter",
                        "url": base,
                        "method": "adapter",
                    }
                logger.info(f"[mailvideo] 适配器接管搜索，候选 {len(adapted)} 个")
                return adapted
        # 原始模板与渲染后 URL 成对保留：记住的是「哪个模板」（不含关键词），
        # 这样换关键词也能直接命中，不用每次重走废模板。
        pairs = list(zip(self.search_templates, self.build_search_urls(base, keyword)))
        if prefer_template:
            hit = [p for p in pairs if p[0] == prefer_template]
            if hit:
                pairs = hit + [p for p in pairs if p[0] != prefer_template]
                logger.info(f"[mailvideo] 优先使用上次命中的搜索模板: {prefer_template}")

        for i, (raw_tpl, search_url) in enumerate(pairs, 1):
            if len(pairs) > 1:
                logger.info(f"[mailvideo] 搜索地址({i}/{len(pairs)}): {search_url}")
            else:
                logger.info(f"[mailvideo] 搜索地址: {search_url}")

            candidates, method = self._search_one_template(search_url, base, keyword)
            if candidates:
                self.last_search = {
                    "template": raw_tpl,
                    "url": search_url,
                    "method": method,
                }
                return candidates

        # 所有模板都没拿到候选：兜底，把最后一个搜索页当作可解析页面
        last_url = pairs[-1][1] if pairs else self.build_search_urls(base, keyword)[-1]
        logger.warning(
            f"[mailvideo] 全部 {len(pairs)} 个搜索模板均未命中候选，兜底尝试: {last_url}"
        )
        return [{"url": last_url, "title": keyword}]

    def search_with_meta(
        self, base: str, keyword: str, prefer_template: str = ""
    ) -> tuple[list[dict], dict]:
        """同 :meth:`search`，额外返回命中信息 ``{"template", "method"}``。"""
        candidates = self.search(base, keyword, prefer_template)
        return candidates, dict(self.last_search)

    def _search_one_template(
        self, search_url: str, base: str, keyword: str
    ) -> tuple[list[dict], str]:
        """单个模板内并发竞速两种解析方式，返回 (候选列表, 命中的方法)。"""
        import concurrent.futures as _cf

        started = time.time()
        pool = _cf.ThreadPoolExecutor(max_workers=2, thread_name_prefix="mv-search")
        try:
            jobs = {
                pool.submit(self._search_via_ytdlp, search_url, keyword): "yt-dlp",
                pool.submit(self._search_via_html, search_url, base, keyword): "html",
            }
            results: dict[str, list[dict]] = {}
            pending = set(jobs)

            def _take(fut) -> list[dict]:
                method = jobs[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    logger.info(f"[mailvideo] {method} 解析搜索页未命中: {type(e).__name__}: {e}")
                    res = []
                results[method] = res
                return res

            while pending:
                done, pending = _cf.wait(
                    pending, return_when=_cf.FIRST_COMPLETED
                )
                for fut in done:
                    if _take(fut):
                        # 先出结果的先记下；给另一种方式一点宽限期，
                        # 两者都成功时优先用标题质量更好的 yt-dlp。
                        if jobs[fut] == "html" and pending:
                            done2, pending = _cf.wait(
                                pending, timeout=self.search_race_grace,
                                return_when=_cf.FIRST_COMPLETED,
                            )
                            for f2 in done2:
                                _take(f2)
                            pending = set()
                        logger.info(
                            f"[mailvideo] 模板命中：{jobs[fut]} | 候选 "
                            f"{len(results[jobs[fut]])} 个 | {time.time() - started:.1f}s"
                        )
                        if results.get("yt-dlp"):
                            return results["yt-dlp"], "yt-dlp"
                        return results[jobs[fut]], jobs[fut]
            return [], ""
        finally:
            # 不等待：yt-dlp 那条线可能还要空等到自己的 socket 超时，
            # 这里直接放生，避免又把流程卡回原来的 30s。
            pool.shutdown(wait=False, cancel_futures=True)


    def _search_via_ytdlp(self, search_url: str, keyword: str) -> list[dict]:
        ydl_opts = self._base_ydl_opts()
        ydl_opts.update(
            {
                "extract_flat": "in_playlist",
                "skip_download": True,
                "playlistend": 30,
                # 搜索页枚举用短超时：通用解析器碰上「不认识」的搜索页会
                # 一直等到 socket 超时，这里宁可快点失败交给 HTML 解析。
                "socket_timeout": self.search_socket_timeout,
            }
        )
        import yt_dlp

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(search_url, download=False)

        entries = []
        if info:
            if info.get("entries"):
                entries = [e for e in info["entries"] if e]
            elif info.get("_type") == "url" or info.get("webpage_url"):
                entries = [info]

        results: list[dict] = []
        for e in entries:
            url = e.get("url") or e.get("webpage_url") or ""
            # extract_flat 下 url 可能是相对/id，需要拼成绝对地址
            if url and not url.startswith("http"):
                if e.get("webpage_url"):
                    url = e["webpage_url"]
                else:
                    url = urljoin(search_url, url)
            if not url:
                continue
            results.append(
                {
                    "url": url,
                    "title": e.get("title") or keyword,
                }
            )
        logger.info(f"[mailvideo] yt-dlp 枚举到 {len(results)} 个候选")
        return results

    def _search_via_html(self, search_url: str, base: str, keyword: str) -> list[dict]:
        import httpx
        from bs4 import BeautifulSoup

        with httpx.Client(
            headers=self._http_headers(),
            timeout=self.timeout,
            follow_redirects=True,
            proxy=self._proxy_arg(),
        ) as client:
            resp = client.get(search_url)
            resp.raise_for_status()
            html = resp.text

        soup = BeautifulSoup(html, "html.parser")
        scored: list[tuple[int, str, str]] = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = (a["href"] or "").strip()
            if not href or href.startswith("#") or href.lower().startswith(_SKIP_SCHEMES):
                continue
            abs_url = urljoin(base, href)
            if abs_url in seen:
                continue
            text = a.get_text(" ", strip=True)
            score = self._score_candidate(abs_url, text, keyword)
            if score <= 0:
                continue
            seen.add(abs_url)
            scored.append((score, abs_url, text or abs_url))

        scored.sort(reverse=True)
        results = [{"url": u, "title": t} for _, u, t in scored]
        logger.info(f"[mailvideo] HTML 解析到 {len(results)} 个候选")
        return results

    @staticmethod
    def _score_candidate(url: str, text: str, keyword: str) -> int:
        lowered = url.lower()
        score = 0
        if lowered.endswith(_MEDIA_EXT):
            score += 4
        if any(h in lowered for h in _VIDEO_PATH_HINTS):
            score += 2
        hay = (url + " " + (text or "")).lower()
        if keyword and keyword.lower() in hay:
            score += 3
        # 明显的静态资源/导航链接降权
        if lowered.endswith((".css", ".js", ".png", ".jpg", ".jpeg", ".gif", ".ico")):
            score -= 5
        return score

    # ============================================================== #
    # 下载
    # ============================================================== #
    def _media_headers(self, referer: str = "") -> dict:
        """构造媒体请求头；带 Referer 便于 CDN 校验。"""
        headers = self._http_headers()
        if referer:
            parsed = urlparse(referer)
            if parsed.scheme and parsed.netloc:
                headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"
            else:
                headers["Referer"] = referer
        return headers

    # ── 分片域名改写（站点换 CDN 后的自救） ────────────────────── #
    def register_host_rewrite(self, old_host: str, new_host: str) -> None:
        """登记「旧分片域名 → 可用镜像域名」，之后的分片/密钥请求自动改写。"""
        old_host = (old_host or "").strip().lower()
        new_host = (new_host or "").strip().lower()
        if not old_host or not new_host or old_host == new_host:
            return
        if self._host_rewrites.get(old_host) == new_host:
            return
        self._host_rewrites[old_host] = new_host
        logger.info(f"[mailvideo] 分片域名改写登记: {old_host} → {new_host}")

    def _rewrite_media_url(self, url: str) -> str:
        """按登记表替换分片 URL 的域名；没有登记则原样返回。"""
        if not self._host_rewrites or not url:
            return url
        try:
            parsed = urlparse(url)
        except Exception:
            return url
        new_host = self._host_rewrites.get((parsed.netloc or "").lower())
        if not new_host:
            return url
        return parsed._replace(netloc=new_host).geturl()

    def _extract_media_urls(self, page_url: str) -> list[str]:
        """从播放页 HTML 中提取全部媒体直链（m3u8/mp4），按 HLS 优先排序。

        很多站点不在页面里直接给 <video src>，而是把播放地址写进内联 JSON
        （常见 ``"\\/"`` 转义斜杠）。这里统一反转义后正则抓取。
        """
        # ── 站点适配器优先：SPA 播放页的地址是前端拼的，正则永远抓不到 ──
        if self._site_adapters is not None:
            adapted = self._site_adapters.media_urls(page_url)
            if adapted:
                logger.info(f"[mailvideo] 适配器解析出媒体直链: {adapted[0][:120]}")
                return adapted

        try:
            import httpx

            with httpx.Client(
                headers=self._http_headers(),
                timeout=self.timeout,
                follow_redirects=True,
                proxy=self._proxy_arg(),
            ) as client:
                resp = client.get(page_url)
                resp.raise_for_status()
                html = resp.text
        except Exception as e:
            logger.warning(f"[mailvideo] 抓取播放页失败，跳过媒体提取: {e}")
            return []

        unescaped = html.replace("\\/", "/")
        found = list(dict.fromkeys(_PAGE_MEDIA_RE.findall(unescaped)))
        if not found:
            return []

        def _rank(u: str) -> int:
            low = u.lower()
            for i, ext in enumerate(_PAGE_MEDIA_PRIORITY):
                if ext in low:
                    return i
            return len(_PAGE_MEDIA_PRIORITY)

        found.sort(key=_rank)
        logger.info(
            f"[mailvideo] 播放页提取到 {len(found)} 条媒体直链，首选: {found[0][:120]}"
        )
        return found

    def download(
        self,
        url: str,
        dest_dir: Path,
        max_size_mb: int = 0,
        title: str = "",
    ) -> dict:
        """下载单个视频，返回 ``{"path": str, "title": str, "webpage_url": str}``。

        ``max_size_mb`` > 0 时启用**预检**：先读取 m3u8 播放列表估算体积，
        超限则直接抛错，避免把几百 MB 甚至 1GB 的内容白下完再丢弃。
        """
        dest_dir = Path(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            f"[mailvideo] 开始下载任务 | 体积上限 {max_size_mb or '不限'}MB | "
            f"目标: {(url or '')[:110]}"
        )

        # 0) 决定候选直链：优先从播放页抠出真实媒体地址
        media_urls: list[str] = []
        if url.lower().endswith(_MEDIA_EXT):
            media_urls = [url]
        else:
            media_urls = self._extract_media_urls(url)
            if not media_urls:
                # 抠不到才退回原页面（交给 yt-dlp 的通用解析器碰碰运气）
                logger.warning("[mailvideo] 未从播放页抠到媒体直链，退回原始页面尝试")
                media_urls = [url]

        # 1) 预检：体积/时长超限的候选逐条剔除，全部超限才放弃
        if max_size_mb > 0:
            kept: list[str] = []
            for mu in media_urls:
                # 媒体直链一律预检（含无扩展名的 HLS 地址）
                if not mu.lower().endswith(_MEDIA_EXT) and mu == url and len(media_urls) == 1:
                    continue  # 只剩原页面时不做预检，交给 yt-dlp
                if not self._within_size_limit(mu, max_size_mb, referer=url):
                    logger.info(f"[mailvideo] 候选超限，跳过: {mu[:100]}")
                    continue
                kept.append(mu)
            if not kept:
                raise VideoFetchError(
                    f"所有候选体积均超过限制（{max_size_mb}MB），已跳过以避免浪费流量"
                )
            media_urls = kept

        last_err: Optional[Exception] = None
        total_cand = len(media_urls)
        for idx, media_url in enumerate(media_urls, 1):
            headers = self._media_headers(url if media_url != url else "")
            is_hls = ".m3u8" in media_url.lower().split("?")[0]
            page_title = title or ""
            logger.info(
                f"[mailvideo] 下载候选 {idx}/{total_cand}: "
                f"{'HLS' if is_hls else '直链'} {media_url[:110]}"
            )

            # 1) HLS 优先：并行下载（多连接榨满带宽，实测提速 8-10x）
            if is_hls and self.ffmpeg_available() and self.hls_parallel_workers > 0:
                logger.info(f"[mailvideo] 走并行下载（{self.hls_parallel_workers} 线程）")
                try:
                    return self._download_hls_parallel(
                        media_url, dest_dir, referer=url, title=page_title,
                    )
                except Exception as e:
                    last_err = e
                    logger.warning(f"[mailvideo] 并行下载失败: {e}")
                    # CDN 可能因并发连接过多暂时限制，冷却 2 秒再试
                    time.sleep(2)

            # 2) HLS 备选：ffmpeg 串行拉流（m3u8 动态变化时更宽容）
            #    分片域名改写只对并行路径生效：ffmpeg 自己解析 m3u8，会照着
            #    已失效的域名去拉，白等到超时，这种情况直接跳过。
            if is_hls and self.ffmpeg_available() and self._host_rewrites:
                logger.info("[mailvideo] 分片域名已改写，跳过 ffmpeg 远端拉流")
            elif is_hls and self.ffmpeg_available():
                logger.info("[mailvideo] 降级走 ffmpeg 串行拉流")
                try:
                    return self._download_via_ffmpeg(
                        media_url, dest_dir, referer=url, title=page_title
                    )
                except Exception as e:
                    last_err = e
                    logger.warning(f"[mailvideo] ffmpeg 下载失败: {e}")

            # 3) 交给 yt-dlp（含无扩展名的 HLS 与普通 mp4 直链）
            logger.info("[mailvideo] 降级走 yt-dlp")
            try:
                return self._download_via_ytdlp(
                    media_url,
                    dest_dir,
                    {"format": "bv*+ba/b", "merge_output_format": "mp4"},
                    "auto",
                    headers,
                )
            except Exception as e:
                last_err = e
                logger.warning(f"[mailvideo] yt-dlp 下载失败: {e}")
                self._clear_range_(dest_dir)

        raise VideoFetchError(f"下载失败: {last_err}")

    @staticmethod
    def _clear_range_(dest_dir: Path) -> None:
        """清掉 yt-dlp 遗留的 .part/.ytdl 分片，避免污染后续尝试。"""
        try:
            for pat in ("*.part", "*.part-Frag*", "*.ytdl", "*.temp"):
                for p in Path(dest_dir).glob(pat):
                    try:
                        p.unlink()
                    except Exception:
                        pass
        except Exception:
            pass

    # ------------------------------------------------------------------ 播放列表
    def _pick_variant(
        self,
        text: str,
        base_url: str,
        client=None,
        headers: Optional[dict] = None,
    ) -> tuple[str, str, int]:
        """在 master playlist 里选一路码流，返回 ``(final_url, m3u8 文本, 码率)``。

        - 未配 ``prefer_max_bitrate_mbps``：取最高 BANDWIDTH（原行为）
        - 配了上限：取不超过上限的最高一档；全都超上限时退回最低一档
        ``client`` 为空时只选地址不抓内容（返回的文本为空串）。
        """
        if "#EXT-X-STREAM-INF" not in text:
            return base_url, text, 0

        lines = [ln.strip() for ln in text.splitlines()]
        variants: list[tuple[int, str]] = []
        for i, line in enumerate(lines):
            if not line.startswith("#EXT-X-STREAM-INF"):
                continue
            m = re.search(r"BANDWIDTH=(\d+)", line)
            bw = int(m.group(1)) if m else 0
            for nxt in lines[i + 1:]:
                if nxt and not nxt.startswith("#"):
                    variants.append((bw, nxt))
                    break
        if not variants:
            return base_url, text, 0

        cap = self.prefer_max_bitrate_mbps * 1_000_000
        if cap > 0:
            under = [v for v in variants if v[0] <= cap]
            bw, uri = (
                max(under, key=lambda v: v[0]) if under
                else min(variants, key=lambda v: v[0])
            )
        else:
            bw, uri = max(variants, key=lambda v: v[0])

        sub_url = urljoin(base_url, uri)
        logger.info(
            f"[mailvideo] 选流 {bw / 1_000_000:.2f}Mbps（共 {len(variants)} 档"
            + (f"，上限 {self.prefer_max_bitrate_mbps:g}Mbps）" if cap > 0 else "，取最高）")
        )
        if client is None:
            return sub_url, "", bw
        resp = client.get(sub_url, headers=headers or self._http_headers(), timeout=self.timeout)
        resp.raise_for_status()
        return sub_url, resp.text, bw

    def _download_hls_parallel(
        self,
        media_url: str,
        dest_dir: Path,
        referer: str = "",
        title: str = "",
    ) -> dict:
        """并行下载 HLS 分片 + ffmpeg 合并。

        支持 AES-128 加密：下载密钥后生成本地 m3u8，
        ffmpeg 自动解密 + 合并。实测 16 线程对比单流提速 ~8-10x。
        """
        import concurrent.futures
        import urllib.parse as _urlparse

        if not self.ffmpeg_available():
            raise VideoFetchError("未检测到 ffmpeg，无法并行下载")
        if self.hls_parallel_workers <= 0:
            raise VideoFetchError("hls_parallel_workers=0，并行下载已禁用")

        headers = self._media_headers(referer)
        # 媒体域名路由：m3u8 所在 CDN 能直连就不挂代理
        logger.info(
            f"[mailvideo] m3u8 路由: {(urlparse(media_url).hostname or '?')} -> "
            f"{'走代理' if self._proxy_for(media_url) else '直连'}"
        )

        # ── 0) 整个下载任务只取一个共享 Client ──
        # m3u8、加密密钥、全部分片都走它：连接池全程只建一次，keep-alive
        # 连接从头用到尾，除首次 TCP+TLS 握手外不再重复握手。若某些分片
        # 落在别的 CDN（代理/直连路由不同），_fetch_seg 会另取同池的另一
        # 分桶——同样是连接复用，不是每片新建 Client。
        client = self._shared_client(media_url, timeout=None)
        task_bucket = self._client_bucket(media_url)

        # ── 1) 获取 m3u8（处理 master playlist）──
        # nullcontext 保留原有缩进结构，避免大幅重排代码
        with contextlib.nullcontext(client) as client:
            # 预检阶段已经拉过播放列表就直接复用：少一次 m3u8 往返，
            # 也让「预检看到的档位」和「实际下载的档位」保持一致。
            cached = self._playlist_cache.pop(media_url, None)
            if cached:
                final_url, text = cached
                logger.info("[mailvideo] 复用预检播放列表（省一次 m3u8 请求）")
            else:
                resp = client.get(media_url, headers=headers, timeout=self.timeout)
                resp.raise_for_status()
                final_url = media_url
                final_url, text, _bw = self._pick_variant(
                    resp.text, final_url, client, headers
                )
                if _bw:
                    logger.debug(f"[mailvideo] 自选档位 {_bw / 1_000_000:.2f}Mbps")

            # ── 2) 下载加密密钥（如果有）──
            key_file: Optional[Path] = None
            key_method = ""
            key_iv = ""
            m_key = re.search(r"#EXT-X-KEY:METHOD=([\w-]+)", text)
            if m_key:
                key_method = m_key.group(1)
                m_uri = re.search(r'URI="([^"]+)"', text)
                m_iv = re.search(r"IV=(0x[0-9a-fA-F]+)", text)
                if m_iv:
                    key_iv = m_iv.group(1)
                if key_method != "NONE" and m_uri:
                    key_url = self._rewrite_media_url(
                        _urlparse.urljoin(final_url, m_uri.group(1))
                    )
                    try:
                        kr = client.get(key_url, timeout=self.timeout)
                        kr.raise_for_status()
                        key_data = kr.content
                        key_file = Path(dest_dir) / f"_key_{int(time.time()*1000)}.bin"
                        key_file.write_bytes(key_data)
                        logger.info(
                            f"[mailvideo] 下载加密密钥: {len(key_data)}B "
                            f"({key_method}) IV={key_iv or 'auto'}"
                        )
                    except Exception as e:
                        logger.warning(f"[mailvideo] 密钥下载失败（可能无需加密）: {e}")
                        key_file = None

        # ── 3) 解析分片 URL + 时长 ──
        segments: list[dict] = []
        target_dur = 5
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("#EXT-X-TARGETDURATION:"):
                try:
                    target_dur = int(line.split(":")[1])
                except ValueError:
                    pass
            elif line.startswith("#EXTINF:"):
                try:
                    dur = float(line.split(":")[1].split(",")[0])
                except ValueError:
                    dur = float(target_dur)
                segments.append({"url": None, "duration": dur})
            elif line and not line.startswith("#"):
                full_url = self._rewrite_media_url(_urlparse.urljoin(final_url, line))
                if segments and segments[-1]["url"] is None:
                    segments[-1]["url"] = full_url
                else:
                    segments.append({"url": full_url, "duration": float(target_dur)})

        if not segments:
            raise VideoFetchError("m3u8 中未解析到分片")

        # ── 4) 准备输出目录 ──
        name = re.sub(r"[^\w.\-]+", "_", (title or "").strip()) or "video"
        if len(name) > 80:
            name = name[:80]
        out = Path(dest_dir) / f"{name}.mp4"
        seg_dir = Path(dest_dir) / f"_segs_{int(time.time() * 1000)}"
        seg_dir.mkdir(exist_ok=True)

        # ── 5) 并行下载分片 ──
        workers = self.hls_parallel_workers
        total = [0]
        done = [0]
        errors: list[str] = []

        def _fetch_seg(seg_url: str, idx: int) -> int:
            """流式下载单个分片并直接落盘，返回字节数。

            边收边写（1MB 大块）省掉「先 r.content 整片进内存再写」的一次
            拷贝；实测 16 连接下比 256KB 小块快约 15%。
            """
            dst = seg_dir / f"seg_{idx:05d}.ts"
            tmp = seg_dir / f"seg_{idx:05d}.ts.part"
            c = (
                client
                if self._client_bucket(seg_url) == task_bucket
                else self._shared_client(seg_url, timeout=None)
            )
            written = 0
            try:
                with c.stream("GET", seg_url, headers=headers) as r:
                    r.raise_for_status()
                    with open(tmp, "wb") as fh:
                        for chunk in r.iter_bytes(chunk_size=1024 * 1024):
                            fh.write(chunk)
                            written += len(chunk)
            except Exception:
                try:
                    tmp.unlink(missing_ok=True)
                except OSError:
                    pass
                raise
            os.replace(tmp, dst)  # 原子落位，避免半截分片被 ffmpeg 读到
            return written

        def _dl_seg(idx: int, seg_url: str) -> bool:
            for attempt in range(3):
                try:
                    size = _fetch_seg(seg_url, idx)
                    total[0] += size
                    done[0] += 1
                    return True
                except Exception as e:
                    if attempt < 2:
                        time.sleep(0.5 * (attempt + 1))
                        continue
                    # 直连路线连挂 3 次 → 该域名降级走代理，再补一次
                    if self.proxy and self._proxy_for(seg_url) is None:
                        self._demote_host(seg_url, f"(分片{idx}: {e})")
                        try:
                            size = _fetch_seg(seg_url, idx)
                            total[0] += size
                            done[0] += 1
                            return True
                        except Exception as e2:
                            errors.append(f"分片{idx}: {e2}")
                            return False
                    errors.append(f"分片{idx}: {e}")
                    return False

        logger.info(f"[mailvideo] 并行下载 {len(segments)} 片（{workers} 线程）…")
        start = time.time()
        last_log = start

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_dl_seg, i, seg["url"]): i
                for i, seg in enumerate(segments)
            }
            for future in concurrent.futures.as_completed(futures):
                now = time.time()
                if now - last_log >= self.progress_interval:
                    last_log = now
                    elapsed = now - start
                    speed = total[0] / elapsed / 1024 / 1024 if elapsed > 0 else 0
                    logger.info(
                        f"[mailvideo] ⏳ 并行 {done[0]}/{len(segments)} 片 | "
                        f"{total[0]/1024/1024:.1f}MB | "
                        f"{speed:.2f}MB/s | {elapsed:.1f}s"
                    )

        elapsed = time.time() - start
        seg_s = elapsed  # 纯拉流耗时（不含合并），用于阶段耗时明细
        missing = [
            i for i in range(len(segments))
            if not (seg_dir / f"seg_{i:05d}.ts").exists()
        ]
        if missing:
            raise VideoFetchError(
                f"{len(missing)}/{len(segments)} 个分片下载失败: {errors[:3]}"
            )

        logger.info(
            f"[mailvideo] 分片下载完成 {done[0]}/{len(segments)} | "
            f"{total[0]/1024/1024:.1f}MB | {elapsed:.1f}s"
        )

        # ── 6) 生成本地 m3u8（含密钥指令，ffmpeg 自动解密）──
        local_m3u8 = seg_dir / "local.m3u8"
        with open(local_m3u8, "w") as f:
            f.write("#EXTM3U\n")
            f.write("#EXT-X-VERSION:3\n")
            if key_method and key_method != "NONE" and key_file and key_file.exists():
                key_line = f"#EXT-X-KEY:METHOD={key_method},URI=\"{key_file.resolve()}\""
                if key_iv:
                    key_line += f",IV={key_iv}"
                f.write(key_line + "\n")
            f.write("#EXT-X-MEDIA-SEQUENCE:0\n")
            f.write(f"#EXT-X-TARGETDURATION:{target_dur}\n")
            for i, seg in enumerate(segments):
                f.write(f"#EXTINF:{seg['duration']:.3f},\n")
                f.write(f"seg_{i:05d}.ts\n")
            f.write("#EXT-X-ENDLIST\n")  # 必须！否则 ffmpeg 当直播流无限等下一片

        # ── 7) ffmpeg 合并（自动解密）──
        _t_mux = time.time()
        try:
            self._mux_local_playlist(local_m3u8, out)
        except Exception:
            shutil.rmtree(seg_dir, ignore_errors=True)
            if key_file:
                key_file.unlink(missing_ok=True)
            raise
        mux_s = time.time() - _t_mux

        # ── 8) 清理 ──
        shutil.rmtree(seg_dir, ignore_errors=True)
        if key_file:
            key_file.unlink(missing_ok=True)

        if not out.exists() or out.stat().st_size == 0:
            raise VideoFetchError("合并后文件为空")

        size_bytes = out.stat().st_size
        # 阶段耗时明细：下次喊「慢」的时候一眼看出是下载慢还是合并慢
        logger.info(
            f"[mailvideo] ✅ 并行下载完成 {out.name} | "
            f"{size_bytes/1024/1024:.2f}MB | "
            f"总耗时 {time.time()-start:.1f}s "
            f"（拉流 {seg_s:.1f}s / 合并 {mux_s:.1f}s / 均速 "
            f"{size_bytes/max(seg_s,0.001)/1024/1024:.2f}MB/s）"
        )
        return {
            "path": str(out),
            "title": out.stem,
            "webpage_url": final_url,
            "filesize": size_bytes,
            "downloaded_bytes": total[0],
        }

    def _mux_local_playlist(self, local_m3u8: Path, out: Path) -> None:
        """把本地 m3u8（含密钥指令）合并为 mp4，``-c copy`` 纯封装，最快。"""
        cmd = [
            self._ffmpeg_bin(), "-y", "-nostdin", "-v", "warning",
            "-allowed_extensions", "ALL",
            "-i", str(local_m3u8),
            "-c", "copy", "-bsf:a", "aac_adtstoasc",
            str(out),
        ]
        # 合并虽是纯封装（-c copy），但分片数一多，open/close 和落盘 IO
        # 也不便宜：原来固定 120s 会把长片直接判死，这里按分片数放宽。
        try:
            seg_count = len(list(local_m3u8.parent.glob("seg_*.ts")))
        except OSError:
            seg_count = 0
        timeout = max(120, min(1800, seg_count * 2 or 120))

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        err_lines: list[str] = []
        threading.Thread(
            target=self._drain_stderr, args=(proc, err_lines), daemon=True
        ).start()

        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise VideoFetchError(f"ffmpeg 合并超时（>{timeout}s）")
        finally:
            if proc.poll() is None:
                proc.kill()

        if proc.returncode != 0:
            tail = [l.strip() for l in err_lines if l.strip()][-3:]
            raise VideoFetchError("ffmpeg 合并失败: " + " | ".join(tail))


            raise VideoFetchError("ffmpeg 合并失败: " + " | ".join(tail))


    def _download_via_ffmpeg(
        self,
        media_url: str,
        dest_dir: Path,
        referer: str = "",
        title: str = "",
    ) -> dict:
        """用 ffmpeg 拉取 HLS(m3u8) 流。

        相比 yt-dlp，ffmpeg 不依赖 ``Content-Length`` 校验分片长度，
        对这类「每次请求内容都会变化」的动态 m3u8 站点更宽容。
        """
        if not self.ffmpeg_available():
            raise VideoFetchError("未检测到 ffmpeg，无法使用该下载方式")

        name = re.sub(r"[^\w.\-]+", "_", (title or "").strip()) or "video"
        if len(name) > 80:
            name = name[:80]
        out = Path(dest_dir) / f"{name}.mp4"

        headers = self._media_headers(referer)
        header_block = "".join(f"{k}: {v}" + CRLF for k, v in headers.items())

        cmd = [shutil.which("ffmpeg") or "ffmpeg", "-y", "-v", "warning"]
        if header_block:
            cmd += ["-headers", header_block]
        # 按媒体域名路由决定 ffmpeg 是否走代理（原先恒直连）
        _ff_proxy = self._proxy_for(media_url)
        if _ff_proxy:
            cmd += ["-http_proxy", _ff_proxy]
        # -progress pipe:1 让 ffmpeg 把实时进度输出到 stdout，方便写日志
        cmd += [
            "-i", media_url,
            "-c", "copy", "-bsf:a", "aac_adtstoasc",
            "-progress", "pipe:1", "-nostats",
            str(out),
        ]

        total_s = self._lookup(self._duration_cache, media_url)
        logger.info(
            f"[mailvideo] ffmpeg 开始下载: {Path(out).name}"
            + (f"（预计 {total_s:.0f}s）" if total_s else "")
        )

        start = time.time()
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        # stderr 单独抽干，避免管道写满导致 ffmpeg 卡死
        err_lines: list[str] = []
        err_thread = threading.Thread(
            target=self._drain_stderr, args=(proc, err_lines), daemon=True
        )
        err_thread.start()

        # 主线程读 stdout 的 -progress 流，定期打日志
        last_log = 0.0
        out_time_s = 0.0
        try:
            for raw in proc.stdout:  # type: ignore[union-attr]
                line = raw.strip()
                if line.startswith("out_time_ms="):
                    try:
                        out_time_s = int(line.split("=", 1)[1]) / 1_000_000
                    except ValueError:
                        continue
                    now = time.time()
                    if now - last_log >= self.progress_interval:
                        last_log = now
                        self._log_download_progress(out, out_time_s, total_s, start)
            proc.wait(timeout=self.download_timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise VideoFetchError(f"ffmpeg 下载超时（>{self.download_timeout}s）")
        finally:
            if proc.poll() is None:
                proc.kill()

        if not out.exists() or out.stat().st_size == 0:
            tail = [l for l in err_lines if l.strip()][-3:]
            raise VideoFetchError("ffmpeg 未生成有效文件: " + " | ".join(tail))

        self._log_download_progress(out, out_time_s, total_s, start, final=True)

        return {
            "path": str(out),
            "title": out.stem,
            "webpage_url": media_url,
            "filesize": out.stat().st_size,
        }

    # ------------------------------------------------------------------ 进度与日志
    @staticmethod
    def _lookup(cache: dict, url: str) -> float:
        """在缓存里找 url 对应的值，找不到就按去查询串再找一次。"""
        if url in cache:
            return cache[url]
        bare = url.split("?", 1)[0]
        for k, v in cache.items():
            if k.split("?", 1)[0] == bare:
                return v
        return 0.0

    @staticmethod
    def _drain_stderr(proc, sink: list) -> None:
        """后台抽干 stderr，避免子进程因管道写满而卡住。"""
        try:
            for line in proc.stderr:  # type: ignore[union-attr]
                sink.append(line)
        except Exception:
            pass

    @staticmethod
    def _human_size(num: float) -> str:
        for unit in ("B", "KB", "MB", "GB"):
            if num < 1024 or unit == "GB":
                return f"{num:.2f}{unit}" if unit != "B" else f"{int(num)}B"
            num /= 1024
        return f"{num:.2f}GB"

    def _log_download_progress(
        self, out: Path, out_time_s: float, total_s: float, start: float, final: bool = False
    ):
        """把下载进度写进日志：已下体积 + 百分比 + 速度。"""
        try:
            size = out.stat().st_size if out.exists() else 0
        except OSError:
            size = 0
        elapsed = max(time.time() - start, 0.001)
        speed = size / elapsed
        if total_s > 0:
            ratio = out_time_s / total_s * 100
            pct = "100.0%" if final else f"{min(ratio, 99.9):.1f}%"
        else:
            pct = "—"
        head = "✅ 下载完成" if final else "⏳ 下载中"
        logger.info(
            f"[mailvideo] {head} {Path(out).name} | "
            f"已下载 {self._human_size(size)} | 进度 {pct} "
            f"({out_time_s:.0f}s/{total_s:.0f}s) | "
            f"速度 {self._human_size(speed)}/s | 耗时 {elapsed:.1f}s"
        )

    # ============================================================== #
    # 画质压缩
    # ============================================================== #
    def _pick_video_encoder(self) -> list[str]:
        """挑编码器参数：有 NVENC 用硬件（几乎不占 CPU），否则 libx264。

        只在「发送前瘦身」这条可选路径上用到，失败会整条降级成不压缩，
        所以这里直接探一次可用性，不做持久缓存。
        """
        nvenc = ["-c:v", "h264_nvenc", "-preset", "p4", "-rc", "vbr", "-cq", "27"]
        x264 = ["-c:v", "libx264", "-preset", "veryfast", "-crf", "27"]
        try:
            probe = subprocess.run(
                [self._ffmpeg_bin(), "-hide_banner", "-loglevel", "error",
                 "-f", "lavfi", "-i", "color=c=black:s=256x144:d=1",
                 *nvenc, "-f", "null", "-"],
                capture_output=True, timeout=20,
            )
            if probe.returncode == 0:
                return nvenc
        except Exception:
            pass
        return x264

    def shrink_for_send(
        self,
        path: Path,
        target_mb: float,
        max_height: int = 720,
    ) -> dict:
        """把视频压到 ``target_mb`` 附近再发，返回体积变化信息。

        QQ 视频消息是**整包上行**，上限速度就是本机上行带宽：500MB 的片子
        在 5Mbps 上行上光上传就要十几分钟。先把码率压到「目标体积/时长」
        对应值，上行字节直接砍掉大半，画质在手机屏幕上基本看不出差别。

        代价是转码时间（2 核 libx264 veryfast 约 2~3x 实时），所以配置项
        默认 0（关闭），只在明确会嫌传得慢的大文件场景打开。
        调用失败一律返回 ``{"skipped": True}``，不影响正常发送。
        """
        path = Path(path)
        out: dict = {"skipped": True, "path": str(path)}
        try:
            src_mb = path.stat().st_size / 1024 / 1024
        except OSError:
            return out
        if src_mb <= target_mb:
            return out
        if not self.ffmpeg_available():
            logger.warning("[mailvideo] 未检测到 ffmpeg，跳过发送前压缩")
            return out

        info = self.probe_video(path)
        dur = float(info.get("duration") or 0)
        if dur <= 0.5:
            logger.warning("[mailvideo] 读不到时长，跳过发送前压缩")
            return out

        audio_kbps = 96
        # 目标视频码率：目标体积按 95% 留余量，扣掉音频
        v_kbps = int(target_mb * 0.95 * 1024 * 1024 * 8 / dur / 1000) - audio_kbps
        v_kbps = max(200, min(v_kbps, int(info.get("bitrate") or 0) // 1000 or v_kbps))

        cmd = [self._ffmpeg_bin(), "-y", "-nostdin", "-v", "warning", "-i", str(path)]
        height = int(info.get("height") or 0)
        if max_height and height > max_height:
            cmd += ["-vf", f"scale=-2:{max_height}"]
        cmd += self._pick_video_encoder()
        cmd += [
            "-b:v", f"{v_kbps}k",
            "-maxrate", f"{int(v_kbps * 1.3)}k",
            "-bufsize", f"{int(v_kbps * 2)}k",
            "-c:a", "aac", "-b:a", f"{audio_kbps}k",
            "-movflags", "+faststart",
            str(path.with_name(f"{path.stem}.send.mp4")),
        ]

        logger.info(
            f"[mailvideo] 发送前瘦身: {src_mb:.1f}MB → 目标 {target_mb:.0f}MB"
            f"（{v_kbps}kbps，{dur:.0f}s）…"
        )
        t0 = time.time()
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=max(120, int(dur * 2)))
        except subprocess.TimeoutExpired:
            logger.warning("[mailvideo] 压缩超时，改为直接发送原文件")
            return out
        if proc.returncode != 0:
            tail = (proc.stderr or "").strip().splitlines()[-2:]
            logger.warning(f"[mailvideo] 压缩失败，改为直接发送原文件: {' | '.join(tail)}")
            try:
                path.with_name(f"{path.stem}.send.mp4").unlink(missing_ok=True)
            except OSError:
                pass
            return out

        new = path.with_name(f"{path.stem}.send.mp4")
        if not new.exists() or new.stat().st_size == 0:
            return out
        new_mb = new.stat().st_size / 1024 / 1024
        if new_mb >= src_mb * 0.95:
            # 压不下去（源码率本来就低），别浪费那点差别
            new.unlink(missing_ok=True)
            logger.info(f"[mailvideo] 压缩收益可忽略（{new_mb:.1f}MB），发送原文件")
            return out
        logger.info(
            f"[mailvideo] 瘦身完成: {src_mb:.1f}MB → {new_mb:.1f}MB"
            f"（-{(1 - new_mb / src_mb) * 100:.0f}%，耗时 {time.time() - t0:.1f}s）"
        )
        return {
            "skipped": False,
            "path": str(new),
            "original_path": str(path),
            "original_mb": src_mb,
            "size_mb": new_mb,
            "seconds": round(time.time() - t0, 1),
        }

    def _ffmpeg_bin(self) -> str:
        if self.ffmpeg_path and Path(self.ffmpeg_path).exists():
            return str(self.ffmpeg_path)
        return shutil.which("ffmpeg") or "ffmpeg"

    def _ffprobe_bin(self) -> str:
        """找 ffprobe：优先与 ffmpeg 同目录，再退回 PATH。"""
        if self.ffmpeg_path and Path(self.ffmpeg_path).exists():
            cand = Path(self.ffmpeg_path).parent / "ffprobe"
            if cand.exists():
                return str(cand)
        return shutil.which("ffprobe") or "ffprobe"

    def probe_video(self, path: Path) -> dict:
        """用 ffprobe 读出视频的宽高、时长、码率。"""
        info = {"width": 0, "height": 0, "duration": 0.0, "bitrate": 0}
        try:
            proc = subprocess.run(
                [
                    self._ffprobe_bin(), "-v", "error",
                    "-select_streams", "v:0",
                    "-show_entries", "stream=width,height",
                    "-show_entries", "format=duration,bit_rate",
                    "-of", "json", str(path),
                ],
                capture_output=True, text=True, timeout=60,
            )
            data = json.loads(proc.stdout or "{}")
            st = (data.get("streams") or [{}])[0]
            fmt = data.get("format") or {}
            info["width"] = int(st.get("width") or 0)
            info["height"] = int(st.get("height") or 0)
            info["duration"] = float(fmt.get("duration") or 0)
            info["bitrate"] = int(fmt.get("bit_rate") or 0)
        except Exception as e:
            logger.debug(f"[mailvideo] ffprobe 探测失败（忽略）: {e}")
        return info

    def _within_size_limit(self, media_url: str, max_size_mb: int, referer: str = "") -> bool:
        """读取 m3u8 播放列表，估算总体积/时长是否在限制内。

        估算失败（非 HLS、网络异常等）时返回 ``True``，交给后续流程处理。
        """
        try:
            hdrs = self._media_headers(referer)
            bw = 0
            with contextlib.nullcontext(
                self._shared_client(media_url, timeout=self.timeout, kind="probe")
            ) as client:
                resp = client.get(media_url, headers=hdrs)
                resp.raise_for_status()
                text = resp.text
                final_url = media_url
                if "#EXT-X-STREAM-INF" in text:
                    # 主播放列表：按码率上限选档，之后一律用选中那一路估算，
                    # 保证「预检的体积」和「实际下载的体积」是同一个档。
                    final_url, text, bw = self._pick_variant(text, final_url, client, hdrs)

            # 不是 HLS 清单（也不是 m3u8）→ 无法估算，放行交给 yt-dlp
            if "#EXTM3U" not in text and "#EXTINF" not in text:
                return True
            if "#EXT-X-STREAM-INF" in text:  # 选档后仍是 master，说明结构异常
                return True

            dur = 0.0
            for m in re.finditer(r"#EXTINF:([\d.]+)", text):
                dur += float(m.group(1))
            if dur <= 0:
                return True
            seg_urls = [ln.strip() for ln in text.splitlines()
                        if ln.strip() and not ln.startswith("#")]
            seg = len(seg_urls)
            if seg <= 0:
                return True

            # 体积估算优先级：master 的 BANDWIDTH（最准、零成本）→ 首片实测
            # 长度 × 分片数 → 1.5Mbps 兜底。预检就是为了不白下几百 MB，
            # 值得多发一个 HEAD。
            if bw > 0:
                est_bytes = dur * bw / 8
                how = f"{bw / 1_000_000:.2f}Mbps"
            else:
                est_bytes = 0.0
                try:
                    first = urljoin(final_url, seg_urls[0])
                    hr = client.head(first, headers=hdrs, timeout=min(self.probe_timeout + 3, 8))
                    clen = int(hr.headers.get("Content-Length") or 0)
                    if clen > 0:
                        est_bytes = clen * seg
                except Exception:
                    est_bytes = 0.0
                if est_bytes > 0:
                    how = f"首片 {est_bytes / seg / 1024:.0f}KB × {seg}"
                else:
                    est_bytes = dur * 1.5 * 1_000_000 / 8
                    how = "1.5Mbps 兜底"

            est_mb = est_bytes / 1024 / 1024
            # 缓存播放列表文本，下载阶段直接复用，省一次往返
            if len(self._playlist_cache) > 32:  # 失败任务会残留，别无限涨
                self._playlist_cache.clear()
            self._playlist_cache[media_url] = (final_url, text)
            # 缓存下来，下载阶段用来算进度百分比
            self._duration_cache[media_url] = dur
            ok = est_mb <= max_size_mb
            logger.info(
                f"[mailvideo] 预检: 时长 {dur:.0f}s | 分片 {seg} | 估算 {est_mb:.0f}MB"
                f"（{how}）| 限制 {max_size_mb}MB -> {'通过' if ok else '超限跳过'}"
            )
            return ok
        except Exception as e:
            logger.warning(f"[mailvideo] 体积预检失败（放行）: {e}")
            return True

    def _download_via_ytdlp(
        self,
        url: str,
        dest_dir: Path,
        extra_opts: dict,
        desc: str,
        headers: Optional[dict] = None,
    ) -> dict:
        import yt_dlp

        ydl_opts = self._base_ydl_opts()
        if headers:
            ydl_opts["http_headers"] = headers
        ydl_opts.update(
            {
                "noplaylist": True,
                "outtmpl": {"default": str(dest_dir / "%(title).60B [%(id).16B].%(ext)s")},
                "trim_file_name": 150,
                "overwrites": True,
                # 关掉 yt-dlp 自带输出，改用我们统一的进度日志
                "quiet": True,
                "no_warnings": True,
                "noprogress": True,
                "progress_hooks": [self._make_ytdlp_hook(desc)],
            }
        )
        ydl_opts.update(extra_opts)

        # 媒体下载按域名分流（搜索页仍恒走代理）
        media_proxy = self._proxy_for(url)
        if media_proxy:
            ydl_opts["proxy"] = media_proxy
        else:
            ydl_opts.pop("proxy", None)

        logger.info(f"[mailvideo] yt-dlp 开始下载（{desc}）: {url}")
        _yt_start = time.time()
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)

        path = self._resolve_output_path(info, dest_dir)
        if not path or not Path(path).exists():
            raise VideoFetchError(f"yt-dlp({desc}) 未生成文件")
        try:
            logger.info(
                f"[mailvideo] ✅ yt-dlp 下载完成 {Path(path).name} | "
                f"{self._human_size(Path(path).stat().st_size)} | 耗时 {time.time() - _yt_start:.1f}s"
            )
        except OSError:
            pass
        try:
            fs = Path(path).stat().st_size
        except OSError:
            fs = 0
        return {
            "path": str(path),
            "title": (info or {}).get("title") or Path(path).stem,
            "webpage_url": (info or {}).get("webpage_url") or url,
            "filesize": fs,
        }

    def _make_ytdlp_hook(self, desc: str):
        """生成 yt-dlp 进度回调：按固定间隔把进度写进日志。"""
        state = {"last": 0.0, "start": time.time()}

        def _hook(d):
            status = d.get("status")
            if status == "downloading":
                now = time.time()
                if now - state["last"] < self.progress_interval:
                    return
                state["last"] = now
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                got = d.get("downloaded_bytes") or 0
                speed = d.get("speed") or 0
                eta = d.get("eta")
                pct = f"{got / total * 100:.1f}%" if total else "—"
                extra = f" | 剩余 {int(eta)}s" if eta else ""
                logger.info(
                    f"[mailvideo] ⏳ 下载中({desc}) {self._human_size(got)}/{self._human_size(total) if total else '?'} "
                    f"| 进度 {pct} | 速度 {self._human_size(speed)}/s"
                    f" | 耗时 {now - state['start']:.1f}s{extra}"
                )
            elif status == "finished":
                logger.info(f"[mailvideo] 📦 分片下载完毕（{desc}），正在合并/转码…")

        return _hook

    @staticmethod
    def _resolve_output_path(info: Optional[dict], dest_dir: Path) -> Optional[str]:
        if info:
            reqs = info.get("requested_downloads") or []
            for r in reqs:
                fp = r.get("filepath") or r.get("_filename")
                if fp and Path(fp).exists():
                    return fp
            for key in ("filepath", "_filename"):
                fp = info.get(key)
                if fp and Path(fp).exists():
                    return fp
        # 兜底：取目录里最新生成的文件
        files = [p for p in Path(dest_dir).glob("*") if p.is_file()]
        if files:
            return str(max(files, key=lambda p: p.stat().st_mtime))
        return None

    # ============================================================== #
    # 媒体域名分流 + 共享连接池
    # ============================================================== #
    @staticmethod
    def _parse_hosts(raw: str) -> list[str]:
        """解析 ``a.com, b.com`` 形式的域名配置（支持逗号/空格/换行分隔）。"""
        if not raw:
            return []
        return [
            t.strip().lower().lstrip(".")
            for t in re.split(r"[,\s\n]+", str(raw))
            if t.strip()
        ]

    @staticmethod
    def _host_matches(host: str, patterns: list[str]) -> bool:
        """域名后缀匹配：``cdn.a.com`` 命中 ``a.com``。"""
        host = (host or "").lower()
        return any(host == p or host.endswith("." + p) for p in patterns)

    def _proxy_for(self, url: str) -> Optional[str]:
        """媒体请求的代理决策：能直连就直连，直连不通用代理。

        站点页面仍走 :meth:`_proxy_arg`（恒代理）；只有媒体下载（m3u8/密钥/
        分片/直链）走这里。判定结果按 host 缓存，同一个 CDN 只探测一次。
        """
        if not self.proxy:
            return None
        host = (urlparse(url).hostname or "").lower()
        if not host:
            return self._proxy_arg()
        if self._host_matches(host, self._force_hosts):
            return self._proxy_arg()
        if self._host_matches(host, self._bypass_hosts):
            return None
        with self._route_lock:
            cached = self._host_route.get(host)
            lock = self._host_probe_locks.setdefault(host, threading.Lock())
        if cached is not None:
            return None if cached else self._proxy_arg()
        if not self.auto_probe_direct:
            return self._proxy_arg()
        # 单飞：多个线程同时首访同一 host 时只探一次，其余等结果——
        # 否则 16 个 worker 会各发一次探测请求，白白多出 15 次往返。
        with lock:
            with self._route_lock:
                cached = self._host_route.get(host)
            if cached is not None:
                return None if cached else self._proxy_arg()
            direct_ok = self._probe_direct(url)
            with self._route_lock:
                self._host_route[host] = direct_ok
            logger.info(
                f"[mailvideo] 媒体域名路由: {host} -> "
                f"{'直连' if direct_ok else '走代理'}（自动探测）"
            )
        return None if direct_ok else self._proxy_arg()

    def _probe_direct(self, url: str) -> bool:
        """直连试探该 host 是否可达；被墙/超时则判为需要代理。"""
        import httpx

        try:
            with httpx.Client(
                headers=self._media_headers(),
                timeout=httpx.Timeout(self.probe_timeout, connect=self.probe_timeout),
                follow_redirects=True,
                proxy=None,
            ) as c:
                r = c.get(url, headers={"Range": "bytes=0-1023"})
                return r.status_code < 400
        except Exception as e:
            logger.info(f"[mailvideo] 直连探测失败，回退代理: {type(e).__name__}: {e}")
            return False

    def _demote_host(self, url: str, reason: str = "") -> None:
        """直连中途出错时把该 host 降级为走代理，后续请求不再重蹈覆辙。"""
        host = (urlparse(url).hostname or "").lower()
        if not host:
            return
        with self._route_lock:
            if self._host_route.get(host) is False:
                return
            self._host_route[host] = False
        logger.warning(f"[mailvideo] 直连异常，{host} 降级走代理 {reason}")

    def _client_bucket(self, url: str, kind: str = "seg") -> str:
        """该 URL 在该用途下属于哪个共享分桶（直连/代理 × 用途）。

        分桶是为了让「代理路线」与「直连路线」各自维护连接池，同一桶内的
        所有请求复用同一组 TCP/TLS 连接。
        """
        use_proxy = bool(self._proxy_for(url))
        return f"{kind}:{'proxy' if use_proxy else 'direct'}"

    def _pool_workers(self, kind: str) -> int:
        """该用途的并发线程数（连接池上限按它 × 2 开）。"""
        # m3u8 / 密钥 / 分片 / 预检共用，跑满 hls_parallel_workers 个线程
        return max(1, int(self.hls_parallel_workers or 1))

    def _shared_client(self, url: str, timeout=None, kind: str = "seg"):
        """取共享 Client（按「直连/代理 × 用途」分桶），复用连接池。

        原先每个分片都新建一次 Client，等于每片重做 TCP+TLS 握手（经过代理
        时代价更大）；改成按桶共享，keep-alive 连接直接复用。

        连接池上限 = 线程数 × 2（keepalive 30s）：连接可复用时单连接开销
        大幅下降，所以并发可以放心开大一点，不会明显增加握手压力。
        """
        import httpx

        bucket = self._client_bucket(url, kind)
        with self._client_lock:
            c = self._shared_clients.get(bucket)
            if c is not None and not c.is_closed:  # type: ignore[attr-defined]
                return c
            workers = self._pool_workers(kind)
            opts = dict(
                headers=self._http_headers(),
                timeout=timeout or httpx.Timeout(60.0, connect=15.0),
                follow_redirects=True,
                proxy=(self.proxy or None) if bucket.endswith(":proxy") else None,
                limits=httpx.Limits(
                    max_connections=workers * self._POOL_OVERCOMMIT,
                    max_keepalive_connections=workers,
                    keepalive_expiry=self._KEEPALIVE_EXPIRY,
                ),
            )
            try:
                c = httpx.Client(http2=self.http2, **opts)
            except Exception as e:
                # h2 未安装 / 代理不支持时回退 HTTP/1.1，不让下载整体失败
                logger.warning(f"[mailvideo] HTTP/2 不可用，回退 HTTP/1.1: {e}")
                self.http2 = False
                c = httpx.Client(**opts)
            logger.debug(
                f"[mailvideo] 新建共享连接池 {bucket}"
                f"（上限 {workers * self._POOL_OVERCOMMIT} 连接 / keepalive "
                f"{self._KEEPALIVE_EXPIRY:.0f}s）"
            )
            self._shared_clients[bucket] = c
            return c

    def close(self) -> None:
        """释放共享连接池（插件重载/退出时调用）。"""
        with self._client_lock:
            for c in self._shared_clients.values():
                try:
                    c.close()  # type: ignore[attr-defined]
                except Exception:
                    pass
            self._shared_clients.clear()

    def _proxy_arg(self):
        return self.proxy or None

    def _base_ydl_opts(self) -> dict:
        opts: dict = {
            "quiet": True,
            "no_warnings": True,
            "concurrent_fragment_downloads": max(1, self.hls_parallel_workers),
            # 分片下载并发（HLS/DASH）。
            "nocheckcertificate": True,
            "retries": 3,
            "fragment_retries": 3,
            "socket_timeout": self.timeout,
            # 大文件按块拉取，单连接也能跑满带宽；yt-dlp 会校验总长度并自动重试。
            "http_chunk_size": 10 * 1024 * 1024,
            "buffersize": 1024 * 1024,
            "http_headers": self._http_headers(),
        }
        if self.proxy:
            opts["proxy"] = self.proxy
        ff = self._ffmpeg_location()
        if ff:
            opts["ffmpeg_location"] = ff
        return opts
