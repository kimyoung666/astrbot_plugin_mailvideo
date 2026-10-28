"""站点适配层：处理「通用 HTML 规则抓不到」的 SPA + JSON API 站。

设计原则
--------
通用流程（``search_url_template`` 拼 URL → 抓 HTML 找 ``<a>`` → 播放页正则
抠 m3u8）只适用于服务端渲染的传统站。下面这类站点会全流程失效：

* 页面是 ``document.write(decodeURIComponent("..."))`` 的壳，任何路径
  （``/index.htm`` / ``/type/10.htm`` / ``/search.htm``）返回**同一份** HTML；
* 视频数据由前端 Vue app 请求 JSON 接口后渲染，服务器返回的 HTML 里没有
  任何视频链接；
* 播放地址不是直链，而是前端用 ``serial_number`` 拼出来的 m3u8；
* 标题走单字符替换表混淆（``decrypt-text``）。

本模块为这类站点提供**显式适配器**：命中则短路通用流程，不命中返回
``None``，原流程完全不受影响。

目前适配
--------
``tiangou``：添狗资源 / 天狗系（tiangou2.cc 发布页 → 反查真实站点）。
"""

from __future__ import annotations

import html as _html
import json
import re
import threading
import time
from typing import Any, Optional
from urllib.parse import quote, unquote, urljoin, urlparse

from astrbot.api import logger

__all__ = [
    "SiteAdapters",
    "TiangouAdapter",
    "decrypt_text",
    "load_site_adapters",
]

# ====================================================================== #
# 标题混淆还原（对应站点 public.min.js 里的 Decrypt()）
# ====================================================================== #
_DECRYPT_MAP = {
    "e": "P", "w": "D", "T": "y", "+": "J", "l": "!", "t": "L", "E": "E",
    "@": "2", "d": "a", "b": "%", "q": "l", "X": "v", "~": "R", "5": "r",
    "&": "X", "C": "j", "]": "F", "a": ")", "^": "m", ",": "~", "}": "1",
    "x": "C", "c": "(", "G": "@", "h": "h", ".": "*", "L": "s", "=": ":",
    "p": "g", "I": "Q", "1": "7", "_": "u", "K": "6", "F": "t", "2": "n",
    "8": "=", "k": "G", "Z": "]", ")": "b", "P": "}", "B": "U", "S": "k",
    "6": "i", "g": ":", "N": "N", "i": "S", "%": "+", "-": "Y", "?": "|",
    "4": "z", "*": "-", "3": "^", "[": "{", "(": "c", "u": "B", "y": "M",
    "U": "Z", "H": "[", "z": "K", "9": "H", "7": "f", "R": "x", "v": "&",
    "!": ";", "M": "_", "Q": "9", "Y": "e", "o": "4", "r": "A", "m": ".",
    "O": "o", "V": "W", "J": "p", "f": "d", ":": "q", "{": "8", "W": "I",
    "j": "?", "n": "5", "s": "3", "|": "T", "A": "V", "D": "w", ";": "O",
}


def decrypt_text(raw: str) -> str:
    """还原站点的 ``decrypt-text`` 标题。

    先按替换表逐字符映射，再 ``html.unescape`` 一次——站点把中文写成了
    ``&#22269;`` 这类数字实体，两层都要处理。
    """
    if not raw:
        return ""
    decoded = "".join(_DECRYPT_MAP.get(ch, ch) for ch in str(raw))
    return _html.unescape(decoded).strip()


# ====================================================================== #
# 通用小工具
# ====================================================================== #
_TARGET_RE = re.compile(r"https?://[A-Za-z0-9.\-]+(?::\d{2,5})?", re.IGNORECASE)
_VIDEO_ID_RE = re.compile(r"/video/(\d+)\.(?:html?|json)", re.IGNORECASE)
_MELON_ID_RE = re.compile(r"/melon/(\d+)\.(?:html?|json)", re.IGNORECASE)
_LANDING_MARK = "decodeURIComponent("
# 发布页里混着的推广/源码/邮箱域名，先排掉能少发几次探测请求
_SKIP_HOST_HINTS = (
    "github.com",
    "gmail.com",
    "google.com",
    "t.me",
    "twitter.com",
    "jf92vvv.cyou",
    "rhpeda.com",
)


class TiangouAdapter:
    """添狗资源（tiangou）系站点适配器。

    站点结构（实测）：

    * 发布页（如 ``www.tiangou2.cc``）只有一句
      ``document.write(decodeURIComponent("..."))``，真实站点用 HTML 实体
      拼在里面（``&#119;&#119;...``）；
    * 真实站点是 Vue SPA，``/data.json`` 里给出 ``csstime`` /
      ``novel_domain`` 等全局配置；
    * 搜索：``GET /search.json?search={关键词}&{csstime}``；
    * 详情：``GET /video/{id}.json``（**不含播放地址**，只有
      ``serial_number``）；
    * 播放：``{novel_domain}/m3u8/{serial_number}/index_domain.m3u8?{csstime}``。
    """

    name = "tiangou"
    #: 站点地址解析结果的缓存时长（秒）
    RESOLVE_TTL = 1800

    def __init__(
        self,
        fetcher: Any,
        ttl: int = RESOLVE_TTL,
        melon_pages: int = 3,
        search_pages: int = 2,
    ) -> None:
        self.fetcher = fetcher
        self.ttl = max(60, int(ttl or RESOLVE_TTL))
        # 吃瓜栏目没有关键词接口，只能拉列表再本地过滤标题；
        # 这里限制最多翻几页（每页 14 条），0 = 不搜吃瓜。
        self.melon_pages = max(0, int(melon_pages or 0))
        # 视频搜索接口支持 page 参数（实测全站 search 有上百页），
        # 只抓第一页容易漏掉长尾关键词的命中，默认抓 2 页。
        self.search_pages = max(1, int(search_pages or 1))
        self._lock = threading.Lock()
        # 输入地址 -> {"real": 真实站, "ts": 解析时间}
        self._resolved: dict[str, dict[str, Any]] = {}
        # 真实站 -> 站点全局配置（csstime / novel_domain / pic_domain）
        self._meta: dict[str, dict[str, str]] = {}

    # ------------------------------------------------------------------ #
    # HTTP
    # ------------------------------------------------------------------ #
    def _client(self):
        import httpx

        return httpx.Client(
            headers=self.fetcher._http_headers(),
            timeout=max(8, int(getattr(self.fetcher, "timeout", 30) or 30)),
            follow_redirects=True,
            proxy=self.fetcher._proxy_arg(),
        )

    def _get(self, url: str, timeout: Optional[float] = None):
        with self._client() as client:
            return client.get(url, timeout=timeout) if timeout else client.get(url)

    def _get_text(self, url: str, timeout: Optional[float] = None) -> str:
        try:
            resp = self._get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.info(f"[mailvideo] 适配器请求失败 {url[:100]}: {type(e).__name__}: {e}")
            return ""

    def _get_json(self, url: str) -> Any:
        text = self._get_text(url)
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            return None

    # ------------------------------------------------------------------ #
    # 站点解析
    # ------------------------------------------------------------------ #
    @staticmethod
    def _normalize(base: str) -> str:
        base = (base or "").strip()
        if not base:
            return ""
        if not base.startswith(("http://", "https://")):
            base = "https://" + base
        return base.rstrip("/")

    def _fetch_group(self, base: str) -> Optional[dict]:
        """拉 ``/data.json``，拿到站点全局配置则返回（用来确认真实站）。"""
        text = self._get_text(f"{base}/data.json")
        if not text or '"novel_domain"' not in text:
            return None
        start, end = text.find("{"), text.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            group = json.loads(text[start : end + 1])
        except Exception:
            return None
        return group if isinstance(group, dict) else None

    def _harvest_from_home(self, home_html: str, base: str) -> list[str]:
        """从发布页 HTML 里挖出候选真实站地址。"""
        text = home_html or ""
        m = re.search(r'decodeURIComponent\("(.*?)"\)', text, re.S)
        if m:
            text = unquote(m.group(1))
        text = _html.unescape(text)

        base_host = urlparse(base).netloc.lower()
        out: list[str] = []
        for url in _TARGET_RE.findall(text):
            host = urlparse(url).netloc.lower()
            if not host or host == base_host:
                continue
            if any(h in host for h in _SKIP_HOST_HINTS):
                continue
            candidate = url.rstrip("/")
            if candidate not in out:
                out.append(candidate)
        return out[:8]

    def resolve(self, base: str, force: bool = False) -> str:
        """把任意入口地址（发布页或真实站）解析成真实站点根地址。

        解析不出来返回空串——说明这个站不吃本适配器，调用方应回退通用流程。
        """
        base = self._normalize(base)
        if not base:
            return ""

        with self._lock:
            hit = self._resolved.get(base)
            if hit and not force and time.time() - hit["ts"] < self.ttl:
                return str(hit["real"])

        # 先抓首页做一次「是不是这类站」的快速判断：普通站只多这一次请求。
        home = self._get_text(base)
        if not home:
            return ""
        looks_like = (
            _LANDING_MARK in home
            or "/data.json" in home
            or "App/dist" in home
        )
        if not looks_like:
            return ""

        real, group = "", None
        # 1) 入口本身就是真实站
        group = self._fetch_group(base)
        if group:
            real = base
        else:
            # 2) 入口是发布页：挖出候选后逐个探测
            for candidate in self._harvest_from_home(home, base):
                found = self._fetch_group(candidate)
                if found:
                    real, group = candidate, found
                    break

        if not real or not group:
            return ""

        meta = {
            "csstime": str(group.get("csstime") or ""),
            "novel_domain": str(group.get("novel_domain") or "").rstrip("/"),
            "pic_domain": str(group.get("pic_domain") or "").rstrip("/"),
            "path": str(group.get("path") or ""),
        }
        with self._lock:
            self._resolved[base] = {"real": real, "ts": time.time()}
            self._resolved[real] = {"real": real, "ts": time.time()}
            self._meta[real] = meta
        if real != base:
            logger.info(f"[mailvideo] 适配器：发布页 {base} → 真实站 {real}")
        return real

    def _meta_of(self, real: str) -> dict[str, str]:
        with self._lock:
            return dict(self._meta.get(real) or {})

    # ------------------------------------------------------------------ #
    # 搜索
    # ------------------------------------------------------------------ #
    def search(self, base: str, keyword: str, limit: int = 30) -> Optional[list[dict]]:
        """返回候选列表；返回 ``None`` 表示「不是本适配器的站点」。"""
        real = self.resolve(base)
        if not real:
            return None

        csstime = self._meta_of(real).get("csstime") or ""
        kw_q = quote(keyword)
        import concurrent.futures as _cf

        def _fetch_page(page: int) -> Any:
            suffix = f"&page={page}" if page > 1 else ""
            return self._get_json(f"{real}/search.json?search={kw_q}{suffix}&{csstime}")

        pages = list(range(1, self.search_pages + 1))
        payloads: list[Any] = []
        if len(pages) == 1:
            payloads = [_fetch_page(1)]
        else:
            try:
                with _cf.ThreadPoolExecutor(max_workers=min(4, len(pages))) as pool:
                    payloads = list(pool.map(_fetch_page, pages))
            except Exception as e:
                logger.warning(f"[mailvideo] 适配器：视频搜索翻页失败，退回单页: {e}")
                payloads = [_fetch_page(1)]

        if not any(isinstance(p, dict) and "videos" in p for p in payloads):
            return None

        results: list[dict] = []
        seen_ids: set[str] = set()
        for data in payloads:
            if not isinstance(data, dict):
                continue
            for item in data.get("videos") or []:
                vid = item.get("id")
                if not vid or str(vid) in seen_ids:
                    continue
                seen_ids.add(str(vid))
                title = decrypt_text(str(item.get("title") or ""))
                results.append(
                    {
                        "url": f"{real}/video/{vid}.htm",
                        "title": title or f"{keyword} #{vid}",
                        "duration": item.get("second"),
                        "adapter": self.name,
                        "source": "视频",
                    }
                )

        # 关键词真出现在标题里的排前面（站点返回顺序未必相关）
        kw_low = (keyword or "").strip().lower()
        if kw_low:
            results.sort(key=lambda r: 0 if kw_low in str(r.get("title") or "").lower() else 1)
        results = results[:limit]

        # 吃瓜（melon）栏目：没有关键词接口，拉列表解密标题后本地过滤
        melon_hits = self._search_melons(real, keyword, csstime)
        if melon_hits:
            results.extend(melon_hits)

        logger.info(
            f"[mailvideo] 适配器({self.name}) 搜到 {len(results)} 个候选 "
            f"（视频 {len(results) - len(melon_hits)} / 吃瓜 {len(melon_hits)}）"
            f"| 关键词「{keyword}」| 站 {real}"
        )
        return results

    def _search_melons(
        self, real: str, keyword: str, csstime: str, limit: int = 10
    ) -> list[dict]:
        """在吃瓜列表里按标题找（列表接口不支持关键词，只能本地过滤）。"""
        if self.melon_pages <= 0:
            return []
        kw = (keyword or "").strip().lower()
        if not kw:
            return []

        out: list[dict] = []
        # 并发拉列表：翻 3 页串行要十几秒，并发后基本等于一次请求的耗时
        import concurrent.futures as _cf

        pages = list(range(1, self.melon_pages + 1))
        try:
            with _cf.ThreadPoolExecutor(max_workers=min(4, len(pages))) as pool:
                payloads = list(
                    pool.map(
                        lambda p: self._get_json(f"{real}/melons_0_{p}.json?{csstime}"),
                        pages,
                    )
                )
        except Exception as e:
            logger.warning(f"[mailvideo] 适配器：吃瓜列表拉取失败: {e}")
            return []

        for data in payloads:
            block = (data or {}).get("data") or {}
            melons = block.get("melons") or []
            if not melons:
                break
            for item in melons:
                title = decrypt_text(str(item.get("title") or ""))
                mid = item.get("id")
                if not title or not mid:
                    continue
                if kw not in title.lower():
                    continue
                out.append(
                    {
                        "url": f"{real}/melon/{mid}.htm",
                        "title": title,
                        "adapter": f"{self.name}-melon",
                    }
                )
                if len(out) >= limit:
                    return out
        return out

    # ------------------------------------------------------------------ #
    # 播放地址
    # ------------------------------------------------------------------ #
    def _fetch_playlist(self, url: str) -> str:
        """取 m3u8 文本（一次请求，供可用性判断与域名探测复用）。"""
        return self._get_text(url)

    def _playable(self, url: str) -> bool:
        """轻量校验 m3u8 是否可拉（防止 serial 拼错时把死链丢给下载器）。"""
        return "#EXTM3U" in self._fetch_playlist(url)[:400]

    #: m3u8 里的分片域名失效时按顺序尝试的镜像域名
    _TS_HOST_FALLBACKS = ("ts2.zblsjy.com",)

    def _seg_ok(self, url: str, allow_proxy: bool = True) -> bool:
        """抓一小段分片，确认这个域名真的能出数据（不是 404/超时）。

        分片 CDN 通常直连可达、走代理反而卡死（实测代理对 ts2.zblsjy.com
        返回 200 但 0 字节），所以先直连；``allow_proxy`` 为真时再补一次代理。
        """
        import httpx

        headers = dict(self.fetcher._http_headers())
        headers["Range"] = "bytes=0-2047"
        proxies = (None, self.fetcher._proxy_arg()) if allow_proxy else (None,)
        for proxy in proxies:
            try:
                with httpx.Client(
                    headers=headers,
                    timeout=5,
                    follow_redirects=True,
                    proxy=proxy,
                ) as client:
                    resp = client.get(url)
                    if resp.status_code in (200, 206) and resp.content:
                        return True
            except Exception:
                continue
        return False

    def _ensure_segment_host(self, m3u8_url: str, text: str = "") -> None:
        """分片域名失效时探测镜像并登记改写。

        实测站点会留旧 CDN 域名在 m3u8 里（同一份分片在 `ts2.ywqishu.com`
        全线超时、在 `ts2.zblsjy.com` 正常 200），路径完全一致，只需换域名。
        """
        text = text or self._fetch_playlist(m3u8_url)
        if not text or "#EXTINF" not in text or "#EXT-X-STREAM-INF" in text:
            return

        seg_lines = [
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        if not seg_lines:
            return

        first_seg = urljoin(m3u8_url, seg_lines[0])
        host = urlparse(first_seg).netloc
        if not host:
            return
        # 已经改写过的域名不再重复探测（同一任务里会被调用多次）
        known = getattr(self.fetcher, "_host_rewrites", {}) or {}
        if host.lower() in known:
            return
        if self._seg_ok(first_seg, allow_proxy=False):
            return

        logger.info(f"[mailvideo] 适配器：分片域名 {host} 无响应，尝试镜像域名")
        path = urlparse(first_seg).path
        pool = list(getattr(self.fetcher, "segment_host_fallbacks", []) or [])
        pool += list(self._TS_HOST_FALLBACKS)
        tried: set[str] = set()
        for cand in pool:
            cand = (cand or "").strip().lower()
            if not cand or cand == host.lower() or cand in tried:
                continue
            tried.add(cand)
            if self._seg_ok(f"https://{cand}{path}", allow_proxy=False):
                self.fetcher.register_host_rewrite(host, cand)
                logger.info(
                    f"[mailvideo] 适配器：分片域名已切换到镜像 {cand}（{host} → {cand}）"
                )
                return
        # 镜像也不行：可能只是这个网络需要走代理，最后再带代理复核一次原域名
        if self._seg_ok(first_seg, allow_proxy=True):
            return
        logger.warning(
            f"[mailvideo] 适配器：{host} 不通且未找到可用镜像，"
            f"可在配置 segment_host_fallbacks 里补一个可用域名"
        )

    def media_urls(self, page_url: str) -> Optional[list[str]]:
        """播放页 → 真实 m3u8 列表；返回 ``None`` 表示不适配这个页面。"""
        page_url = page_url or ""
        # 吃瓜详情页（/melon/{id}.htm）与视频详情页走不同接口与不同目录
        if _MELON_ID_RE.search(page_url):
            return self._melon_media_urls(page_url)

        matched = _VIDEO_ID_RE.search(page_url)
        if not matched:
            return None

        parsed = urlparse(page_url)
        root = f"{parsed.scheme}://{parsed.netloc}"
        real = self.resolve(root)
        if not real:
            return None

        meta = self._meta_of(real)
        csstime = meta.get("csstime") or ""
        novel = meta.get("novel_domain") or ""
        if not novel:
            logger.warning("[mailvideo] 适配器：站点配置里没有 novel_domain，无法拼播放地址")
            return None

        vid = matched.group(1)
        data = self._get_json(f"{real}/video/{vid}.json?{csstime}")
        video = (data or {}).get("video") if isinstance(data, dict) else None
        if not isinstance(video, dict):
            logger.info(f"[mailvideo] 适配器：视频 {vid} 详情为空（可能已下架）")
            return None

        serial = str(video.get("serial_number") or "").strip()
        if not serial:
            logger.info(f"[mailvideo] 适配器：视频 {vid} 没有 serial_number")
            return None

        candidates = [
            f"{novel}/m3u8/{serial}/index_domain.m3u8?{csstime}",
            f"{novel}/m3u8/{serial}/index.m3u8?{csstime}",
        ]
        for url in candidates:
            text = self._fetch_playlist(url)
            if "#EXTM3U" in text[:400]:
                # 分片域名可能已失效（站点换 CDN 后 m3u8 里留着旧域名）
                self._ensure_segment_host(url, text)
                logger.info(
                    f"[mailvideo] 适配器：{vid} → serial={serial} → {url[:110]}"
                )
                return [url]
        logger.warning(f"[mailvideo] 适配器：{vid} 的 m3u8 均不可用 serial={serial}")
        return None

    def _melon_media_urls(self, page_url: str) -> Optional[list[str]]:
        """吃瓜详情页 → m3u8。

        结构：``GET /melon/{id}.json`` → ``data.melon.serial_number`` 与
        ``melon_videos[].path``（多段/多集，形如 ``video_0``、``video_1``），
        地址为 ``{novel_domain}/melon_m3u8/{serial}/{path}/index_domain.m3u8``。
        多段时返回第一个可用的，其余按顺序备用。
        """
        matched = _MELON_ID_RE.search(page_url)
        if not matched:
            return None

        parsed = urlparse(page_url)
        root = f"{parsed.scheme}://{parsed.netloc}"
        real = self.resolve(root)
        if not real:
            return None

        meta = self._meta_of(real)
        csstime = meta.get("csstime") or ""
        novel = meta.get("novel_domain") or ""
        if not novel:
            logger.warning("[mailvideo] 适配器：站点配置里没有 novel_domain，无法拼吃瓜地址")
            return None

        mid = matched.group(1)
        data = self._get_json(f"{real}/melon/{mid}.json?{csstime}")
        melon = ((data or {}).get("data") or {}).get("melon") if isinstance(data, dict) else None
        if not isinstance(melon, dict):
            logger.info(f"[mailvideo] 适配器：吃瓜 {mid} 详情为空（可能已下架）")
            return None

        serial = str(melon.get("serial_number") or "").strip()
        if not serial:
            logger.info(f"[mailvideo] 适配器：吃瓜 {mid} 没有 serial_number")
            return None

        videos = melon.get("melon_videos") or []
        paths = [
            str(v.get("path")).strip()
            for v in videos
            if isinstance(v, dict) and str(v.get("path") or "").strip()
        ] or ["video_0"]

        candidates = [
            f"{novel}/melon_m3u8/{serial}/{path}/index_domain.m3u8?{csstime}"
            for path in paths
        ]
        playable: list[str] = []
        playlists: dict[str, str] = {}
        for url in candidates:
            text = self._fetch_playlist(url)
            if "#EXTM3U" in text[:400]:
                playable.append(url)
                playlists[url] = text
        if not playable:
            logger.warning(
                f"[mailvideo] 适配器：吃瓜 {mid} 的 m3u8 均不可用 serial={serial}"
            )
            return None
        logger.info(
            f"[mailvideo] 适配器：吃瓜 {mid} → serial={serial} | "
            f"{len(playable)}/{len(candidates)} 段可用 → {playable[0][:110]}"
        )
        # 吃瓜片源换过 CDN，m3u8 里常留着已失效的分片域名
        self._ensure_segment_host(playable[0], playlists.get(playable[0], ""))
        return playable


# ====================================================================== #
# 适配器集合
# ====================================================================== #
class SiteAdapters:
    """按顺序尝试所有适配器，全部返回 ``None`` 时交回通用流程。"""

    def __init__(
        self, fetcher: Any, enabled: bool = True, melon_pages: int = 3
    ) -> None:
        self.enabled = bool(enabled)
        self.items: list[Any] = []
        if self.enabled:
            for cls in (TiangouAdapter,):
                try:
                    self.items.append(cls(fetcher, melon_pages=melon_pages))
                except Exception as e:  # pragma: no cover
                    logger.warning(f"[mailvideo] 初始化适配器 {cls.__name__} 失败: {e}")

    def search(self, base: str, keyword: str) -> Optional[list[dict]]:
        for adapter in self.items:
            try:
                result = adapter.search(base, keyword)
            except Exception as e:
                logger.warning(
                    f"[mailvideo] 适配器 {getattr(adapter, 'name', '?')} 搜索异常: {e}"
                )
                result = None
            if result is not None:
                return result
        return None

    def media_urls(self, page_url: str) -> Optional[list[str]]:
        for adapter in self.items:
            try:
                result = adapter.media_urls(page_url)
            except Exception as e:
                logger.warning(
                    f"[mailvideo] 适配器 {getattr(adapter, 'name', '?')} 解析播放页异常: {e}"
                )
                result = None
            if result:
                return result
        return None


def load_site_adapters() -> type[SiteAdapters]:
    """便于外部按需引用（当前直接返回类本身）。"""
    return SiteAdapters
