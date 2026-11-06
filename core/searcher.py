from __future__ import annotations

import html as html_lib
import re
from dataclasses import dataclass
from urllib.parse import quote, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from astrbot.api import logger

from .config import PluginConfig, SiteConfig

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

# 一条站点地址时的通用解析：抓页面里所有 <a>
GENERIC_LINK_RE = re.compile(
    r"""<a\b[^>]*?href\s*=\s*["'](?P<url>[^"'#\s]+)["'][^>]*>(?P<title>.*?)</a>""",
    re.S | re.I,
)
TAG_RE = re.compile(r"<[^>]+>")
# 明显不是内容页的路径
SKIP_PATH_PARTS = (
    "/search",
    "/login",
    "/logout",
    "/signup",
    "/register",
    "/user",
    "/member",
    "/account",
    "/tag",
    "/tags",
    "/category",
    "/categories",
    "/help",
    "/faq",
    "/dmca",
    "/terms",
    "/privacy",
    "/contact",
    "/about",
    "/rss",
    "/feed",
    "/comment",
    "/upload",
    "/vip",
    "/premium",
)
SKIP_SUFFIX = (
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
    ".css",
    ".js",
    ".json",
    ".xml",
    ".pdf",
    ".zip",
    ".mp4",
    ".m3u8",
)
SKIP_TITLE_RE = re.compile(
    r"^(下一页|上一页|首页|尾页|登录|注册|搜索|更多|返回|跳转|"
    r"next|prev|previous|home|login|signup|register|search|more|back)\b",
    re.I,
)


def make_client(cfg: PluginConfig, timeout: int | None = None) -> httpx.AsyncClient:
    """构造带代理的 httpx 客户端，兼容新旧版本参数名"""
    kwargs: dict = {
        "timeout": timeout or cfg.timeout,
        "follow_redirects": True,
        "headers": DEFAULT_HEADERS,
    }
    if cfg.proxy:
        try:
            return httpx.AsyncClient(proxy=cfg.proxy, **kwargs)
        except TypeError:  # httpx < 0.26
            return httpx.AsyncClient(proxies=cfg.proxy, **kwargs)
    return httpx.AsyncClient(**kwargs)


@dataclass
class SearchItem:
    site: str
    title: str
    url: str
    cover: str = ""
    duration: str = ""


def _attr(el, names: tuple[str, ...]) -> str:
    for name in names:
        val = el.get(name)
        if val and val.strip():
            return val.strip()
    return ""


def _select(node, selector: str):
    if not selector:
        return None
    try:
        return node.select_one(selector)
    except Exception:
        return None


class Searcher:
    def __init__(self, cfg: PluginConfig):
        self.cfg = cfg
        self.client = make_client(cfg)

    async def close(self):
        await self.client.aclose()

    @property
    def _probe_timeout(self) -> int:
        """探测搜索入口时别等太久"""
        return max(6, min(self.cfg.timeout, 12))

    async def search(self, site: SiteConfig, keyword: str) -> list[SearchItem]:
        urls = site.candidates()
        if not urls:
            raise RuntimeError("站点地址没填，或者填的不是个正常链接")

        tried = 0
        last_error = ""
        fallback: tuple[str, list[SearchItem]] | None = None
        for raw in urls:
            if tried >= 8:
                break
            tried += 1
            url = site.render(raw, keyword)
            try:
                resp = await self.client.request(
                    site.method,
                    url,
                    headers=site.headers or None,
                    timeout=self._probe_timeout,
                )
                resp.raise_for_status()
            except Exception as exc:
                last_error = str(exc)
                continue
            if site.encoding:
                resp.encoding = site.encoding
            body = resp.text

            if site.list_selector:
                items = self._parse_html(site, body)
            elif site.link_regex:
                items = self._parse_regex(site, body)
            else:
                items = self._parse_generic(site, body)
            if items:
                # 通用模式下只扒到一两条，多半是导航残留，先记着继续往下试
                if not site.list_selector and not site.link_regex and len(items) < 2:
                    if fallback is None:
                        fallback = (raw, items)
                    continue
                if raw != site.search_url:
                    logger.info(f"[mailvideo] {site.key} 搜索入口命中: {raw}")
                site.search_url = raw
                return items[: self.cfg.max_results]

        if fallback is not None:
            raw, items = fallback
            site.search_url = raw
            return items[: self.cfg.max_results]
        if last_error:
            raise RuntimeError(f"搜索入口都没通，最后一个错误：{last_error[-160:]}")
        raise RuntimeError(
            f"试了 {tried} 个搜索入口都没扒到结果，站点可能要登录、改版或者得手动填配置"
        )

    # ---------- html 选择器模式 ----------

    def _parse_html(self, site: SiteConfig, html: str) -> list[SearchItem]:
        soup = BeautifulSoup(html, "html.parser")
        try:
            nodes = soup.select(site.list_selector)
        except Exception as exc:
            raise RuntimeError(f"list_selector 不合法: {exc}") from exc

        items: list[SearchItem] = []
        for node in nodes:
            if node.name == "a":
                link_el = node
            else:
                link_el = _select(node, site.item_link) or node.find("a")
            if link_el is None:
                continue
            href = _attr(link_el, ("href",))
            if not href or href.startswith(("javascript:", "#")):
                continue

            title = ""
            title_el = _select(node, site.item_title) if site.item_title else None
            if title_el is not None:
                title = _attr(title_el, ("title",)) or title_el.get_text(" ", strip=True)
            if not title:
                title = _attr(link_el, ("title",)) or link_el.get_text(" ", strip=True)

            cover_el = _select(node, site.item_cover) if site.item_cover else node.find("img")
            cover = ""
            if cover_el is not None:
                cover = _attr(
                    cover_el,
                    ("data-src", "data-original", "data-mediumthumb", "data-lazy-src", "src"),
                )

            duration = ""
            dur_el = _select(node, site.item_duration) if site.item_duration else None
            if dur_el is not None:
                duration = dur_el.get_text(" ", strip=True)

            items.append(
                SearchItem(
                    site=site.key,
                    title=title or "无标题",
                    url=urljoin(site.base + "/", href),
                    cover=urljoin(site.base + "/", cover) if cover else "",
                    duration=duration,
                )
            )
        return items

    # ---------- 正则模式 ----------

    @staticmethod
    def _group(match: re.Match, *names: str) -> str:
        for name in names:
            try:
                val = match.group(name)
            except (IndexError, KeyError):
                continue
            if val:
                return val
        try:
            return match.group(1) or ""
        except IndexError:
            return ""

    def _parse_regex(self, site: SiteConfig, html: str) -> list[SearchItem]:
        items: list[SearchItem] = []
        for match in re.finditer(site.link_regex, html):
            href = self._group(match, "url", "link", "href")
            if not href:
                continue

            title = ""
            if site.title_regex:
                tm = re.search(site.title_regex, match.group(0))
                if tm:
                    title = self._group(tm, "title")
            items.append(
                SearchItem(
                    site=site.key,
                    title=title or "无标题",
                    url=urljoin(site.base + "/", href),
                )
            )
        return items

    # ---------- 通用模式（只填了站点地址） ----------

    @staticmethod
    def _clean(text: str) -> str:
        text = TAG_RE.sub(" ", text or "")
        text = html_lib.unescape(text)
        return re.sub(r"\s+", " ", text).strip()

    def _parse_generic(self, site: SiteConfig, html: str) -> list[SearchItem]:
        host = urlparse(site.base).netloc.lower()
        # 详情页的链接该站点自己吐出来的，标题太短的多半是导航
        scored: list[tuple[int, SearchItem]] = []
        seen: set[str] = set()
        for match in GENERIC_LINK_RE.finditer(html):
            title = self._clean(match.group("title"))
            if len(title) < 2 or SKIP_TITLE_RE.match(title):
                continue
            href = (match.group("url") or "").strip()
            if not href or href.startswith(("javascript:", "mailto:", "tel:")):
                continue
            url = urljoin(site.base + "/", href)
            parsed = urlparse(url)
            if parsed.netloc.lower() != host:
                continue
            path = parsed.path
            if not path or path == "/":
                continue
            low = path.lower()
            if low.endswith(SKIP_SUFFIX) or any(p in low for p in SKIP_PATH_PARTS):
                continue
            if url in seen:
                continue
            seen.add(url)
            # 带数字路径的通常是内容页，排前面
            score = 1 if re.search(r"\d", path) else 0
            scored.append((score, SearchItem(site=site.key, title=title, url=url)))

        scored.sort(key=lambda pair: -pair[0])
        return [item for _, item in scored]
