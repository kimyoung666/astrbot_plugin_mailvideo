from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote, urlparse

from astrbot.api import logger

# 老版本 JSON 配置的兜底示例（仅在 site_url 为空、且 sites 不是合法 JSON 时用到）
DEFAULT_SITES: dict = {}

# 只有站点地址时，按顺序去试这些搜索入口，谁先出结果就用谁
SEARCH_PATTERNS: tuple[str, ...] = (
    "/search/{kw}",
    "/search?q={kw}",
    "/search?keyword={kw}",
    "/search?search={kw}",
    "/search?wd={kw}",
    "/?s={kw}",
    "/?search={kw}",
    "/?keyword={kw}",
    "/index.php/vod/search.html?wd={kw}",
    "/index.php/vod/search.html?searchword={kw}",
    "/vodsearch/{kw}----------.html",
    "/vodsearch/-------------.html?wd={kw}",
    "/search/{kw}/",
    "/s/{kw}",
    "/so/{kw}",
    "/search.php?search={kw}",
    "/search.php?keyword={kw}",
    "/video/search?search={kw}",
)


HOST_RE = re.compile(r"^[a-z0-9]([a-z0-9\-\.]*[a-z0-9])?$")


def normalize_base(url: str) -> str:
    """把用户填的东西收拾成 https://host 形式，填得不像链接就返回空"""
    url = (url or "").strip()
    if not url:
        return ""
    if not url.lower().startswith(("http://", "https://")):
        url = "https://" + url.lstrip("/")
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError:
        return ""
    if not host or not HOST_RE.match(host):
        return ""
    if "." not in host and host != "localhost":
        return ""
    netloc = host if port is None else f"{host}:{port}"
    base = f"{parsed.scheme}://{netloc}"
    path = parsed.path.rstrip("/")
    if path:
        base += path
    return base


def build_search_urls(base: str) -> list[str]:
    urls: list[str] = []
    for pat in SEARCH_PATTERNS:
        url = base + pat
        if url not in urls:
            urls.append(url)
    # 地址本身就带路径的（比如 /xxx/），再按站内根目录试一遍
    parsed = urlparse(base)
    if parsed.path and parsed.path != "/":
        root = f"{parsed.scheme}://{parsed.netloc}"
        for pat in SEARCH_PATTERNS:
            url = root + pat
            if url not in urls:
                urls.append(url)
    return urls


@dataclass
class SiteConfig:
    key: str
    name: str
    base: str
    search_url: str = ""
    search_urls: list[str] = field(default_factory=list)
    method: str = "GET"
    headers: dict = field(default_factory=dict)
    encoding: str | None = None
    # html 模式
    list_selector: str = ""
    item_link: str = "a[href]"
    item_title: str = ""
    item_cover: str = ""
    item_duration: str = ""
    # regex 模式（list_selector 为空时生效）
    link_regex: str = ""
    title_regex: str = ""
    # 下载
    downloader: str = "auto"  # auto | yt-dlp | direct
    video_regex: str = r"https?://[^\"'\s]+?\.(?:m3u8|mp4)[^\"'\s]*"

    def candidates(self) -> list[str]:
        """待探测的搜索入口，已确认过的排最前"""
        urls: list[str] = []
        if self.search_url:
            urls.append(self.search_url)
        for url in self.search_urls:
            if url not in urls:
                urls.append(url)
        return urls

    def render(self, url: str, keyword: str) -> str:
        return url.replace("{kw}", quote(keyword)).replace("{base}", self.base)

    @staticmethod
    def from_url(url: str) -> "SiteConfig | None":
        base = normalize_base(url)
        if not base:
            return None
        host = urlparse(base).netloc.lower()
        key = host.replace("www.", "").split(":")[0]
        return SiteConfig(
            key=key,
            name=key,
            base=base,
            search_url="",
            search_urls=build_search_urls(base),
            downloader="auto",
        )

    @staticmethod
    def parse(key: str, raw: dict) -> "SiteConfig":
        return SiteConfig(
            key=key,
            name=raw.get("name") or key,
            base=normalize_base(raw.get("base") or ""),
            search_url=raw.get("search_url") or "",
            search_urls=list(raw.get("search_urls") or []),
            method=(raw.get("method") or "GET").upper(),
            headers=raw.get("headers") or {},
            encoding=raw.get("encoding"),
            list_selector=raw.get("list_selector") or "",
            item_link=raw.get("item_link") or "a[href]",
            item_title=raw.get("item_title") or "",
            item_cover=raw.get("item_cover") or "",
            item_duration=raw.get("item_duration") or "",
            link_regex=raw.get("link_regex") or "",
            title_regex=raw.get("title_regex") or "",
            downloader=(raw.get("downloader") or "auto").lower(),
            video_regex=raw.get("video_regex")
            or r"https?://[^\"'\s]+?\.(?:m3u8|mp4)[^\"'\s]*",
        )


class PluginConfig:
    def __init__(self, config: dict, context=None):
        cfg = dict(config or {})
        self.raw = cfg
        self.proxy: str | None = (cfg.get("proxy") or "").strip() or None
        self.max_results: int = max(1, int(cfg.get("max_results") or 10))
        self.max_filesize_mb: int = int(cfg.get("max_filesize_mb") or 0)
        self.max_send_mb: int = int(cfg.get("max_send_mb") or 80)
        self.send_as_file: bool = bool(cfg.get("send_as_file"))
        self.cookiefile: str = (cfg.get("cookiefile") or "").strip()
        self.timeout: int = int(cfg.get("timeout") or 20)
        self.download_timeout: int = int(cfg.get("download_timeout") or 600)
        self.pending_ttl: int = int(cfg.get("pending_ttl") or 600)
        self.site_url: str = (cfg.get("site_url") or "").strip()
        self.sites: dict[str, SiteConfig] = self._load_sites(cfg)
        self.default_site: str = (cfg.get("default_site") or "").strip() or (
            next(iter(self.sites), "")
        )
        self.cache_dir = self._data_dir() / "cache"

    @staticmethod
    def _data_dir() -> Path:
        """用户数据目录：data/plugin_data/astrbot_plugin_mailvideo/"""
        try:
            from astrbot.core.utils.astrbot_path import (
                get_astrbot_plugin_data_path,
            )

            return Path(get_astrbot_plugin_data_path()) / "astrbot_plugin_mailvideo"
        except Exception:
            pass
        try:
            from astrbot.api.star import StarTools

            return Path(StarTools.get_data_dir("astrbot_plugin_mailvideo"))
        except Exception:
            return Path("data/plugin_data/astrbot_plugin_mailvideo")

    def _load_sites(self, cfg: dict) -> dict[str, SiteConfig]:
        # 1) 首选：站点地址输入框
        site = SiteConfig.from_url(self.site_url) if self.site_url else None
        if site is not None:
            return {site.key: site}

        # 2) 兼容老配置里的 sites JSON
        raw = cfg.get("sites")
        if isinstance(raw, str):
            raw = raw.strip()
            if not raw:
                raw = {}
            else:
                try:
                    raw = json.loads(raw)
                except json.JSONDecodeError as exc:
                    logger.error(f"[mailvideo] sites 配置不是合法 JSON: {exc}")
                    raw = {}
        if not isinstance(raw, dict) or not raw:
            raw = DEFAULT_SITES

        sites: dict[str, SiteConfig] = {}
        for key, item in raw.items():
            if not isinstance(item, dict):
                continue
            site = SiteConfig.parse(key, item)
            if not site.base:
                continue
            if not site.search_urls and not site.search_url:
                site.search_urls = build_search_urls(site.base)
            sites[key] = site
        return sites

    def get_site(self, key: str) -> SiteConfig | None:
        if key in self.sites:
            return self.sites[key]
        low = key.lower()
        for site in self.sites.values():
            if site.key.lower() == low or site.name.lower() == low:
                return site
        return None
