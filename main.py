from __future__ import annotations

import time

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
from astrbot.core import AstrBotConfig
from astrbot.core.message.components import File, Video

from .core.config import PluginConfig
from .core.downloader import Downloader
from .core.searcher import Searcher

SEARCH_NAMES = {"搜片", "求片", "vs"}
DOWNLOAD_NAMES = {"下", "下片", "下载", "vd"}


@register(
    "astrbot_plugin_mailvideo",
    "local",
    "固定站点搜索 + 下载：/搜片 关键词",
    "1.4.5",
)
class MailVideoPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.cfg = PluginConfig(config or {}, context)
        self.searcher = Searcher(self.cfg)
        self.downloader = Downloader(self.cfg)
        # key: umo + sender -> 最近一次搜索结果
        self.pending: dict[str, dict] = {}
        self._clean_cache()

    async def terminate(self):
        await self.searcher.close()
        await self.downloader.close()

    # ---------- 工具 ----------

    def _clean_cache(self):
        """清掉一天前的缓存文件，免得撑爆盘"""
        try:
            self.cfg.cache_dir.mkdir(parents=True, exist_ok=True)
            deadline = time.time() - 86400
            for p in self.cfg.cache_dir.iterdir():
                if p.is_file() and p.stat().st_mtime < deadline:
                    p.unlink(missing_ok=True)
        except Exception as exc:
            logger.warning(f"[mailvideo] 清理缓存失败: {exc}")

    @staticmethod
    def _key(event: AstrMessageEvent) -> str:
        return f"{event.unified_msg_origin}:{event.get_sender_id()}"

    @staticmethod
    def _strip(event: AstrMessageEvent, names: set[str]) -> str:
        text = (event.message_str or "").strip().lstrip("/").strip()
        for name in sorted(names, key=len, reverse=True):
            if text.startswith(name):
                return text[len(name) :].strip()
        return text

    def _target_site(self):
        site = self.cfg.get_site(self.cfg.default_site) if self.cfg.default_site else None
        if site is None:
            site = next(iter(self.cfg.sites.values()), None)
        return site

    def _get_pending(self, event: AstrMessageEvent) -> dict | None:
        key = self._key(event)
        rec = self.pending.get(key)
        if not rec:
            return None
        if time.time() - rec["ts"] > self.cfg.pending_ttl:
            self.pending.pop(key, None)
            return None
        return rec

    @staticmethod
    def _render(items, site_name: str) -> str:
        lines = [f"{site_name}｜{len(items)} 条"]
        for i, it in enumerate(items, 1):
            title = it.title if len(it.title) <= 40 else it.title[:40] + "..."
            dur = f" [{it.duration}]" if it.duration else ""
            lines.append(f"{i}. {title}{dur}")
        lines.append("回数字直接下，或者 /下 序号")
        return "\n".join(lines)

    async def _deliver(self, event: AstrMessageEvent, rec: dict, idx: int):
        """取第 idx 条下载并发送"""
        items = rec["items"]
        if not 1 <= idx <= len(items):
            yield event.plain_result(f"序号只有 1-{len(items)}")
            return

        item = items[idx - 1]
        site = self.cfg.get_site(item.site) or self._target_site()
        if site is None:
            yield event.plain_result("站点配置没了")
            return

        yield event.plain_result(f"开下：{item.title}")
        try:
            path = await self.downloader.fetch(item, site)
        except Exception as exc:
            logger.error(f"[mailvideo] 下载失败: {exc}")
            yield event.plain_result(f"下不了：{exc}")
            return

        size_mb = path.stat().st_size / 1024 / 1024
        logger.info(f"[mailvideo] 下载完成 {path} ({size_mb:.1f}MB)")
        safe_name = "".join(c for c in item.title[:30] if c not in '\\/:*?"<>|').strip()
        safe_name = safe_name or path.stem

        if self.cfg.send_as_file or size_mb > self.cfg.max_send_mb:
            yield event.chain_result(
                [File(name=f"{safe_name}{path.suffix}", file=str(path))]
            )
        else:
            yield event.chain_result([Video.fromFileSystem(str(path))])

    # ---------- 命令 ----------

    @filter.command("搜片", alias={"求片", "vs"}, desc="在固定站点搜索视频")
    async def cmd_search(self, event: AstrMessageEvent):
        keyword = self._strip(event, SEARCH_NAMES)
        if not keyword:
            yield event.plain_result("用法：/搜片 关键词")
            return

        site = self._target_site()
        if site is None:
            yield event.plain_result("没配站点，先去插件配置里把站点地址填上")
            return

        try:
            items = await self.searcher.search(site, keyword)
        except Exception as exc:
            logger.error(f"[mailvideo] 搜索失败 {site.key}: {exc}")
            yield event.plain_result(f"搜索挂了：{exc}")
            return

        if not items:
            yield event.plain_result(f"{site.name} 没搜到「{keyword}」")
            return

        now = time.time()
        for k in [k for k, v in self.pending.items() if now - v["ts"] > self.cfg.pending_ttl]:
            self.pending.pop(k, None)
        self.pending[self._key(event)] = {
            "ts": now,
            "site": site.key,
            "items": items,
        }
        yield event.plain_result(self._render(items, site.name))

    @filter.command("下", alias={"下片", "下载", "vd"}, desc="下载上次搜到的第 N 条")
    async def cmd_download(self, event: AstrMessageEvent):
        args = self._strip(event, DOWNLOAD_NAMES)
        rec = self._get_pending(event)
        if not rec:
            yield event.plain_result("先 /搜片 再说话")
            return
        if not args.isdigit():
            yield event.plain_result("给个序号，比如 /下 1")
            return
        async for res in self._deliver(event, rec, int(args)):
            yield res

    @filter.regex(r"^\s*\d{1,2}\s*$")
    async def cmd_pick(self, event: AstrMessageEvent):
        """搜完之后直接发数字就下，不用打命令"""
        rec = self._get_pending(event)
        if not rec:
            return
        text = (event.message_str or "").strip().lstrip("/").strip()
        if not text.isdigit():
            return
        async for res in self._deliver(event, rec, int(text)):
            yield res
