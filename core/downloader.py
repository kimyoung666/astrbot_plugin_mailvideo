from __future__ import annotations

import asyncio
import os
import re
import uuid
from pathlib import Path

from astrbot.api import logger

from .config import PluginConfig, SiteConfig
from .searcher import DEFAULT_HEADERS, SearchItem, make_client

YTDLP = "yt-dlp"
FFMPEG = "ffmpeg"


class Downloader:
    """参考 astrbot_plugin_parser 的下载层：优先 yt-dlp，兜底流式/ffmpeg 直链"""

    def __init__(self, cfg: PluginConfig):
        self.cfg = cfg
        self.client = make_client(cfg, timeout=max(cfg.timeout, 60))

    async def close(self):
        await self.client.aclose()

    async def fetch(self, item: SearchItem, site: SiteConfig) -> Path:
        self.cfg.cache_dir.mkdir(parents=True, exist_ok=True)
        mode = (site.downloader or "auto").lower()
        if mode == "direct":
            return await self._direct(item, site)
        if mode == "yt-dlp":
            return await self._ytdlp(item, site)
        # auto：先 yt-dlp，失败了再从详情页扒直链
        try:
            return await self._ytdlp(item, site)
        except Exception as exc:
            logger.warning(f"[mailvideo] yt-dlp 挂了，改扒直链: {exc}")
            return await self._direct(item, site)

    # ---------- yt-dlp ----------

    def _base_cmd(self, site: SiteConfig) -> list[str]:
        cmd = [YTDLP, "--no-playlist", "--no-warnings", "--no-progress"]
        if self.cfg.proxy:
            cmd += ["--proxy", self.cfg.proxy]
        cookie = Path(self.cfg.cookiefile) if self.cfg.cookiefile else None
        if cookie and cookie.is_file():
            cmd += ["--cookies", str(cookie)]
        for k, v in (site.headers or {}).items():
            cmd += ["--add-header", f"{k}: {v}"]
        if self.cfg.max_filesize_mb:
            cmd += ["--max-filesize", f"{self.cfg.max_filesize_mb}M"]
        return cmd

    async def _run(self, cmd: list[str], env: dict | None = None) -> None:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        try:
            _, err = await asyncio.wait_for(
                proc.communicate(), timeout=self.cfg.download_timeout
            )
        except asyncio.TimeoutError:
            proc.kill()
            raise RuntimeError("下载超时")
        if proc.returncode != 0:
            msg = err.decode(errors="ignore").strip()[-300:]
            raise RuntimeError(msg or f"退出码 {proc.returncode}")

    async def _ytdlp(self, item: SearchItem, site: SiteConfig) -> Path:
        uid = uuid.uuid4().hex
        tmpl = str(self.cfg.cache_dir / f"{uid}.%(ext)s")
        cmd = self._base_cmd(site) + [
            "-o",
            tmpl,
            "-f",
            "bv*+ba/b",
            "--merge-output-format",
            "mp4",
            item.url,
        ]
        logger.info(f"[mailvideo] yt-dlp 下载: {item.url}")
        await self._run(cmd)
        files = [p for p in self.cfg.cache_dir.glob(f"{uid}.*") if p.is_file()]
        if not files:
            raise RuntimeError("下完了但没文件，八成被 --max-filesize 拦了")
        return max(files, key=lambda p: p.stat().st_size)

    # ---------- 直链 ----------

    async def _direct(self, item: SearchItem, site: SiteConfig) -> Path:
        resp = await self.client.get(item.url, headers=site.headers or None)
        resp.raise_for_status()
        if site.encoding:
            resp.encoding = site.encoding
        match = re.search(site.video_regex, resp.text)
        if not match:
            raise RuntimeError("详情页里没扒到视频直链，改改 video_regex")
        src = match.group(0).replace("\\/", "/")
        uid = uuid.uuid4().hex
        if ".m3u8" in src or src.endswith(".m3u"):
            return await self._hls(src, uid, site)
        return await self._stream(src, uid, site)

    async def _stream(self, src: str, uid: str, site: SiteConfig) -> Path:
        path = self.cfg.cache_dir / f"{uid}.mp4"
        headers = dict(DEFAULT_HEADERS)
        headers["Referer"] = site.base + "/"
        max_bytes = (self.cfg.max_filesize_mb or 0) * 1024 * 1024
        async with self.client.stream("GET", src, headers=headers) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length") or 0)
            if max_bytes and total > max_bytes:
                raise RuntimeError(f"文件 {total / 1048576:.1f}MB，超过上限")
            with path.open("wb") as f:
                async for chunk in resp.aiter_bytes(1 << 16):
                    f.write(chunk)
        return path

    async def _hls(self, src: str, uid: str, site: SiteConfig) -> Path:
        path = self.cfg.cache_dir / f"{uid}.mp4"
        cmd = [
            FFMPEG,
            "-y",
            "-loglevel",
            "error",
            "-headers",
            f"User-Agent: {DEFAULT_HEADERS['User-Agent']}\r\nReferer: {site.base}/\r\n",
            "-i",
            src,
            "-c",
            "copy",
            "-bsf:a",
            "aac_adtstoasc",
            str(path),
        ]
        env = os.environ.copy()
        if self.cfg.proxy:
            env["http_proxy"] = self.cfg.proxy
            env["https_proxy"] = self.cfg.proxy
        logger.info(f"[mailvideo] ffmpeg 拉流: {src[:80]}")
        await self._run(cmd, env=env)
        if not path.exists() or path.stat().st_size == 0:
            raise RuntimeError("ffmpeg 没产出文件")
        return path
