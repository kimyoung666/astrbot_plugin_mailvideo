"""AstrBot 插件：邮箱取址视频搜索。

流程（指令式，一条命令跑完）：
    1. 登录邮箱（默认 QQ 邮箱），向配置的收件邮箱发送一封邮件；
    2. 轮询收件箱，匹配回信，从回信正文中提取站点地址；
    3. 用该地址作为站点入口，按用户关键词搜索视频（yt-dlp 通用解析）；
    4. 下载视频文件，直接回传给用户。

指令（统一入口 /搜片，按关键词分发）：
    /搜片 <关键词>    主流程：搜索并回传视频
    /搜片 地址        只跑邮箱环节，返回并缓存当前站点地址
    /搜片 邮箱        测试邮箱 IMAP/SMTP 连通性
    /搜片 帮助        用法说明

注意：QQ 官方平台仅在用户发消息后 5 分钟内允许主动消息（且最多 5 条），
长流程（取址+搜索+下载）结束时的回传可能因窗口过期而发送失败（"主动消息失败,
无权限"）。此时结果会自动暂存为「待投递」，用户下次发送任意消息（私聊即任意
消息，群聊需 @ 机器人或调用本插件指令）时自动补发。
"""

from __future__ import annotations

import asyncio
import importlib.util
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Optional

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import MessageChain, filter, AstrMessageEvent
from astrbot.api.event.filter import EventMessageType
from astrbot.api.star import Context, Star, register
import astrbot.api.message_components as Comp

# --------------------------------------------------------------------------- #
# 加载内部模块
# --------------------------------------------------------------------------- #
# 逐个按文件路径加载，使用唯一模块名，避免与其它插件的同名包冲突。
_PLUGIN_DIR = Path(__file__).resolve().parent


def _load_core_module(name: str):
    full_name = f"astrbot_plugin_mailvideo_core.{name}"
    path = _PLUGIN_DIR / "core" / f"{name}.py"

    # 源文件版本戳：文件改动后即使模块名不匹配 AstrBot 的清理规则，
    # 也能在这里自动重新加载，避免「main 是新的、core 还是旧的」这种错配。
    try:
        stamp = path.stat().st_mtime_ns
    except OSError:
        stamp = None

    cached = sys.modules.get(full_name)
    if cached is not None and getattr(cached, "__mf_source_stamp__", None) == stamp:
        return cached
    if cached is not None:
        logger.info(f"[mailvideo] 检测到 {name}.py 已更新，重新加载核心模块")

    spec = importlib.util.spec_from_file_location(full_name, path)
    if spec is None or spec.loader is None:  # pragma: no cover
        raise ImportError(f"无法加载模块: {path}")
    module = importlib.util.module_from_spec(spec)
    module.__mf_source_stamp__ = stamp
    sys.modules[full_name] = module
    spec.loader.exec_module(module)
    return module


_state_store = _load_core_module("state_store")
_mail_client = _load_core_module("mail_client")
_address_extractor = _load_core_module("address_extractor")
_video_fetcher = _load_core_module("video_fetcher")

StateStore = _state_store.StateStore
resolve_data_dir = _state_store.resolve_data_dir
MailClient = _mail_client.MailClient
MailError = _mail_client.MailError
extract_address = _address_extractor.extract_address
VideoFetcher = _video_fetcher.VideoFetcher
VideoFetchError = _video_fetcher.VideoFetchError

PLUGIN_NAME = "astrbot_plugin_mailvideo"


@register(PLUGIN_NAME, "WorkBuddy", "邮箱取址视频搜索", "1.1.0",
          "https://github.com/kimyoung666/astrbot_plugin_mailvideo")
class MailVideoPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config: AstrBotConfig = config or {}  # type: ignore[assignment]
        self.data_dir = resolve_data_dir(PLUGIN_NAME)
        self.store = StateStore(self.data_dir)
        self._locks: dict[str, asyncio.Lock] = {}
        self._tasks: set[asyncio.Task] = set()
        self._gc_pending_files(self.data_dir / "pending")

    # ================================================================== #
    # 配置读取
    # ================================================================== #
    def _cfg(self, key: str, default=None):
        try:
            return self.config.get(key, default)
        except Exception:
            return default

    def _build_mail_client(self) -> MailClient:
        return MailClient(
            account=str(self._cfg("email_account", "")).strip(),
            auth_code=str(self._cfg("email_auth_code", "")).strip(),
            imap_server=str(self._cfg("imap_server", "imap.qq.com")).strip() or "imap.qq.com",
            imap_port=int(self._cfg("imap_port", 993) or 993),
            smtp_server=str(self._cfg("smtp_server", "smtp.qq.com")).strip() or "smtp.qq.com",
            smtp_port=int(self._cfg("smtp_port", 465) or 465),
            recipient=str(self._cfg("recipient_email", "")).strip(),
        )

    def _build_fetcher(self) -> VideoFetcher:
        return VideoFetcher(
            search_url_template=str(
                self._cfg("search_url_template", "{base}/search?wd={keyword}")
            ).strip()
            or "{base}/search?wd={keyword}",
            max_results=int(self._cfg("max_results", 1) or 1),
            cookie=str(self._cfg("cookie", "") or ""),
            proxy=str(self._cfg("http_proxy", "") or ""),
            ffmpeg_path=str(self._cfg("ffmpeg_path", "") or ""),
            hls_parallel_workers=int(self._cfg("hls_parallel_workers", 16) or 16),
        )

    def _allowed(self, event: AstrMessageEvent) -> bool:
        allow = self._cfg("allowed_users", []) or []
        if not allow:
            return True
        return str(event.get_sender_id()) in {str(x) for x in allow}

    # ================================================================== #
    # 指令
    # ================================================================== #
    @filter.event_message_type(EventMessageType.PRIVATE_MESSAGE)
    async def on_private_message(self, event: AstrMessageEvent):
        """私聊钩子：用户发消息时，优先补发该会话暂存的「待投递」结果
        （如长流程结束后因 QQ 官方主动消息窗口过期而没发出去的视频）。"""
        await self._deliver_pending_if_any(event)

    @filter.command("搜片")
    async def cmd_video(self, event: AstrMessageEvent):
        """统一入口：/搜片 <关键词> 搜索视频；/搜片 地址|邮箱|帮助 为子指令。"""
        if not self._allowed(event):
            yield event.plain_result("你没有权限使用该指令。")
            return
        await self._deliver_pending_if_any(event)
        keyword = self._parse_keyword(event.message_str)
        kw = (keyword or "").strip().lower()

        # ── 子指令：/搜片 地址 ──
        if kw in ("地址", "取地址"):
            yield event.plain_result("\u200b📮 正在获取站点地址…")
            umo = event.unified_msg_origin
            task = asyncio.create_task(self._address_only(umo))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            return

        # ── 子指令：/搜片 邮箱 ──
        if kw in ("邮箱", "邮箱测试", "测试邮箱"):
            try:
                client = self._build_mail_client()
                info = await client.test_connection()
                yield event.plain_result(f"✅ 邮箱连接正常。\n{info}")
            except Exception as e:
                logger.error(f"[mailvideo] 邮箱测试失败: {e}")
                yield event.plain_result(f"❌ 邮箱连接失败：{e}")
            return

        # ── 子指令：/搜片 帮助 ──
        if kw in ("帮助", "help", "用法"):
            fixed = str(self._cfg("site_address", "") or "").strip()
            cached = fixed or self.store.get_last_address() or "（暂无）"
            yield event.plain_result(
                "邮箱取址视频搜索 · 用法\n"
                "/搜片 <关键词>   搜索并回传视频\n"
                "/搜片 地址        获取并缓存站点地址\n"
                "/搜片 邮箱        测试邮箱连接\n"
                "/搜片 帮助        显示本帮助\n"
                f"\n当前缓存站点地址：{cached}\n"
                "首次使用请先在插件配置中填写邮箱账号、授权码与收件邮箱。"
            )
            return

        # ── 主流程：/搜片 <关键词> ──
        if not keyword:
            yield event.plain_result(
                "用法：/搜片 <关键词>\n例如：/搜片 鞠婧祎\n\n"
                "子指令：\n/搜片 地址   获取站点地址\n"
                "/搜片 邮箱   测试邮箱连接\n/搜片 帮助   显示帮助"
            )
            return

        umo = event.unified_msg_origin
        # 同一会话下避免并发下载
        lock = self._locks.get(umo)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[umo] = lock
        if lock.locked():
            yield event.plain_result("上一个任务还在处理中，请稍候再试。")
            return

        # 先秒回，重活丢到后台任务，避免平台请求超时
        yield event.plain_result(
            f"\u200b🔍 收到，开始处理「{keyword}」\n① 正在获取站点地址…"
        )
        task = asyncio.create_task(self._run_pipeline(umo, keyword, lock))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # ================================================================== #
    # 流程实现
    # ================================================================== #
    async def _address_only(self, umo: str):
        try:
            address, _ = await self._fetch_address(umo)
            if address:
                await self._send_text(umo, f"✅ 当前站点地址：\n{address}")
            else:
                await self._send_text(umo, "❌ 未能从回信中提取到地址，请检查配置。")
        except Exception as e:
            logger.error(f"[mailvideo] 取地址失败: {e}")
            await self._send_text(umo, f"❌ 取地址失败：{e}")

    async def _run_pipeline(self, umo: str, keyword: str, lock: asyncio.Lock):
        async with lock:
            tmp_dir = self.store.temp_dir / f"job_{int(time.time())}"
            try:
                # ---- ① 取地址 ----
                address, _ = await self._fetch_address(umo)
                if not address:
                    await self._send_text(umo, "❌ 未能获取站点地址，流程终止。", critical=True)
                    return

                # ---- ② 搜索 ----
                await self._send_text(umo, f"② 站点地址：{address}\n③ 正在搜索「{keyword}」…")
                fetcher = self._build_fetcher()
                candidates = await asyncio.to_thread(fetcher.search, address, keyword)
                if not candidates:
                    await self._send_text(umo, "❌ 未搜索到相关视频。", critical=True)
                    return

                if not fetcher.ffmpeg_available():
                    await self._send_text(
                        umo, "⚠️ 未检测到 ffmpeg，m3u8 合并/高清合流可能失败，将尽量尝试。"
                    )

                # ---- ③ 下载 ----
                max_results = max(1, int(self._cfg("max_results", 1) or 1))
                max_size_mb = max(1, int(self._cfg("max_video_size_mb", 100) or 100))
                # 最多尝试的候选数：回传数 + 少量容错，避免无脑刷 6 个
                max_attempts = max(1, int(self._cfg("max_download_attempts", 0) or 0))
                if max_attempts <= 0:
                    max_attempts = max_results + 1
                got = 0
                attempts = 0
                errors: list[str] = []
                for cand in candidates:
                    if got >= max_results or attempts >= max_attempts:
                        break
                    attempts += 1
                    url = cand["url"]
                    logger.info(
                        f"[mailvideo] —— 开始第 {attempts}/{max_attempts} 次下载 —— "
                        f"标题: {cand.get('title')} | 页面: {url}"
                    )
                    await self._send_text(
                        umo,
                        f"④ 正在下载（{attempts}/{max_attempts}）：{cand.get('title') or url}",
                    )
                    _dl_started = time.time()
                    try:
                        result = await asyncio.to_thread(
                            fetcher.download,
                            url,
                            tmp_dir,
                            max_size_mb,
                            cand.get("title") or "",
                        )
                    except Exception as e:
                        errors.append(str(e))
                        logger.warning(
                            f"[mailvideo] —— 下载失败（耗时 {time.time() - _dl_started:.1f}s）"
                            f"{url}: {e}"
                        )
                        continue

                    path = Path(result["path"])
                    size_mb = path.stat().st_size / 1024 / 1024
                    logger.info(
                        f"[mailvideo] —— 下载成功 —— {path.name} | {size_mb:.2f}MB | "
                        f"耗时 {time.time() - _dl_started:.1f}s"
                    )

                    # ---- ⓸b 画质压缩（可选）----
                    path, size_mb = await self._maybe_compress(
                        umo, fetcher, path, size_mb, tmp_dir
                    )

                    if size_mb > max_size_mb:
                        await self._send_text(
                            umo,
                            f"⚠️ 文件过大（{size_mb:.1f}MB > {max_size_mb}MB），"
                            f"改为返回链接：\n{result.get('webpage_url') or url}",
                            critical=True,
                        )
                        got += 1
                        continue
                    ok = await self._send_video(umo, path)
                    got += 1
                    if ok:
                        logger.info(f"[mailvideo] 已回传第 {got} 个视频: {path.name}")
                    else:
                        logger.info(f"[mailvideo] 第 {got} 个视频已转为待投递: {path.name}")

                if got == 0:
                    detail = ("；".join(errors[-2:])) if errors else "无可用结果"
                    await self._send_text(umo, f"❌ 下载失败：{detail}", critical=True)
                else:
                    logger.info(
                        f"[mailvideo] 任务结束 | 成功回传 {got} 个 | 尝试 {attempts} 次"
                        + (f" | 失败 {len(errors)} 次" if errors else "")
                    )
            except Exception as e:
                logger.error(f"[mailvideo] 处理失败: {e}", exc_info=True)
                await self._send_text(umo, f"❌ 处理失败：{e}")
            finally:
                self._cleanup(tmp_dir)

    async def _fetch_address(self, umo: str) -> tuple[Optional[str], str]:
        """执行「发信 → 等回信 → 提取地址」，返回 (地址, 回信正文)。

        若配置了 ``site_address``（固定站点地址），则直接使用该地址，
        完全跳过邮件流程（不发信、不等回信），也不依赖缓存。
        """
        # ── 固定站点地址优先：填了就直连使用 ──
        fixed = str(self._cfg("site_address", "") or "").strip()
        if fixed:
            if not fixed.startswith(("http://", "https://")):
                fixed = "https://" + fixed
            self.store.set_last_address(fixed)  # 同步进缓存，兜底其它逻辑
            logger.info(f"[mailvideo] 使用固定站点地址: {fixed}")
            return fixed, ""

        client = self._build_mail_client()
        client.validate()

        subject = str(self._cfg("mail_subject", "获取地址") or "获取地址")
        body = str(self._cfg("mail_body", "获取最新地址") or "获取最新地址")
        timeout = int(self._cfg("reply_wait_timeout", 120) or 120)
        interval = int(self._cfg("reply_poll_interval", 5) or 5)

        message_id, sent_at = await client.send_mail(subject, body)
        logger.info(f"[mailvideo] 已发信 Message-ID={message_id} 至 {client.recipient}")

        await self._send_text(
            umo, f"· 邮件已发送至 {client.recipient}，等待回信（最长 {timeout}s）…"
        )

        reply = await client.wait_for_reply(
            after_ts=int(sent_at.timestamp()),
            message_id=message_id,
            original_subject=subject,
            timeout=timeout,
            interval=interval,
            exclude_uids=set(self.store.get_processed_uids()),
        )

        text = reply.get("text", "") if reply else ""
        if reply:
            self.store.add_processed_uids([reply["uid"]])

        custom_regex = str(self._cfg("address_regex", "") or "").strip() or None
        address = extract_address(text, custom_regex) if text else None

        if not address and self._cfg("use_cache_on_fail", True):
            cached = self.store.get_last_address()
            if cached:
                logger.info(f"[mailvideo] 提取失败，回退缓存地址: {cached}")
                return cached, text

        if address:
            self.store.set_last_address(address)
        return address, text

    # ================================================================== #
    # 发送辅助
    # ================================================================== #
    async def _send_text(self, umo: str, text: str, critical: bool = False) -> bool:
        """发送文本。critical=True 的结果若发送失败，会暂存为「待投递」，
        等用户下次发消息时自动补发（应对 QQ 官方主动消息窗口过期）。"""
        try:
            await self.context.send_message(umo, MessageChain([Comp.Plain("\u200b" + text)]))
            return True
        except Exception as e:
            logger.error(f"[mailvideo] 发送文本失败: {e}")
            if critical:
                self._stash_pending(umo, {"type": "text", "text": text})
            return False

    async def _send_video(self, umo: str, path: Path) -> bool:
        """发送视频文件，成功返回 True。

        QQ 官方平台仅允许在用户发消息后 5 分钟内主动消息（最多 5 条），
        长流程结束时窗口可能已过期：先重试几次（用户这期间可能发新消息
        刷新窗口），仍失败则暂存为「待投递」，等用户下次发消息时补发。
        """
        send_as = str(self._cfg("send_as", "video") or "video").lower()
        for attempt in range(1, 4):
            try:
                if send_as == "file":
                    chain = MessageChain([Comp.File(file=str(path), name=path.name)])
                else:
                    chain = MessageChain([Comp.Video.fromFileSystem(path=str(path))])
                await self.context.send_message(umo, chain)
                return True
            except Exception as e:
                logger.warning(f"[mailvideo] 发送视频失败（第 {attempt}/3 次）: {e}")
                if attempt < 3:
                    await asyncio.sleep(20)
        # 重试全部失败：暂存待投递，等用户下次发消息时补发
        self._stash_pending(umo, {"type": "video", "path": str(path), "name": path.name})
        return False

    # ================================================================== #
    # 待投递（发送失败兜底，应对平台主动消息限制）
    # ================================================================== #
    PENDING_MAX_AGE = 24 * 3600  # 待投递条目 24 小时后过期
    PENDING_MAX_FILES = 10  # 待投递目录最多保留的文件数

    def _stash_pending(self, umo: str, item: dict) -> None:
        """把发送失败的结果存为「待投递」，用户下次发消息时补发。

        视频类结果会把文件从 tmp 移入持久化目录，避免被 _cleanup 删掉。
        """
        try:
            if item.get("type") == "video":
                src = Path(item.get("path", ""))
                if src.exists():
                    pending_dir = self.data_dir / "pending"
                    pending_dir.mkdir(parents=True, exist_ok=True)
                    dest = pending_dir / f"{int(time.time())}_{src.name}"
                    shutil.move(str(src), str(dest))
                    item["path"] = str(dest)
                    self._gc_pending_files(pending_dir)
            self.store.set_pending(umo, item)
            logger.warning(
                f"[mailvideo] 已暂存待投递: 会话={umo} 类型={item.get('type')} "
                f"（用户下次发送消息时自动补发）"
            )
        except Exception as e:
            logger.error(f"[mailvideo] 暂存待投递失败: {e}")

    def _gc_pending_files(self, pending_dir: Path) -> None:
        """清理待投递目录：过期文件删除，最多保留最近 N 个。"""
        try:
            files = sorted(
                pending_dir.glob("*"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            now = time.time()
            for p in files:
                if not p.is_file():
                    continue
                expired = now - p.stat().st_mtime > self.PENDING_MAX_AGE
                overflow = files.index(p) >= self.PENDING_MAX_FILES
                if expired or overflow:
                    p.unlink(missing_ok=True)
        except Exception as e:  # pragma: no cover
            logger.debug(f"[mailvideo] 清理待投递文件失败: {e}")

    def _discard_pending_file(self, item: dict) -> None:
        try:
            if item.get("type") == "video":
                Path(item.get("path", "")).unlink(missing_ok=True)
        except Exception:  # pragma: no cover
            pass

    async def _deliver_pending(self, event: AstrMessageEvent, item: dict) -> None:
        """把待投递结果发到会话；仍失败则重新暂存，等下一次消息再试。"""
        umo = event.unified_msg_origin
        try:
            if item.get("type") == "video":
                path = Path(item.get("path", ""))
                if not path.exists():
                    logger.warning(f"[mailvideo] 待投递文件不存在: {path}")
                    return
                send_as = str(self._cfg("send_as", "video") or "video").lower()
                if send_as == "file":
                    chain = MessageChain([Comp.File(file=str(path), name=path.name)])
                else:
                    chain = MessageChain([Comp.Video.fromFileSystem(path=str(path))])
                await self.context.send_message(umo, chain)
                path.unlink(missing_ok=True)
                logger.info(f"[mailvideo] 待投递视频补发成功: {path.name}")
            elif item.get("type") == "text":
                await self._send_text(umo, "\u200b📬 补发之前的结果：\n" + str(item.get('text', '')))
            else:
                self._discard_pending_file(item)
        except Exception as e:
            logger.error(f"[mailvideo] 待投递补发失败: {e}，等用户下次消息再试")
            self.store.set_pending(umo, item)

    async def _deliver_pending_if_any(self, event: AstrMessageEvent) -> None:
        """若会话有待投递结果（如窗口过期没发出去的视频），立即补发。"""
        try:
            item = self.store.pop_pending(event.unified_msg_origin)
            if not item:
                return
            if time.time() - int(item.get("ts", 0) or 0) > self.PENDING_MAX_AGE:
                self._discard_pending_file(item)
                return
            await self._deliver_pending(event, item)
        except Exception as e:
            logger.error(f"[mailvideo] 待投递处理异常: {e}")

    async def _maybe_compress(
        self, umo: str, fetcher, path: Path, size_mb: float, tmp_dir: Path
    ) -> tuple[Path, float]:
        """按配置对下载好的视频做画质压缩，返回 (可能被替换的路径, 新大小MB)。

        压缩失败不影响主流程——沿用原文件继续走后续逻辑。
        """
        if not bool(self._cfg("compress_enable", False)):
            return path, size_mb

        threshold = int(self._cfg("compress_threshold_mb", 0) or 0)
        if threshold > 0 and size_mb <= threshold:
            logger.info(
                f"[mailvideo] {size_mb:.1f}MB 未超过压缩阈值 {threshold}MB，跳过压缩"
            )
            return path, size_mb

        target_h = int(self._cfg("compress_target_height", 720) or 0)
        crf = int(self._cfg("compress_crf", 30) or 30)
        preset = str(self._cfg("compress_preset", "veryfast") or "veryfast").strip()
        v_br = str(self._cfg("compress_target_bitrate", "") or "").strip()
        a_br = str(self._cfg("compress_audio_bitrate", "64k") or "64k").strip()

        await self._send_text(
            umo,
            f"🗜️ 正在压缩画质（原始 {size_mb:.1f}MB"
            + (f" → {target_h}p" if target_h else "")
            + "）…",
        )
        _c_started = time.time()
        try:
            res = await asyncio.to_thread(
                fetcher.compress_video,
                path, tmp_dir, target_h, crf, preset, v_br, a_br,
            )
        except Exception as e:
            logger.warning(f"[mailvideo] —— 压缩失败，改用原文件: {e}")
            await self._send_text(umo, f"⚠️ 压缩失败，改用原文件：{e}")
            return path, size_mb

        new_path = Path(res["path"])
        if not new_path.exists() or new_path.stat().st_size == 0:
            logger.warning("[mailvideo] 压缩产物无效，改用原文件")
            return path, size_mb

        new_mb = new_path.stat().st_size / 1024 / 1024
        saved = (1 - new_mb / size_mb) * 100 if size_mb else 0
        logger.info(
            f"[mailvideo] —— 压缩成功 —— {size_mb:.2f}MB → {new_mb:.2f}MB"
            f"（省 {saved:.0f}%）| 耗时 {time.time() - _c_started:.1f}s"
        )
        return new_path, new_mb

    def _cleanup(self, tmp_dir: Path):
        """清理本次任务的临时文件（等待发送完成后调用）。"""
        try:
            if Path(tmp_dir).exists():
                for p in Path(tmp_dir).rglob("*"):
                    if p.is_file():
                        try:
                            p.unlink()
                        except OSError:
                            pass
                try:
                    for d in sorted(Path(tmp_dir).rglob("*"), reverse=True):
                        if d.is_dir():
                            d.rmdir()
                    Path(tmp_dir).rmdir()
                except OSError:
                    pass
        except Exception as e:  # pragma: no cover
            logger.debug(f"[mailvideo] 清理临时文件失败: {e}")

    # ================================================================== #
    @staticmethod
    def _parse_keyword(raw: str) -> str:
        """从消息原文中去掉指令前缀，取出关键词。

        兼容 "/搜片 关键词"、"!搜片 关键词"、"搜片 关键词" 等写法；
        子指令（地址/邮箱/帮助）也在 cmd_video 内按关键词分发。
        """
        raw = (raw or "").strip()
        m = re.search(r"[/!#]?\s*搜片\s+(.+)$", raw, re.S)
        if m:
            return m.group(1).strip()
        parts = raw.split(maxsplit=1)
        return parts[1].strip() if len(parts) > 1 else ""

    async def terminate(self):
        """插件卸载/停用时取消后台任务。"""
        for t in list(self._tasks):
            t.cancel()
        self._tasks.clear()
        logger.info("[mailvideo] 插件已卸载。")
