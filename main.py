"""AstrBot 插件：邮箱取址视频搜索。

流程（指令式，一条命令跑完）：
    1. 登录邮箱（默认 QQ 邮箱），向配置的收件邮箱发送一封邮件；
    2. 轮询收件箱，匹配回信，从回信正文中提取站点地址；
    3. 用该地址作为站点入口，按用户关键词搜索视频（yt-dlp 通用解析）；
    4. 下载视频文件，直接回传给用户。

回传方式（固定为候选清单模式）：
    搜索完成后先列出候选清单，用户回复序号后才下载并回传那一个；
    只有用户明确选了哪一条，才会去下载，避免下错片子白等。

指令（统一入口 /搜片，按关键词分发）：
    /搜片 <关键词>    主流程：搜索并列出候选，回复序号后回传视频
    /搜片 地址        只跑邮箱环节，返回并缓存当前站点地址
    /搜片 邮箱        测试邮箱 IMAP/SMTP 连通性
    /搜片 取消        放弃当前待选的候选清单
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

# 候选选择：用户回复的文本 → 序号（兼容 "3" / "选3" / "#3" / "第3个" / "3号"）
_SELECT_RE = re.compile(r"^(?:选|选择|第)?[#＃]?(\d{1,2})(?:号|个|条)?$")
_CANCEL_WORDS = {"取消", "算了", "放弃", "不要了", "不用了", "退出", "cancel", "q"}


@register(PLUGIN_NAME, "WorkBuddy", "邮箱取址视频搜索", "1.4.2",
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
            hls_parallel_workers=int(self._cfg("hls_parallel_workers", 24) or 24),
            direct_parallel_workers=int(self._cfg("direct_parallel_workers", 12) or 0),
            direct_parallel_min_mb=int(self._cfg("direct_parallel_min_mb", 4) or 4),
            # 媒体域名分流：站点页面恒走代理，视频 CDN 自动判定直连/代理
            proxy_bypass_hosts=str(self._cfg("proxy_bypass_hosts", "") or ""),
            proxy_force_hosts=str(self._cfg("proxy_force_hosts", "") or ""),
            auto_probe_direct=bool(self._cfg("auto_probe_direct", True)),
            probe_timeout=int(self._cfg("probe_timeout", 5) or 5),
            http2=bool(self._cfg("http2", True)),
        )

    def _preferred_template(self, base: str) -> str:
        """取该站点上次命中的搜索模板（没有则返回空串）。"""
        try:
            m = self.store.get("search_template_ok", {}) or {}
            if not isinstance(m, dict):
                return ""
            return str(m.get(self._host_of(base), "") or "")
        except Exception:
            return ""

    def _remember_template(self, base: str, hit: dict) -> None:
        """记住本次命中的搜索模板；下次跳过已知不可用的模板。"""
        tpl = str((hit or {}).get("template") or "")
        if not tpl:
            return
        try:
            m = self.store.get("search_template_ok", {}) or {}
            if not isinstance(m, dict):
                m = {}
            m[self._host_of(base)] = tpl
            if len(m) > 20:  # 只留最近 20 个站点
                m = dict(list(m.items())[-20:])
            self.store.set("search_template_ok", m)
            logger.info(f"[mailvideo] 记录搜索模板偏好: {self._host_of(base)} -> {tpl}")
        except Exception as e:
            logger.debug(f"[mailvideo] 记录搜索模板偏好失败（忽略）: {e}")

    @staticmethod
    def _host_of(url: str) -> str:
        try:
            from urllib.parse import urlparse

            return (urlparse(url).hostname or url or "").lower()
        except Exception:
            return url or ""

    def _fused_encode_opts(self, fetcher) -> Optional[dict]:
        """决定是否把「压缩」提前到下载的合并阶段一趟完成。

        返回压缩参数字典交给 ``VideoFetcher.download``（HLS 走「合并+压缩」
        一次 ffmpeg 调用，不再落一份完整体积的中转 mp4）；返回 ``None``
        则沿用原来的「下完整文件 → 再单独压缩」流程。

        仅在**压缩阈值 = 0（总是压缩）**时启用：阈值 > 0 时需要先知道文件
        实际多大才能决定压不压，而下载阶段无从预判。
        """
        if not bool(self._cfg("compress_enable", False)):
            return None
        if not bool(self._cfg("compress_fuse_hls", True)):
            return None
        if int(self._cfg("compress_threshold_mb", 0) or 0) > 0:
            return None
        if not fetcher.ffmpeg_available():
            return None
        return {
            "target_height": int(self._cfg("compress_target_height", 720) or 0),
            "crf": int(self._cfg("compress_crf", 30) or 30),
            "preset": str(self._cfg("compress_preset", "veryfast") or "veryfast").strip(),
            "target_bitrate": str(self._cfg("compress_target_bitrate", "") or "").strip(),
            "audio_bitrate": str(self._cfg("compress_audio_bitrate", "64k") or "64k").strip(),
        }

    def _allowed(self, event: AstrMessageEvent) -> bool:
        allow = self._cfg("allowed_users", []) or []
        if not allow:
            return True
        return str(event.get_sender_id()) in {str(x) for x in allow}

    # 候选清单相关配置 ------------------------------------------------- #
    def _select_timeout(self) -> int:
        return max(10, int(self._cfg("select_timeout", 180) or 180))

    def _select_max(self) -> int:
        return max(1, int(self._cfg("select_candidates", 10) or 10))

    # ================================================================== #
    # 指令
    # ================================================================== #
    @filter.event_message_type(EventMessageType.PRIVATE_MESSAGE)
    async def on_private_message(self, event: AstrMessageEvent):
        """私聊钩子：用户发消息时，优先补发该会话暂存的「待投递」结果
        （如长流程结束后因 QQ 官方主动消息窗口过期而没发出去的视频）。"""
        await self._deliver_pending_if_any(event)

    @filter.event_message_type(EventMessageType.ALL)
    async def on_any_message(self, event: AstrMessageEvent):
        """拦截「候选选择」的回复：只有该会话有待选候选、且文本是序号/取消时才响应，
        处理完直接终止事件传播，避免再被 LLM 或其它插件接走。"""
        try:
            text = (event.message_str or "").strip()
            if not text or text[0] in "/!！":
                return  # 指令交给 cmd_video 处理
            if not self._allowed(event):
                return
            if await self._try_handle_selection(event, text):
                event.stop_event()
        except Exception as e:
            logger.error(f"[mailvideo] 候选选择处理异常: {e}")

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
            fixed = self._enabled_fixed_sites()
            cached = (fixed[0] if fixed else "") or self.store.get_last_address() or "（暂无）"
            extra = ""
            if len(fixed) > 1:
                extra = f"（共 {len(fixed)} 个固定站点已启用）"
            yield event.plain_result(
                "邮箱取址视频搜索 · 用法\n"
                "/搜片 <关键词>   搜索并回传视频\n"
                "/搜片 地址        获取并缓存站点地址\n"
                "/搜片 邮箱        测试邮箱连接\n"
                "/搜片 取消        取消待选的候选\n"
                "/搜片 帮助        显示本帮助\n"
                f"\n当前站点地址：{cached}{extra}\n"
                "首次使用请先在插件配置中填写邮箱账号、授权码与收件邮箱。"
            )
            return

        # ── 子指令：/搜片 取消 ──
        if kw in ("取消", "放弃", "cancel"):
            if self.store.pop_selection(event.unified_msg_origin):
                yield event.plain_result("已取消本次候选选择。")
            else:
                yield event.plain_result("当前没有待选择的候选。")
            return

        # ── 主流程：/搜片 <关键词> ──
        if not keyword:
            yield event.plain_result(
                "用法：/搜片 <关键词>\n例如：/搜片 鞠婧祎\n\n"
                "子指令：\n/搜片 地址   获取站点地址\n"
                "/搜片 邮箱   测试邮箱连接\n/搜片 取消   取消候选选择\n"
                "/搜片 帮助   显示帮助"
            )
            return

        # 该会话正等待选择时，/搜片 3 也视作「选了第 3 个候选」
        if await self._try_handle_selection(event, keyword):
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
        task = asyncio.create_task(
            self._run_pipeline(umo, keyword, lock, event.get_sender_id())
        )
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

    async def _run_pipeline(
        self, umo: str, keyword: str, lock: asyncio.Lock, sender_id: str = ""
    ):
        async with lock:
            try:
                # ---- ① 取地址 ----
                address, _ = await self._fetch_address(umo)
                if not address:
                    await self._send_text(umo, "❌ 未能获取站点地址，流程终止。", critical=True)
                    return

                # ---- ② 搜索 ----
                await self._send_text(umo, f"② 站点地址：{address}\n③ 正在搜索「{keyword}」…")
                fetcher = self._build_fetcher()
                candidates, hit = await asyncio.to_thread(
                    fetcher.search_with_meta, address, keyword, self._preferred_template(address)
                )
                self._remember_template(address, hit)
                if not candidates:
                    await self._send_text(umo, "❌ 未搜索到相关视频。", critical=True)
                    return

                # ---- ③ 列出候选，等用户回复序号后再下载那一个 ----
                await self._send_candidate_list(
                    umo, keyword, address, candidates, sender_id
                )
            except Exception as e:
                logger.error(f"[mailvideo] 处理失败: {e}", exc_info=True)
                await self._send_text(umo, f"❌ 处理失败：{e}")

    async def _download_one(
        self,
        umo: str,
        fetcher,
        cand: dict,
        tmp_dir: Path,
        max_size_mb: int,
        tag: str = "",
    ) -> tuple[bool, str]:
        """下载单个候选并回传，返回 (是否已处理, 错误信息)。"""
        url = cand.get("url") or ""
        # 压缩开启且阈值为 0 时，让 HLS 下载在合并阶段顺带完成压缩，
        # 省掉一份完整体积的中转文件和一次全量读写。
        encode_opts = self._fused_encode_opts(fetcher)
        _dl_started = time.time()
        try:
            result = await asyncio.to_thread(
                fetcher.download,
                url,
                tmp_dir,
                max_size_mb,
                cand.get("title") or "",
                encode_opts,
            )
        except Exception as e:
            logger.warning(
                f"[mailvideo] —— 下载失败（耗时 {time.time() - _dl_started:.1f}s）{url}: {e}"
            )
            return False, str(e)

        dl_seconds = time.time() - _dl_started
        path = Path(result["path"])
        size_mb = path.stat().st_size / 1024 / 1024
        logger.info(
            f"[mailvideo] —— 下载成功 —— {path.name} | {size_mb:.2f}MB | "
            f"耗时 {dl_seconds:.1f}s"
        )

        # ---- ⓸b 画质压缩（可选）----
        # HLS 路径若已在合并阶段顺带编码，这里直接跳过，避免压两遍。
        cp_seconds = 0.0
        if result.get("pre_encoded"):
            logger.info("[mailvideo] —— 已在下载合并阶段顺带压缩，跳过独立压缩")
        else:
            _cp_started = time.time()
            path, size_mb = await self._maybe_compress(umo, fetcher, path, size_mb, tmp_dir)
            cp_seconds = time.time() - _cp_started

        if size_mb > max_size_mb:
            await self._send_text(
                umo,
                f"⚠️ 文件过大（{size_mb:.1f}MB > {max_size_mb}MB），"
                f"改为返回链接：\n{result.get('webpage_url') or url}",
                critical=True,
            )
            return True, ""

        suffix = f"（{tag}）" if tag else ""
        _send_started = time.time()
        ok = await self._send_video(umo, path)
        send_seconds = time.time() - _send_started
        if ok:
            # 发送成功：立即删除本地文件，不依赖 finally 兜底，避免长时间占用磁盘
            try:
                path.unlink(missing_ok=True)
            except OSError as e:
                logger.warning(f"[mailvideo] 删除已发送文件失败: {e}")
            logger.info(
                f"[mailvideo] ✅ 发送成功{suffix} | {path.name} | {size_mb:.2f}MB | "
                f"已送达用户，本地文件已删除"
            )
        else:
            logger.warning(
                f"[mailvideo] ⚠️ 发送失败{suffix} | {path.name} | {size_mb:.2f}MB | "
                f"已暂存为待投递，等用户下次发消息时补发"
            )
        logger.info(
            f"[mailvideo] ⏱️ 本次耗时明细 | 下载 {dl_seconds:.1f}s"
            + (f" | 压缩 {cp_seconds:.1f}s" if cp_seconds else "")
            + f" | 发送 {send_seconds:.1f}s"
            + f" | 合计 {dl_seconds + cp_seconds + send_seconds:.1f}s"
            + f" | 最终体积 {size_mb:.2f}MB"
        )
        return True, ""

    # ================================================================== #
    # 候选选择模式（列出候选 → 用户回复序号 → 只下载那一条）
    # ================================================================== #
    @staticmethod
    def _cand_title(cand: dict, idx: int = 0) -> str:
        """候选的展示标题：优先标题，退而用 URL，最后兜底编号。"""
        title = str(cand.get("title") or "").strip()
        url = str(cand.get("url") or "").strip()
        if not title or title == url:
            title = re.sub(r"^https?://", "", url)
        title = title.strip()
        if not title:
            title = f"候选 {idx}" if idx else "未命名"
        return title[:60]

    async def _send_candidate_list(
        self,
        umo: str,
        keyword: str,
        address: str,
        candidates: list,
        sender_id: str = "",
    ) -> None:
        """搜索完成后不立即下载，先把候选编号列出，等用户回复序号。"""
        shown = list(candidates[: self._select_max()])
        timeout = self._select_timeout()
        lines = [
            f"\u200b📋 「{keyword}」共找到 {len(candidates)} 个候选，回复序号选择：",
            "",
        ]
        for i, cand in enumerate(shown, 1):
            lines.append(f"{i}. {self._cand_title(cand, i)}")
        lines.append("")
        lines.append(
            f"回复数字即可（如「3」）· 回复「取消」放弃 · {timeout} 秒内有效"
        )
        self.store.set_selection(
            umo,
            {
                "keyword": keyword,
                "address": address,
                "candidates": shown,
                "sender_id": str(sender_id or ""),
            },
        )
        await self._send_text(umo, "\n".join(lines))
        logger.info(f"[mailvideo] 已列出 {len(shown)}/{len(candidates)} 个候选，等待选择: {umo}")

    async def _try_handle_selection(self, event: AstrMessageEvent, text: str) -> bool:
        """若该会话有待选候选且本条消息是有效选择，处理它并返回 True。"""
        umo = event.unified_msg_origin
        session = self.store.get_selection(umo)
        if not session:
            return False

        owner = str(session.get("sender_id") or "")
        sender = str(event.get_sender_id() or "")
        if owner and sender and owner != sender:
            return False  # 群聊里只认发起者本人的回复

        norm = re.sub(r"\s+", "", text or "")
        if not norm:
            return False

        if norm.lower() in _CANCEL_WORDS:
            self.store.pop_selection(umo)
            await self._send_text(umo, "已取消本次候选选择。")
            return True

        m = _SELECT_RE.fullmatch(norm)
        if not m:
            return False

        if time.time() - int(session.get("ts", 0) or 0) > self._select_timeout():
            self.store.pop_selection(umo)
            await self._send_text(umo, "⏳ 候选列表已超时失效，请重新发送 /搜片 <关键词>。")
            return True

        cands = list(session.get("candidates") or [])
        idx = int(m.group(1))
        if not (1 <= idx <= len(cands)):
            await self._send_text(
                umo,
                f"❌ 序号超出范围，请回复 1-{len(cands)} 之间的数字（或回复「取消」）。",
            )
            return True

        # 先弹出会话，避免重复点选造成重复下载；下载失败会在任务里放回
        self.store.pop_selection(umo)
        await self._send_text(
            umo,
            f"· 已选择第 {idx} 个：{self._cand_title(cands[idx - 1], idx)}\n④ 开始下载…",
        )
        task = asyncio.create_task(self._run_selection(umo, session, idx - 1))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return True

    async def _run_selection(self, umo: str, session: dict, index: int) -> None:
        """下载用户选中的那一个候选并回传；失败则把候选列表放回去让用户换一个。"""
        cands = list(session.get("candidates") or [])
        if not (0 <= index < len(cands)):
            return
        cand = cands[index]

        lock = self._locks.get(umo)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[umo] = lock

        async with lock:
            tmp_dir = self.store.temp_dir / f"job_{int(time.time())}"
            try:
                fetcher = self._build_fetcher()
                if not fetcher.ffmpeg_available():
                    await self._send_text(
                        umo, "⚠️ 未检测到 ffmpeg，m3u8 合并/高清合流可能失败，将尽量尝试。"
                    )
                max_size_mb = max(1, int(self._cfg("max_video_size_mb", 100) or 100))
                label = self._cand_title(cand, index + 1)
                logger.info(
                    f"[mailvideo] —— 选择模式下载第 {index + 1} 个 —— "
                    f"{label} | {cand.get('url')}"
                )
                handled, err = await self._download_one(
                    umo, fetcher, cand, tmp_dir, max_size_mb, tag="选择"
                )
                if handled:
                    logger.info(f"[mailvideo] 选择模式回传成功: {label}")
                    return
                await self._send_text(
                    umo,
                    f"❌ 第 {index + 1} 个下载失败：{err}\n"
                    "可回复其它序号重试，或回复「取消」放弃。",
                    critical=True,
                )
                # 失败：恢复候选列表，允许换个序号重试
                self.store.set_selection(umo, {**session, "candidates": cands})
            except Exception as e:
                logger.error(f"[mailvideo] 选择模式下载失败: {e}", exc_info=True)
                await self._send_text(umo, f"❌ 处理失败：{e}")
            finally:
                self._cleanup(tmp_dir)

    def _enabled_fixed_sites(self) -> list[str]:
        """返回所有已启用且非空的固定站点地址，按 1→2→3 顺序。"""
        out: list[str] = []
        for i in (1, 2, 3):
            if not bool(self._cfg(f"site_{i}_enabled", False)):
                continue
            url = str(self._cfg(f"site_{i}_address", "") or "").strip()
            if not url:
                continue
            if not url.startswith(("http://", "https://")):
                url = "https://" + url
            out.append(url.rstrip("/") + "/")
        return out

    async def _fetch_address(self, umo: str) -> tuple[Optional[str], str]:
        """取站点地址：优先用已启用的固定站点；否则走「发信→等回信→提取」。"""
        # ── 固定站点：按顺序取第一个启用的 ──
        fixed_sites = self._enabled_fixed_sites()
        if fixed_sites:
            address = fixed_sites[0]
            self.store.set_last_address(address)
            logger.info(
                f"[mailvideo] 使用固定站点({len(fixed_sites)}个已启用): {address}"
            )
            return address, ""

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
        try:
            size_mb = path.stat().st_size / 1024 / 1024
        except OSError:
            size_mb = 0.0
        desc = f"{path.name} | {size_mb:.2f}MB"
        send_started = time.time()
        for attempt in range(1, 4):
            logger.info(f"[mailvideo] 📤 开始发送（第 {attempt}/3 次）：{desc}")
            _t0 = time.time()
            # 心跳：send_msg 整包传完才返回，中间没有任何进度回调，
            # 这里在等待期间定期打日志，避免看起来“卡死”。
            hb = asyncio.create_task(self._send_heartbeat(desc, attempt, _t0))
            try:
                if send_as == "file":
                    chain = MessageChain([Comp.File(file=str(path), name=path.name)])
                else:
                    chain = MessageChain([Comp.Video.fromFileSystem(path=str(path))])
                await self.context.send_message(umo, chain)
                logger.info(
                    f"[mailvideo] ✅ 发送成功（第 {attempt}/3 次）：{desc} | "
                    f"单次耗时 {time.time() - _t0:.1f}s | 累计 {time.time() - send_started:.1f}s"
                )
                return True
            except Exception as e:
                logger.warning(
                    f"[mailvideo] ❌ 发送失败（第 {attempt}/3 次）：{desc} | "
                    f"耗时 {time.time() - _t0:.1f}s | {e}"
                )
                if attempt < 3:
                    await asyncio.sleep(20)
            finally:
                hb.cancel()
        # 重试全部失败：暂存待投递，等用户下次发消息时补发
        logger.warning(f"[mailvideo] 📥 发送重试耗尽，转待投递：{desc}")
        self._stash_pending(umo, {"type": "video", "path": str(path), "name": path.name})
        return False

    async def _send_heartbeat(self, desc: str, attempt: int, t0: float) -> None:
        """发送等待期间定期写日志，替代 QQ/OneBot 缺失的上传进度回调。

        OneBot 的 send_msg / upload_group_file 属于一次性 RPC：整包上传完成
        才返回结果，中途不会回调进度。所以这里用心跳把「已等待多久」写进
        日志，至少能区分「在传」和「卡死」。
        """
        interval = max(1, int(self._cfg("send_heartbeat_interval", 5) or 5))
        try:
            while True:
                await asyncio.sleep(interval)
                logger.info(
                    f"[mailvideo] ⏳ 发送中（第 {attempt}/3 次）：{desc} | "
                    f"已等待 {time.time() - t0:.0f}s（等待平台上传返回）"
                )
        except asyncio.CancelledError:
            pass

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
                size_mb = path.stat().st_size / 1024 / 1024 if path.exists() else 0
                path.unlink(missing_ok=True)
                logger.info(
                    f"[mailvideo] ✅ 待投递视频补发成功 | {path.name} | {size_mb:.2f}MB | 本地文件已删除"
                )
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
