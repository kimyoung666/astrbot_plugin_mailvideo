"""邮箱收发客户端：SMTP 发信 + IMAP 收信 + 回信匹配。

邮件协议使用标准库 ``smtplib`` / ``imaplib`` / ``email``，避免额外依赖；
由于它们是同步阻塞的，统一用 ``asyncio.to_thread`` 包一层，避免阻塞事件循环。
"""

from __future__ import annotations

import asyncio
import email
import imaplib
import re
import smtplib
import ssl
from datetime import datetime, timezone
from email.header import decode_header, make_header
from email.message import Message
from email.utils import make_msgid, parsedate_to_datetime
from typing import Callable, Optional

from astrbot.api import logger

__all__ = ["MailClient", "MailError"]


class MailError(Exception):
    """邮箱相关错误。"""


# --------------------------------------------------------------------------- #
# 解析辅助
# --------------------------------------------------------------------------- #
def _decode_mime_words(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return value


def _html_to_text(html: str) -> str:
    """极简 HTML 转纯文本，保留链接（把 href 也拼进正文，便于提取地址）。"""
    # 先把 <a href="...">text</a> 转成 "text URL"
    html = re.sub(
        r'<a[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        lambda m: f"{re.sub(r'<[^>]+>', '', m.group(2))} {m.group(1)}",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    # 去掉 script/style
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    # 标签变空格
    html = re.sub(r"<[^>]+>", " ", html)
    # 常见实体
    for entity, char in (
        ("&nbsp;", " "),
        ("&amp;", "&"),
        ("&lt;", "<"),
        ("&gt;", ">"),
        ("&quot;", '"'),
        ("&#39;", "'"),
    ):
        html = html.replace(entity, char)
    return re.sub(r"[ \t\r\f\v]+", " ", html)


def extract_text_from_message(msg: Message) -> str:
    """从邮件对象中提取纯文本正文（优先 text/plain，回退 text/html）。"""
    plain_parts: list[str] = []
    html_parts: list[str] = []

    for part in msg.walk():
        ctype = part.get_content_type()
        disp = str(part.get("Content-Disposition") or "")
        if "attachment" in disp.lower():
            continue
        if ctype not in ("text/plain", "text/html"):
            continue
        payload = part.get_payload(decode=True)
        if payload is None:
            continue
        charset = part.get_content_charset() or "utf-8"
        try:
            text = payload.decode(charset, errors="ignore")
        except (LookupError, UnicodeDecodeError):
            text = payload.decode("utf-8", errors="ignore")
        if ctype == "text/plain":
            plain_parts.append(text)
        else:
            html_parts.append(_html_to_text(text))

    if plain_parts:
        return "\n".join(plain_parts)
    if html_parts:
        return "\n".join(html_parts)
    return ""


def _parse_date(msg: Message) -> Optional[datetime]:
    raw = msg.get("Date")
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# 客户端
# --------------------------------------------------------------------------- #
class MailClient:
    """封装一个邮箱账号的收发能力。"""

    def __init__(
        self,
        account: str,
        auth_code: str,
        imap_server: str = "imap.qq.com",
        imap_port: int = 993,
        smtp_server: str = "smtp.qq.com",
        smtp_port: int = 465,
        recipient: str = "",
        timeout: int = 30,
    ) -> None:
        self.account = account
        self.auth_code = auth_code
        self.imap_server = imap_server
        self.imap_port = int(imap_port)
        self.smtp_server = smtp_server
        self.smtp_port = int(smtp_port)
        self.recipient = recipient
        self.timeout = timeout

    # -------------------------------------------------------------- #
    # 配置校验
    # -------------------------------------------------------------- #
    def validate(self) -> None:
        if not self.account or not self.auth_code:
            raise MailError("未配置邮箱账号或授权码，请在插件配置中填写。")
        if not self.recipient:
            raise MailError("未配置收件邮箱（recipient_email）。")

    # -------------------------------------------------------------- #
    # SMTP 发信（同步）
    # -------------------------------------------------------------- #
    def _send_sync(self, subject: str, body: str, to_addr: str) -> tuple[str, datetime]:
        from email.mime.text import MIMEText

        msg = MIMEText(body, "plain", "utf-8")
        msg["From"] = self.account
        msg["To"] = to_addr
        msg["Subject"] = subject
        message_id = make_msgid()
        msg["Message-ID"] = message_id
        sent_at = datetime.now(timezone.utc)

        ctx = ssl.create_default_context()
        if self.smtp_port == 465:
            server = smtplib.SMTP_SSL(
                self.smtp_server, self.smtp_port, timeout=self.timeout, context=ctx
            )
        else:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=self.timeout)
            server.ehlo()
            server.starttls(context=ctx)
            server.ehlo()
        try:
            server.login(self.account, self.auth_code)
            server.send_message(msg)
        finally:
            try:
                server.quit()
            except Exception:
                pass
        return message_id, sent_at

    async def send_mail(self, subject: str, body: str, to_addr: str = "") -> tuple[str, datetime]:
        """发送邮件，返回 (Message-ID, 发送时间)。"""
        self.validate()
        to_addr = to_addr or self.recipient
        return await asyncio.to_thread(self._send_sync, subject, body, to_addr)

    # -------------------------------------------------------------- #
    # IMAP 收信（同步）
    # -------------------------------------------------------------- #
    def _connect_imap(self) -> imaplib.IMAP4:
        ctx = ssl.create_default_context()
        if self.imap_port == 993:
            conn: imaplib.IMAP4 = imaplib.IMAP4_SSL(
                self.imap_server, self.imap_port, timeout=self.timeout, ssl_context=ctx
            )
        else:
            conn = imaplib.IMAP4(self.imap_server, self.imap_port, timeout=self.timeout)
            conn.starttls(ssl_context=ctx)
        conn.login(self.account, self.auth_code)
        return conn

    def test_connection_sync(self) -> str:
        """测试 IMAP 登录，返回可用邮箱夹数量信息（用于 /搜片邮箱 指令）。"""
        conn = self._connect_imap()
        try:
            typ, data = conn.select("INBOX", readonly=True)
            if typ != "OK":
                raise MailError(f"无法打开收件箱: {data}")
            typ, data = conn.search(None, "ALL")
            total = len(data[0].split()) if data and data[0] else 0
            return f"IMAP 正常，收件箱共 {total} 封邮件。"
        finally:
            try:
                conn.logout()
            except Exception:
                pass

    async def test_connection(self) -> str:
        self.validate()
        return await asyncio.to_thread(self.test_connection_sync)

    def _fetch_since_sync(self, since_date: str, limit: int = 20) -> list[dict]:
        """抓取 since_date（IMAP 日期格式，如 12-Oct-2026）之后的邮件。"""
        conn = self._connect_imap()
        results: list[dict] = []
        try:
            typ, _ = conn.select("INBOX", readonly=True)
            if typ != "OK":
                raise MailError("无法打开收件箱")
            typ, data = conn.search(None, f'(SINCE "{since_date}")')
            if typ != "OK" or not data or not data[0]:
                return []
            uids = data[0].split()[-limit:]
            for uid in uids:
                typ, msg_data = conn.fetch(uid, "(RFC822)")
                if typ != "OK" or not msg_data:
                    continue
                raw = None
                for item in msg_data:
                    if isinstance(item, tuple) and len(item) >= 2:
                        raw = item[1]
                        break
                if not raw:
                    continue
                msg = email.message_from_bytes(raw)
                results.append(
                    {
                        "uid": uid.decode() if isinstance(uid, bytes) else str(uid),
                        "subject": _decode_mime_words(msg.get("Subject")),
                        "from": _decode_mime_words(msg.get("From")),
                        "in_reply_to": (msg.get("In-Reply-To") or "").strip(),
                        "references": (msg.get("References") or "").strip(),
                        "date": _parse_date(msg),
                        "text": extract_text_from_message(msg),
                    }
                )
        finally:
            try:
                conn.logout()
            except Exception:
                pass
        return results

    async def fetch_messages_since(self, since_ts: int, limit: int = 20) -> list[dict]:
        """异步抓取指定时间戳之后的邮件（按天粒度检索，再精确过滤）。"""
        since_dt = datetime.fromtimestamp(since_ts, tz=timezone.utc)
        since_date = since_dt.strftime("%d-%b-%Y")
        return await asyncio.to_thread(self._fetch_since_sync, since_date, limit)

    # -------------------------------------------------------------- #
    # 回信匹配 + 轮询
    # -------------------------------------------------------------- #
    @staticmethod
    def _matches(
        mail: dict,
        message_id: str,
        original_subject: str,
        recipient: str,
        after_ts: int,
    ) -> bool:
        # 时间必须晚于发信时间（留 120s 余量，容忍服务器时钟误差）
        dt = mail.get("date")
        if dt is not None and dt.timestamp() < after_ts - 120:
            return False

        in_reply_to = mail.get("in_reply_to") or ""
        references = mail.get("references") or ""
        subject = mail.get("subject") or ""
        sender = mail.get("from") or ""

        if message_id and (message_id in in_reply_to or message_id in references):
            return True
        core_subject = re.sub(r"^(re|回复|答复|fw|fwd|转发)\s*[:：]\s*", "", subject, flags=re.I).strip()
        if original_subject and core_subject and original_subject.strip() in core_subject:
            return True
        if recipient and recipient.lower() in sender.lower():
            return True
        return False

    async def wait_for_reply(
        self,
        after_ts: int,
        message_id: str = "",
        original_subject: str = "",
        timeout: int = 120,
        interval: int = 5,
        exclude_uids: Optional[set[str]] = None,
        progress_cb: Optional[Callable[[float], None]] = None,
    ) -> Optional[dict]:
        """轮询等待回信，返回匹配到的邮件 dict（含 text 字段），超时返回 None。

        ``progress_cb`` 会收到 0~1 的进度值，便于上层提示用户。
        """
        self.validate()
        exclude_uids = exclude_uids or set()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(5, int(timeout))
        while True:
            mails = await self.fetch_messages_since(after_ts)
            # 最新优先：UID 越大越新（非纯数字 UID 退化为字符串比较）
            def _uid_key(m: dict):
                raw = str(m.get("uid") or "")
                return int(raw) if raw.isdigit() else -1

            mails.sort(key=_uid_key, reverse=True)
            for mail in mails:
                if mail["uid"] in exclude_uids:
                    continue
                if self._matches(
                    mail, message_id, original_subject, self.recipient, after_ts
                ):
                    return mail
            now = loop.time()
            if now >= deadline:
                return None
            if progress_cb:
                elapsed = max(0.0, (now - (deadline - max(5, int(timeout)))))
                try:
                    progress_cb(min(0.99, elapsed / max(1, timeout)))
                except Exception:
                    pass
            logger.debug("[mailvideo] 等待回信中…")
            await asyncio.sleep(max(2, int(interval)))
