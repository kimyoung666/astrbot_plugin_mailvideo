"""插件自检测试（无需 AstrBot 运行时）。

在导入被测模块前，会注入一个最小的 ``astrbot.api`` 桩，只提供 ``logger``，
从而可以在沙箱/本地直接跑通核心逻辑的单元测试。

运行：
    python tests/selftest.py
"""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_DIR))


# --------------------------------------------------------------------------- #
# 注入 astrbot 桩
# --------------------------------------------------------------------------- #
def _install_astrbot_stub() -> None:
    if "astrbot" in sys.modules:
        return
    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")

    class _Logger:
        def __getattr__(self, _name):
            return lambda *a, **k: None

    api.logger = _Logger()
    astrbot.api = api
    sys.modules["astrbot"] = astrbot
    sys.modules["astrbot.api"] = api


_install_astrbot_stub()


def _load_core(name: str):
    full = f"selftest_core.{name}"
    if full in sys.modules:
        return sys.modules[full]
    spec = importlib.util.spec_from_file_location(full, PLUGIN_DIR / "core" / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    spec.loader.exec_module(module)
    return module


address_extractor = _load_core("address_extractor")
mail_client = _load_core("mail_client")
video_fetcher = _load_core("video_fetcher")
state_store = _load_core("state_store")

extract_address = address_extractor.extract_address
MailClient = mail_client.MailClient
extract_text_from_message = mail_client.extract_text_from_message
_html_to_text = mail_client._html_to_text
VideoFetcher = video_fetcher.VideoFetcher
StateStore = state_store.StateStore


# --------------------------------------------------------------------------- #
class TestAddressExtractor(unittest.TestCase):
    def test_plain_text_prefers_boosted_link(self):
        text = (
            "您好，这是自动回复。\n"
            "最新地址：https://good-site.top\n"
            "如有疑问请联系 help@qq.com 或访问 https://www.qq.com/privacy\n"
        )
        self.assertEqual(extract_address(text), "https://good-site.top")

    def test_html_link_is_kept(self):
        text = _html_to_text(
            '<p>请访问 <a href="https://nice-site.cc">最新地址</a> 收藏本页</p>'
            '<a href="https://unsubscribe.example.net/u/1">退订</a>'
        )
        self.assertEqual(extract_address(text), "https://nice-site.cc")

    def test_bare_domain(self):
        text = "最新地址 www.mysite.tv 请收藏，防止失联。"
        self.assertEqual(extract_address(text), "http://www.mysite.tv")

    def test_noise_only_returns_none(self):
        text = "本邮件由系统发送，请勿回复。官网 https://www.qq.com"
        self.assertIsNone(extract_address(text))

    def test_custom_regex(self):
        text = "地址是 ==> ADDR: good-site.top <=="
        self.assertEqual(
            extract_address(text, custom_regex=r"ADDR:\s*([^\s<]+)"),
            "http://good-site.top",
        )

    def test_trailing_punctuation_stripped(self):
        text = "最新地址: https://final-site.cc。谢谢！"
        self.assertEqual(extract_address(text), "https://final-site.cc")


class TestMailClient(unittest.TestCase):
    def test_extract_text_prefers_plain(self):
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText("纯文本正文 最新地址 https://plain.top", "plain", "utf-8"))
        msg.attach(MIMEText("<p>HTML 正文</p>", "html", "utf-8"))
        text = extract_text_from_message(msg)
        self.assertIn("https://plain.top", text)

    def test_extract_text_from_html_only(self):
        msg = MIMEMultipart("alternative")
        msg.attach(MIMEText('<a href="https://html.top">点这里</a>', "html", "utf-8"))
        text = extract_text_from_message(msg)
        self.assertIn("https://html.top", text)

    def test_matches_by_message_id(self):
        now = datetime.now(timezone.utc)
        mail = {
            "uid": "1",
            "subject": "Re: 获取地址",
            "from": "auto@service.com",
            "in_reply_to": "<abc@qq.com>",
            "references": "",
            "date": now,
            "text": "https://x.top",
        }
        self.assertTrue(
            MailClient._matches(mail, "<abc@qq.com>", "获取地址", "me@qq.com", int(now.timestamp()) - 10)
        )

    def test_matches_by_subject(self):
        now = datetime.now(timezone.utc)
        mail = {
            "uid": "2",
            "subject": "回复: 获取地址",
            "from": "auto@service.com",
            "in_reply_to": "",
            "references": "",
            "date": now,
            "text": "x",
        }
        self.assertTrue(
            MailClient._matches(mail, "<none>", "获取地址", "me@qq.com", int(now.timestamp()) - 10)
        )

    def test_rejects_old_mail(self):
        old = datetime.now(timezone.utc) - timedelta(days=2)
        mail = {
            "uid": "3",
            "subject": "Re: 获取地址",
            "from": "auto@service.com",
            "in_reply_to": "<abc@qq.com>",
            "references": "",
            "date": old,
            "text": "x",
        }
        self.assertFalse(
            MailClient._matches(mail, "<abc@qq.com>", "获取地址", "me@qq.com", int(datetime.now(timezone.utc).timestamp()))
        )


class TestVideoFetcher(unittest.TestCase):
    def test_build_search_url(self):
        f = VideoFetcher(search_url_template="{base}/search?wd={keyword}")
        url = f.build_search_url("https://site.top/", "复仇者 联盟")
        self.assertTrue(url.startswith("https://site.top/search?wd="))
        self.assertNotIn(" ", url)  # 关键词已编码

    def test_build_search_url_custom_template(self):
        f = VideoFetcher(search_url_template="{base}/index.php/vod/search.html?wd={keyword}")
        self.assertEqual(
            f.build_search_url("https://s.cc", "abc"),
            "https://s.cc/index.php/vod/search.html?wd=abc",
        )

    def test_score_candidate(self):
        score_media = VideoFetcher._score_candidate("https://s.cc/v/1.mp4", "片子", "片子")
        score_nav = VideoFetcher._score_candidate("https://s.cc/about", "关于", "片子")
        self.assertGreater(score_media, score_nav)


class TestStateStore(unittest.TestCase):
    def test_last_address_and_uids(self):
        with tempfile.TemporaryDirectory() as d:
            store = StateStore(Path(d))
            self.assertEqual(store.get_last_address(), "")
            store.set_last_address("https://persist.top")
            store.add_processed_uids(["1", "2"])
            store.add_processed_uids(["2", "3"])
            self.assertEqual(store.get_last_address(), "https://persist.top")
            self.assertEqual(sorted(store.get_processed_uids()), ["1", "2", "3"])
            # 重新加载应保留
            store2 = StateStore(Path(d))
            self.assertEqual(store2.get_last_address(), "https://persist.top")


if __name__ == "__main__":
    unittest.main(verbosity=2)
