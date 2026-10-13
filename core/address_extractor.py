"""从邮件正文中提取站点地址（域名 / 入口链接）。

策略：
1. 若配置了自定义正则，优先按正则提取；
2. 否则用内置规则扫描正文，收集 ``http(s)://`` 链接与裸域名候选；
3. 过滤邮箱服务商 / 退订 / 追踪域名 / 静态资源等噪声；
4. 对「地址、网址、最新、官网、发布页」等关键词附近的候选加权，取分最高者。
"""

from __future__ import annotations

import re
from urllib.parse import urlparse

# --------------------------------------------------------------------------- #
# 正则
# --------------------------------------------------------------------------- #
_URL_RE = re.compile(r"https?://[^\s<>\"'()\[\]{}，。、；：！？【】《》]+", re.IGNORECASE)
_BARE_DOMAIN_RE = re.compile(
    r"(?<![@\w.])((?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+"
    r"(?:com|net|org|cc|tv|me|io|top|xyz|vip|club|site|online|info|app|pro|fun|live|"
    r"biz|cn|com\.cn|net\.cn|org\.cn|cc\.cn|co|la|pw|us|ws|tk|ml|ga|cf|gd|link))"
    r"(?:[:/][^\s<>\"'()\[\]{}，。、；：！？【】《》]*)?",
    re.IGNORECASE,
)
# 需要剥离的尾部标点
_TRAILING = "。，,.;；:：!！?？)）]】}》>\"'、"

# 明确要排除的噪声域名（邮箱服务商、基础设施、追踪等）
_NOISE_DOMAINS = (
    "qq.com",
    "qqmail.com",
    "foxmail.com",
    "163.com",
    "126.com",
    "139.com",
    "sina.com",
    "gmail.com",
    "google.com",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "yahoo.com",
    "icloud.com",
    "me.com",
    "mail.com",
    "aliyun.com",
    "alibaba.com",
    "bytecdn.cn",
    "weixin.qq.com",
    "w3.org",
    "schema.org",
    "example.com",
    "mailchimp.com",
    "sendgrid.net",
    "amazonses.com",
    "cloudflare.com",
    "gstatic.com",
    "googleapis.com",
)

# 出现即视为噪声的关键词（出现在候选附近或 URL 中）
_NOISE_KEYWORDS = (
    "unsubscribe",
    "退订",
    "privacy",
    "隐私",
    "help",
    "帮助中心",
    "service@",
    "noreply",
    "no-reply",
    "microsoft",
    "android.com",
    "beian",
    "icp",
)

# 静态资源后缀
_STATIC_EXT = (
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
    ".css",
    ".js",
    ".woff",
    ".woff2",
    ".ttf",
)

# 加权关键词：命中说明该行大概率是「最新地址」
_BOOST_KEYWORDS = (
    "最新地址",
    "地址发布",
    "发布页",
    "网址发布",
    "新地址",
    "官网",
    "永久地址",
    "請收藏",
    "请收藏",
    "记住本页",
    "回家地址",
    "入口",
    "线路",
    "备用地址",
    "最新网址",
    "网址",
    "地址",
)


def _is_noise(url: str) -> bool:
    lowered = url.lower()
    if any(bad in lowered for bad in _NOISE_KEYWORDS):
        return True
    try:
        host = urlparse(url if "://" in url else "http://" + url).hostname or ""
    except Exception:
        host = ""
    host = host.lower()
    if not host:
        return True
    for bad in _NOISE_DOMAINS:
        if host == bad or host.endswith("." + bad):
            return True
    if lowered.endswith(_STATIC_EXT):
        return True
    # 排除邮箱地址（形如 user@domain 的裸域名匹配）
    return False


def _normalize(url: str) -> str:
    url = url.strip().strip(_TRAILING)
    if not url:
        return ""
    if "://" not in url:
        url = "http://" + url
    # 去掉后续路径里的尾部标点
    url = url.rstrip(_TRAILING)
    return url


def _score(url: str, text: str) -> int:
    score = 0
    lowered_url = url.lower()
    if lowered_url.startswith("https://"):
        score += 1
    # 裸域名（无路径）通常就是入口地址
    try:
        parsed = urlparse(url)
        if parsed.path in ("", "/"):
            score += 2
    except Exception:
        pass
    # 关键词加权：定位候选在原文中的位置，看附近是否有关键词
    idx = text.find(url)
    if idx == -1:
        # 去掉协议前缀再找一次
        idx = text.find(url.replace("http://", "").replace("https://", ""))
    if idx != -1:
        window = text[max(0, idx - 60) : idx + len(url) + 60]
        for kw in _BOOST_KEYWORDS:
            if kw in window:
                score += 3
                break
    return score


def extract_address(text: str, custom_regex: str | None = None) -> str | None:
    """从文本中提取最可能的站点地址。

    Args:
        text: 邮件正文（已去 HTML 标签的纯文本）。
        custom_regex: 可选自定义正则，若提供则优先使用。

    Returns:
        提取到的地址（带 scheme），失败返回 ``None``。
    """
    if not text:
        return None

    # 1) 自定义正则
    if custom_regex:
        try:
            m = re.search(custom_regex, text, re.IGNORECASE)
            if m:
                candidate = m.group(1) if m.groups() else m.group(0)
                candidate = _normalize(candidate)
                if candidate:
                    return candidate
        except re.error:
            pass  # 正则非法则回退内置规则

    # 2) 收集候选
    candidates: list[str] = []
    for raw in _URL_RE.findall(text):
        candidates.append(raw)
    for raw in _BARE_DOMAIN_RE.findall(text):
        candidates.append(raw)

    # 3) 归一化 + 过滤
    seen: set[str] = set()
    scored: list[tuple[int, int, str]] = []
    for order, raw in enumerate(candidates):
        url = _normalize(raw)
        if not url or url in seen:
            continue
        seen.add(url)
        if _is_noise(url):
            continue
        scored.append((_score(url, text), -order, url))

    if not scored:
        return None

    # 4) 取分最高（同分取出现更早的）
    scored.sort(reverse=True)
    return scored[0][2]
