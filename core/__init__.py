"""邮箱取址视频搜索插件 - 内部逻辑模块。

拆分为：
- state_store: 持久化（最近地址、已处理 UID 等）
- mail_client: IMAP 收信 + SMTP 发信 + 回信匹配
- address_extractor: 从邮件正文提取站点地址
- video_fetcher: yt-dlp 搜索与下载
"""

__all__ = [
    "state_store",
    "mail_client",
    "address_extractor",
    "video_fetcher",
]
