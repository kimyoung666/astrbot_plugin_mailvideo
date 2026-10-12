# astrbot_plugin_mailvideo · 邮箱取址搜片神器

功能简介：**登录邮箱（默认 QQ 邮箱）→ 发信 → 从回信正文提取站点地址 → 在该站按关键词搜索视频 → 把视频文件回传给用户**。

> 适用于「站点域名经常更换、通过邮件自动回复最新地址」这类场景：域名不写死在插件里，而是每次从邮箱回信中动态获取，站点换域名无需改代码。

---

## 功能流程

```
用户: /搜片 关键词
  │
  ├─ ① SMTP 向配置的收件邮箱发送一封邮件（主题/正文可配，正文即“任意消息”）
  ├─ ② IMAP 轮询收件箱，匹配到回信后，从正文中提取站点地址（自动过滤邮箱服务商/退订等噪声）
  ├─ ③ 用该地址作为站点入口，按关键词搜索视频（yt-dlp 通用解析，兼容 mp4 直链 / m3u8 / 播放页）
  └─ ④ 下载视频文件，直接发回当前会话
```

## 指令

| 指令 | 别名 | 说明 |
|---|---|---|
| `/搜片 <关键词>` | `/搜片` | 主流程：发信 → 取地址 → 搜索 → 下载 → 回传 |
| `/搜片地址` | `/取地址` | 只跑邮箱环节，返回并缓存当前站点地址 |
| `/搜片邮箱` | `/邮箱测试` | 测试 IMAP/SMTP 连通性与授权码是否正确 |
| `/搜片帮助` | — | 显示用法与当前缓存地址 |

## 安装

1. 将 `astrbot_plugin_mailvideo` 整个目录放入 AstrBot 的 `data/plugins/` 下；
2. 在 AstrBot WebUI 的「插件」页重载插件，或在插件页点击刷新；
3. 若提示缺少依赖，插件会自动按 `requirements.txt` 安装（`yt-dlp` / `httpx` / `beautifulsoup4`）。

> 依赖 `ffmpeg`（用于 m3u8 合并、音视频合流）。请确保运行 AstrBot 的机器已安装并能在 PATH 中找到 `ffmpeg`，或在配置里填写 `ffmpeg_path`。

## 配置（WebUI → 插件 → 配置）

### 邮箱（必填）

| 配置项 | 说明 |
|---|---|
| `email_account` | 登录/发件邮箱地址，如 `123456@qq.com` |
| `email_auth_code` | **授权码**（不是登录密码），见下方获取方法 |
| `imap_server` / `imap_port` | 默认 `imap.qq.com` / `993` |
| `smtp_server` / `smtp_port` | 默认 `smtp.qq.com` / `465` |

### 收发信

| 配置项 | 默认 | 说明 |
|---|---|---|
| `recipient_email` | — | 收件邮箱（“用户设置的邮箱”）。插件向它发信并等待其回信 |
| `mail_subject` | `获取地址` | 发信主题。若对方是「关键词自动回复」服务，填其要求的触发词 |
| `mail_body` | `获取最新地址` | 发信正文 |
| `reply_wait_timeout` | `120` | 等待回信超时（秒） |
| `reply_poll_interval` | `5` | 轮询间隔（秒） |

### 地址提取

| 配置项 | 默认 | 说明 |
|---|---|---|
| `address_regex` | 空 | 自定义提取正则（含捕获组时取第 1 组），留空用内置规则 |
| `use_cache_on_fail` | `true` | 提取失败时回退上次成功地址 |

### 搜索与下载

| 配置项 | 默认 | 说明 |
|---|---|---|
| `search_url_template` | `{base}/search?wd={keyword}` | 站点搜索 URL 模板，支持 `{base}`（邮件地址）、`{keyword}`（URL 编码后的关键词）与 `{raw_keyword}`（未编码关键词）占位符 |
| `max_results` | `1` | 最多回传几个视频 |
| `max_download_attempts` | `0` | 最多尝试下载的候选数；`0` = 自动（回传数 + 1），调大可提高成功率 |
| `cookie` | 空 | 目标站点 Cookie（部分站需要） |
| `http_proxy` | 空 | 代理，如 `http://127.0.0.1:7890` / `socks5://127.0.0.1:1080` |
| `ffmpeg_path` | 空 | 自定义 ffmpeg 路径 |

### 画质压缩（可选）

下载完成后、发送之前对视频转码压缩，便于绕过平台的文件大小限制。

| 配置项 | 默认 | 说明 |
|---|---|---|
| `compress_enable` | `false` | 是否启用画质压缩。注意压缩发生在下载之后，不会减少下载耗时 |
| `compress_threshold_mb` | `100` | 仅当文件大于该值才压缩；`0` = 所有视频都压 |
| `compress_target_height` | `720` | 目标分辨率高度；`0` = 不缩放分辨率，仅靠 CRF 降码率 |
| `compress_crf` | `30` | 质量因子，越大越糊越小（x264 常用 18~32）；仅在未填目标码率时生效 |
| `compress_target_bitrate` | 空 | 目标视频码率（如 `800k`）；填了则按固定码率压缩，体积可预估 |
| `compress_preset` | `veryfast` | libx264 编码预设，越快越省时间、压缩率越低 |
| `compress_audio_bitrate` | `64k` | 音频统一转 AAC，`64k` 够用 |

### 回传

| 配置项 | 默认 | 说明 |
|---|---|---|
| `send_as` | `video` | `video` 用视频消息发送；`file` 用文件消息发送 |
| `max_video_size_mb` | `1024` | 超过该大小不再直接发文件，改为返回直链文本 |
| `allowed_users` | `[]` | 允许使用的用户 ID 白名单，留空 = 所有人 |

## QQ 邮箱：如何获取授权码

1. 登录 [QQ 邮箱网页版](https://mail.qq.com/) → 顶部「设置」旁边→「账号与安全」；
2. 找到 **POP3/IMAP/SMTP/Exchange/CardDAV/CalDAV 服务**；
3. 开启 **IMAP/SMTP 服务**（需要短信验证）；
4. 按提示生成 **授权码**（一串字母），填入插件配置的 `email_auth_code`。

> 其它邮箱同理：开启 IMAP/SMTP，使用其提供的授权码，并把 `imap_server` / `smtp_server` 改成对应服务商地址。

## 常见问题

**Q：提示授权码错误 / 登录失败？**
- 确认用的是「授权码」而非 QQ 密码；确认已在网页版开启 IMAP/SMTP；
- 可先用 `/搜片邮箱` 测试连接。

**Q：一直等不到回信？**
- 确认 `recipient_email` 会回信（可先手动发一封测试）；
- 适当调大 `reply_wait_timeout`；确认 `mail_subject` 是对方要求的触发词。

**Q：搜不到视频 / 下载失败？**
- 该站可能不被 yt-dlp 支持，或搜索 URL 模板不对。请据该站实际搜索地址调整 `search_url_template`；
- 需要登录/风控的站，可填 `cookie`；
- 境外站点可尝试配置 `http_proxy`。

**Q：视频发不出来？**
- 多数平台视频消息有大小限制（QQ 通常 ≤100MB）。超过 `max_video_size_mb` 时会自动改为返回直链；
- `send_as=file` 可改走文件消息（部分平台不支持）。

## 开发说明

```
astrbot_plugin_mailvideo/
├── metadata.yaml          # 插件元数据
├── main.py                # 插件入口：指令、异步任务编排
├── _conf_schema.json      # WebUI 配置 Schema
├── requirements.txt       # 依赖
├── core/
│   ├── __init__.py        # 内部模块说明
│   ├── state_store.py     # 持久化（最近地址、已处理邮件 UID），存于插件数据目录
│   ├── mail_client.py     # IMAP 收信 + SMTP 发信 + 回信匹配
│   ├── address_extractor.py # 地址提取（噪声过滤 + 打分）
│   └── video_fetcher.py   # yt-dlp 搜索 + 下载（m3u8/播放页兼容与降级）
└── tests/selftest.py      # 自检测试（无需 AstrBot 运行时）
```

自测：

```bash
python tests/selftest.py
```

## 合规声明

本插件为通用技术实现，不针对任何特定站点或内容类型。请确保使用行为符合当地法律法规以及目标网站的服务条款；因使用本插件产生的任何后果由使用者自行承担。
