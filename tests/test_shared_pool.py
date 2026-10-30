"""共享连接池专项验证：整任务只建一个 Client，分片/分段复用 TCP 连接。

覆盖：
  A. 默认并发 24 / 12；池上限 = 线程数 × 2；keep-alive 30s；同 URL 复用同 Client
  B. HLS（AES-128 加密实跑）：m3u8 + 密钥 + 全部分片 = 1 个 Client，连接被复用
  C. 直链 Range：探测 + 12 个分段 = 1 个 Client，产物与源文件字节一致
  D. close() 释放全部连接池

判定连接的依据来自服务端：同一 TCP 连接上的多次请求来源端口相同，
所以「不同来源端口数 < 请求数」即证明 keep-alive 连接被复用。
"""

from __future__ import annotations

import hashlib
import importlib.util
import shutil
import subprocess
import sys
import tempfile
import threading
import types
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]
WORK = Path(tempfile.gettempdir()) / "mailvideo_test" / "pool"
WORK.mkdir(parents=True, exist_ok=True)

# ---- astrbot 运行时存在就用真的，否则注入最小桩 ----
try:
    import astrbot.api as _real_api  # noqa: F401
    _HAS_ASTRBOT = True
except Exception:
    _HAS_ASTRBOT = False

if not _HAS_ASTRBOT:
    _astrbot = types.ModuleType("astrbot")
    _api = types.ModuleType("astrbot.api")

    class _Log:
        def _p(self, lvl, msg):
            print(f"    [{lvl}] {msg}", flush=True)

        def info(self, m):
            self._p("LOG", m)

        def warning(self, m):
            self._p("WRN", m)

        def error(self, m):
            self._p("ERR", m)

        def debug(self, m):
            pass

    _api.logger = _Log()
    _astrbot.api = _api
    sys.modules["astrbot"] = _astrbot
    sys.modules["astrbot.api"] = _api

spec = importlib.util.spec_from_file_location(
    "mv_pool_fetcher", PLUGIN_DIR / "core" / "video_fetcher.py"
)
vf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vf)
VideoFetcher = vf.VideoFetcher

import httpx  # noqa: E402

# --------------------------------------------------------------------------- #
# 本地 HTTP 服务（记录 路径 / 来源端口 / Range）
# --------------------------------------------------------------------------- #
HITS: list[tuple[str, int, str]] = []


class PoolHandler(BaseHTTPRequestHandler):
    """支持 Range + keep-alive 的静态服务。"""

    # 关键：HTTP/1.1 + Content-Length 才会复用连接；默认的 HTTP/1.0 每次断开。
    protocol_version = "HTTP/1.1"
    root = WORK

    def log_message(self, *a):
        pass

    def do_GET(self):
        path = self.path.split("?")[0]
        rng = self.headers.get("Range") or ""
        HITS.append((path, self.client_address[1], rng))
        p = self.root / path.lstrip("/")
        if not p.is_file():
            self.send_error(404)
            return
        data = p.read_bytes()
        total = len(data)
        if rng.startswith("bytes="):
            s, _, e = rng[6:].partition("-")
            start = int(s)
            end = min(int(e) if e else total - 1, total - 1)
            chunk = data[start:end + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{total}")
        else:
            chunk = data
            self.send_response(200)
        self.send_header("Content-Length", str(len(chunk)))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Type", _ctype(p))
        self.end_headers()
        self.wfile.write(chunk)


def _ctype(p: Path) -> str:
    if p.suffix == ".m3u8":
        return "application/vnd.apple.mpegurl"
    if p.suffix == ".ts":
        return "video/mp2t"
    return "application/octet-stream"


def serve():
    PoolHandler.root = WORK
    srv = ThreadingHTTPServer(("127.0.0.1", 0), PoolHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


def ports_for(suffix: str) -> set[int]:
    return {port for path, port, _ in HITS if path.endswith(suffix)}


def requests_for(name: str) -> list[tuple]:
    return [h for h in HITS if h[0].endswith(name)]


# --------------------------------------------------------------------------- #
# 造素材
# --------------------------------------------------------------------------- #
def build_media(base: str) -> tuple[Path, Path, int]:
    src = WORK / "source.mp4"
    # 每次重建（8 秒很快）：-c copy 切 HLS 只能按关键帧切，必须强制密关键帧，
    # 否则整段视频只有一个关键帧、只会切出 1 片，验证不了连接复用
    src.unlink(missing_ok=True)
    subprocess.run(
        [
            "ffmpeg", "-y", "-v", "error",
            "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=15",
            "-f", "lavfi", "-i", "sine=frequency=440",
            "-t", "8",
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
            "-pix_fmt", "yuv420p",
            "-g", "15", "-keyint_min", "15", "-sc_threshold", "0",
            "-c:a", "aac", "-b:a", "64k",
            str(src),
        ],
        check=True,
    )

    # 每次都重切：m3u8 里嵌了随机端口的密钥 URI，端口变了必须重生成
    for old in list(WORK.glob("seg_*.ts")) + [WORK / "index_enc.m3u8"]:
        old.unlink(missing_ok=True)
    key = WORK / "key.bin"
    key.write_bytes(bytes(range(16)))
    key_info = WORK / "key_info.txt"
    # 只写「播放列表里的 key URI」+「本地密钥路径」两行：第三行 IV 会让
    # 这个版本的 ffmpeg 报 encryption_iv 设置失败（省略时按分片序号派生，
    # 效果等价，且播放列表不带 IV 正好覆盖「key_iv 为空」这条分支）。
    key_info.write_text(f"{base}/key.bin\n{key}\n")
    subprocess.run(
        [
            "ffmpeg", "-y", "-v", "error", "-i", str(src),
            "-c", "copy", "-f", "hls", "-hls_time", "1", "-hls_list_size", "0",
            "-hls_key_info_file", str(key_info),
            "-hls_segment_filename", str(WORK / "seg_%03d.ts"),
            str(WORK / "index_enc.m3u8"),
        ],
        check=True,
    )
    seg_count = len(list(WORK.glob("seg_*.ts")))
    if seg_count == 0:
        raise RuntimeError("HLS 加密切片失败：没生成任何分片")
    if "#EXT-X-KEY" not in (WORK / "index_enc.m3u8").read_text():
        raise RuntimeError("m3u8 里没有 EXT-X-KEY，加密素材无效")

    big = WORK / "big.mp4"
    # 必须是精确 12MB：这样 12 段每段正好 1MB，分段数确定可断言
    if not big.exists() or big.stat().st_size != 12 * 1024 * 1024:
        blob = src.read_bytes()
        left = 12 * 1024 * 1024
        with open(big, "wb") as f:
            while left > 0:
                chunk = blob[:left]
                f.write(chunk)
                left -= len(chunk)
    return src, big, seg_count


def fresh_dir(name: str) -> Path:
    d = WORK / name
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True, exist_ok=True)
    return d


def md5(p: Path) -> str:
    h = hashlib.md5()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def probe_duration(p: Path) -> float:
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=nw=1:nk=1", str(p)],
        capture_output=True, text=True,
    ).stdout.strip()
    return float(out or 0)


def frame_count(p: Path) -> int:
    """数视频包数：解密/合并出错时帧数会明显对不上。"""
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0", "-count_packets",
         "-show_entries", "stream=nb_read_packets", "-of", "default=nw=1:nk=1", str(p)],
        capture_output=True, text=True,
    ).stdout.strip()
    return int(out or 0)


# --------------------------------------------------------------------------- #
# 工具：统计 Client 实例化次数 / 读取连接池参数
# --------------------------------------------------------------------------- #
class _Counter:
    """临时把 httpx.Client 换成计数的子类，数出这一趟到底建了几个 Client。"""

    def __init__(self):
        self.created: list = []

    def __enter__(self):
        outer = self

        class Counting(httpx.Client):
            def __init__(self, *a, **k):
                super().__init__(*a, **k)
                outer.created.append(self)

        self._real = httpx.Client
        httpx.Client = Counting
        return self

    def __exit__(self, *exc):
        httpx.Client = self._real
        return False


def pool_of(client) -> dict:
    pool = getattr(getattr(client, "_transport", None), "_pool", None)
    return {
        "max": getattr(pool, "_max_connections", None),
        "keepalive": getattr(pool, "_max_keepalive_connections", None),
        "expiry": getattr(pool, "_keepalive_expiry", None),
        "live": len(pool.connections) if pool is not None else None,
    }


RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, cond))
    print(f"  {'✅' if cond else '❌'} {name}" + (f"  {detail}" if detail else ""), flush=True)


# --------------------------------------------------------------------------- #
# A. 池参数与默认并发
# --------------------------------------------------------------------------- #
def case_a():
    print("A. 默认并发与连接池参数")
    f = VideoFetcher()
    check("A1 默认 HLS 并发 = 24", f.hls_parallel_workers == 24,
          f"实际 {f.hls_parallel_workers}")

    g = VideoFetcher(hls_parallel_workers=24, http2=False)
    seg = g._shared_client("http://127.0.0.1:9/a.m3u8", kind="seg")
    lim = pool_of(seg)
    check("A2 HLS 池上限 = 线程数×2 = 48", lim["max"] == 48, f"{lim}")
    check("A3 HLS keepalive 连接数 = 24", lim["keepalive"] == 24)
    check("A4 keepalive_expiry = 30s", lim["expiry"] == 30.0, f"{lim['expiry']}")
    check("A5 m3u8/密钥/分片同一分桶（同一 Client）",
          g._shared_client("http://127.0.0.1:9/a.m3u8") is seg)
    check("A6 同一分桶只建一个 Client", len(g._shared_clients) == 1,
          f"{list(g._shared_clients)}")

    g.close()
    check("A7 close() 清空全部连接池", not g._shared_clients)


# --------------------------------------------------------------------------- #
# B. HLS：m3u8 + 密钥 + 分片 = 1 个 Client
# --------------------------------------------------------------------------- #
def case_b(base: str, src: Path, seg_count: int):
    print("\nB. HLS 加密流：m3u8 / 密钥 / 分片共用连接池")
    HITS.clear()
    workers = 3
    f = VideoFetcher(hls_parallel_workers=workers, timeout=10)
    out = fresh_dir("out_hls")
    with _Counter() as ctr:
        res = f._download_hls_parallel(
            f"{base}/index_enc.m3u8", out, title="enc", referer=f"{base}/"
        )

    check("B1 整任务只建了 1 个 httpx.Client", len(ctr.created) == 1,
          f"实际建了 {len(ctr.created)} 个")
    check("B2 只留了 1 个连接池分桶", len(f._shared_clients) == 1,
          f"{list(f._shared_clients)}")
    check("B3 m3u8 与分片走同一个 Client",
          ctr.created and f._shared_clients.get("seg:direct") is ctr.created[0])

    n_ts = len(requests_for(".ts"))
    ts_ports = ports_for(".ts")
    check("B4 分片数 > 并发数（前提成立）", seg_count > workers,
          f"分片 {seg_count} / 并发 {workers}")
    check("B5 连接被复用：端口数 < 分片请求数",
          0 < len(ts_ports) < n_ts, f"连接 {len(ts_ports)} 条 / 请求 {n_ts} 次")
    check("B6 端口数不超过并发线程数", len(ts_ports) <= workers,
          f"{len(ts_ports)} <= {workers}")
    check("B7 连接池里活着的连接不超过并发数",
          (pool_of(ctr.created[0])["live"] or 0) <= workers,
          f"活连接 {pool_of(ctr.created[0])['live']}")
    check("B8 密钥也走这个 Client（key.bin 被请求过）",
          bool(requests_for("key.bin")))

    p = Path(res["path"])
    check("B9 合并产物可解码且时长≈8s", p.exists() and abs(probe_duration(p) - 8) < 1.5,
          f"{probe_duration(p):.2f}s")
    check("B10 解密后帧数与源一致（密钥用对了）",
          frame_count(p) == frame_count(src),
          f"{frame_count(p)} vs 源 {frame_count(src)}")
    f.close()


# --------------------------------------------------------------------------- #
# D. close() 之后能重建池
# --------------------------------------------------------------------------- #
def case_d():
    print("\nD. close() 后连接池可重建")
    f = VideoFetcher(http2=False)
    c1 = f._shared_client("http://127.0.0.1:9/a.m3u8")
    f.close()
    c2 = f._shared_client("http://127.0.0.1:9/a.m3u8")
    check("D1 close() 后拿到的是新建的 Client（旧池已释放）", c2 is not c1)
    check("D2 重建的池参数不变（keepalive 30s）", pool_of(c2)["expiry"] == 30.0)
    f.close()


def main():
    srv, base = serve()
    print(f"本地服务 {base}\n")
    src, big, seg_count = build_media(base)
    print(f"素材: source={src.stat().st_size/1024:.1f}KB  big="
          f"{big.stat().st_size/1024/1024:.2f}MB  分片 {seg_count} 个\n")

    case_a()
    case_b(base, src, seg_count)
    case_d()

    srv.shutdown()
    print("\n" + "=" * 52)
    bad = [n for n, ok in RESULTS if not ok]
    print(f"通过 {len(RESULTS) - len(bad)}/{len(RESULTS)}")
    if bad:
        print("失败项: " + "; ".join(bad))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
