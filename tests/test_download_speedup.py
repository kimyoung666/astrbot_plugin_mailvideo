"""mailvideo 下载加速改动的集成验证：本地起 HTTP 服务实跑。

覆盖：
  A. 直链 mp4 → 交给 yt-dlp 兜底，文件必须与源文件字节一致
  C. HLS 并行下载（纯合并）   → 产物可解码、时长正常
"""

from __future__ import annotations

import hashlib
import importlib.util
import os
import subprocess
import sys
import tempfile
import threading
import types
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]
WORK = Path(tempfile.gettempdir()) / "mailvideo_test" / "work"
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
else:
    class _Quiet:
        def info(self, m):
            print(f"    [LOG] {m}", flush=True)

        def warning(self, m):
            print(f"    [WRN] {m}", flush=True)

        def error(self, m):
            print(f"    [ERR] {m}", flush=True)

        def debug(self, m):
            pass

    _real_api.logger = _Quiet()

spec = importlib.util.spec_from_file_location(
    "mv_fetcher", PLUGIN_DIR / "core" / "video_fetcher.py"
)
vf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vf)
VideoFetcher = vf.VideoFetcher


# --------------------------------------------------------------------------- #
# 本地 HTTP 服务
# --------------------------------------------------------------------------- #
class RangeHandler(BaseHTTPRequestHandler):
    """支持 Range 的静态文件服务。"""

    root = WORK

    def log_message(self, *a):
        pass

    def do_GET(self):
        p = self.root / self.path.lstrip("/").split("?")[0]
        if not p.is_file():
            self.send_error(404)
            return
        data = p.read_bytes()
        total = len(data)
        rng = self.headers.get("Range")
        if rng and rng.startswith("bytes="):
            s, _, e = rng[6:].partition("-")
            start = int(s)
            end = int(e) if e else total - 1
            end = min(end, total - 1)
            chunk = data[start:end + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{total}")
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header(
                "Content-Type",
                "application/vnd.apple.mpegurl" if p.suffix == ".m3u8" else "video/mp2t"
                if p.suffix == ".ts" else "video/mp4",
            )
            self.end_headers()
            self.wfile.write(chunk)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(total))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header(
            "Content-Type",
            "application/vnd.apple.mpegurl" if p.suffix == ".m3u8" else "video/mp2t"
            if p.suffix == ".ts" else "video/mp4",
        )
        self.end_headers()
        self.wfile.write(data)


class NoRangeHandler(RangeHandler):
    """忽略 Range 的服务（永远返回 200 全量）。"""

    def do_GET(self):
        p = self.root / self.path.lstrip("/").split("?")[0]
        if not p.is_file():
            self.send_error(404)
            return
        data = p.read_bytes()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.send_header(
            "Content-Type",
            "application/vnd.apple.mpegurl" if p.suffix == ".m3u8" else "video/mp2t"
            if p.suffix == ".ts" else "video/mp4",
        )
        self.end_headers()
        self.wfile.write(data)


def serve(handler):
    handler.root = WORK
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


# --------------------------------------------------------------------------- #
# 造测试素材
# --------------------------------------------------------------------------- #
def build_media():
    src = WORK / "source.mp4"
    if not src.exists():
        subprocess.run(
            [
                "ffmpeg", "-y", "-v", "error",
                "-f", "lavfi", "-i", "testsrc2=size=640x360:rate=25",
                "-f", "lavfi", "-i", "sine=frequency=440",
                "-t", "20",
                "-c:v", "libx264", "-preset", "ultrafast", "-crf", "18",
                "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "128k",
                str(src),
            ],
            check=True,
        )
    # 造一个够大的"直链"文件用于分段测试：把 source 复制拼到 12MB 左右
    big = WORK / "big.mp4"
    if not big.exists():
        blob = src.read_bytes()
        with open(big, "wb") as f:
            f.write(blob * (12 * 1024 * 1024 // len(blob) + 1))
    # HLS 切片
    if not (WORK / "index.m3u8").exists():
        subprocess.run(
            [
                "ffmpeg", "-y", "-v", "error", "-i", str(src),
                "-c", "copy", "-f", "hls", "-hls_time", "2",
                "-hls_list_size", "0",
                "-hls_segment_filename", str(WORK / "seg_%03d.ts"),
                str(WORK / "index.m3u8"),
            ],
            check=True,
        )
    return src, big


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


RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, cond))
    print(f"  {'✅' if cond else '❌'} {name}" + (f"  {detail}" if detail else ""), flush=True)


def main():
    src, big = build_media()
    print(f"素材: source={src.stat().st_size/1024/1024:.2f}MB  big={big.stat().st_size/1024/1024:.2f}MB")

    nor_srv, nor_base = serve(NoRangeHandler)
    print(f"直链服务 {nor_base}\n")

    # ---------------- A. 直链 mp4 走 yt-dlp ----------------
    print("A. 直链 mp4（yt-dlp 兜底）")
    out_a = WORK / "out_a"
    out_a.mkdir(exist_ok=True)
    f = VideoFetcher(timeout=10)
    res = f.download(f"{nor_base}/big.mp4", out_a, title="direct")
    got = Path(res["path"])
    check("A1 产物存在", got.exists() and got.stat().st_size > 0, f"{got.stat().st_size}B")
    check("A2 与源文件字节一致", md5(got) == md5(big))

    # ---------------- C. HLS 纯合并 ----------------
    print("\nC. HLS 并行下载（纯合并）")
    out_c = WORK / "out_c"
    out_c.mkdir(exist_ok=True)
    f3 = VideoFetcher(hls_parallel_workers=4, timeout=10)
    res3 = f3._download_hls_parallel(f"{nor_base}/index.m3u8", out_c, title="c")
    p3 = Path(res3["path"])
    check("C1 产物存在且非空", p3.exists() and p3.stat().st_size > 0,
          f"{p3.stat().st_size/1024/1024:.2f}MB")
    check("C3 时长正常(~20s)", abs(probe_duration(p3) - 20) < 1.5,
          f"{probe_duration(p3):.2f}s")

    # ---------------- D. 分片目录与临时文件清理 ----------------
    print("\nD. 下载产物不留临时文件")
    check("D1 分片目录已清理", not any(out_c.glob("_segs_*")))
    check("D2 无加密密钥残留", not any(out_c.glob("_key_*")))
    check("D3 只留一个成品文件", len(list(out_c.glob("*.mp4"))) == 1,
          f"目录内 mp4 数 = {len(list(out_c.glob('*.mp4')))}")

    nor_srv.shutdown()

    print("\n" + "=" * 52)
    bad = [n for n, ok in RESULTS if not ok]
    print(f"通过 {len(RESULTS) - len(bad)}/{len(RESULTS)}")
    if bad:
        print("失败项: " + "; ".join(bad))
    return 1 if bad else 0


def _height(p: Path) -> int:
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=height", "-of", "default=nw=1:nk=1", str(p)],
        capture_output=True, text=True,
    ).stdout.strip()
    return int(out or 0)


if __name__ == "__main__":
    sys.exit(main())
