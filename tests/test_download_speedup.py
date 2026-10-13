"""mailvideo 下载加速改动的集成验证：本地起 HTTP 服务实跑。

覆盖：
  A. 直链 + 服务端支持 Range  → 走多连接分段，文件必须与源文件字节一致
  B. 直链 + 服务端不支持 Range → 自动退回单连接，文件仍必须一致
  C. HLS 并行下载（纯合并）   → 产物可解码、时长正常
  D. HLS 并行下载 + 合并即压缩 → pre_encoded=True、体积明显变小、可解码
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

    rng_srv, rng_base = serve(RangeHandler)
    nor_srv, nor_base = serve(NoRangeHandler)
    print(f"Range 服务 {rng_base} / NoRange 服务 {nor_base}\n")

    # ---------------- A. 直链 + Range ----------------
    print("A. 直链多连接分段下载")
    out_a = WORK / "out_a"
    out_a.mkdir(exist_ok=True)
    f = VideoFetcher(direct_parallel_workers=4, direct_parallel_min_mb=1, timeout=10)
    calls = []
    orig_fetch = f._fetch_range

    def spy(*a, **k):
        calls.append(a[3])
        return orig_fetch(*a, **k)

    f._fetch_range = spy
    res = f._download_direct(f"{rng_base}/big.mp4", out_a)
    got = Path(res["path"])
    check("A1 走的是多连接分段", len(calls) >= 2, f"发起了 {len(calls)} 段")
    check("A2 分段后与源文件字节一致", md5(got) == md5(big))
    check("A3 体积与源一致", got.stat().st_size == big.stat().st_size,
          f"{got.stat().st_size} vs {big.stat().st_size}")

    # ---------------- B. 直链 + 不支持 Range ----------------
    print("\nB. 直链不支持 Range → 退回单连接")
    out_b = WORK / "out_b"
    out_b.mkdir(exist_ok=True)
    f2 = VideoFetcher(direct_parallel_workers=4, direct_parallel_min_mb=1, timeout=10)
    calls2 = []
    f2._fetch_range = lambda *a, **k: calls2.append(1)
    res2 = f2._download_direct(f"{nor_base}/big.mp4", out_b)
    got2 = Path(res2["path"])
    check("B1 未走分段（自动降级）", len(calls2) == 0)
    check("B2 单连接下载文件正确", md5(got2) == md5(big))

    # ---------------- C. HLS 纯合并 ----------------
    print("\nC. HLS 并行下载（纯合并）")
    out_c = WORK / "out_c"
    out_c.mkdir(exist_ok=True)
    f3 = VideoFetcher(hls_parallel_workers=4, timeout=10)
    res3 = f3._download_hls_parallel(f"{rng_base}/index.m3u8", out_c, title="c")
    p3 = Path(res3["path"])
    check("C1 产物存在且非空", p3.exists() and p3.stat().st_size > 0,
          f"{p3.stat().st_size/1024/1024:.2f}MB")
    check("C2 pre_encoded=False", res3.get("pre_encoded") is False)
    check("C3 时长正常(~20s)", abs(probe_duration(p3) - 20) < 1.5,
          f"{probe_duration(p3):.2f}s")
    check("C4 分片目录已清理",
          not any(out_c.glob("_segs_*")), "临时分片已删")

    # ---------------- D. HLS 合并即压缩 ----------------
    print("\nD. HLS 合并+压缩一次完成")
    out_d = WORK / "out_d"
    out_d.mkdir(exist_ok=True)
    f4 = VideoFetcher(hls_parallel_workers=4, timeout=10)
    res4 = f4._download_hls_parallel(
        f"{rng_base}/index.m3u8", out_d, title="d",
        encode_opts={"target_height": 360, "crf": 32, "preset": "ultrafast",
                     "target_bitrate": "", "audio_bitrate": "64k"},
    )
    p4 = Path(res4["path"])
    check("D1 pre_encoded=True", res4.get("pre_encoded") is True)
    check("D2 产物可解码且时长正常", abs(probe_duration(p4) - 20) < 1.5,
          f"{probe_duration(p4):.2f}s")
    check("D3 画面已按目标高度缩放",
          _height(p4) == 360, f"实际高度 {_height(p4)}")
    check("D4 没有留下中转 mp4（只应有压缩产物）",
          len(list(out_d.glob("*.mp4"))) == 1,
          f"目录内 mp4 数 = {len(list(out_d.glob('*.mp4')))}")

    # ---------------- E. 编码失败要能回退 ----------------
    print("\nE. 压缩参数非法时回退纯合并")
    out_e = WORK / "out_e"
    out_e.mkdir(exist_ok=True)
    f5 = VideoFetcher(hls_parallel_workers=4, timeout=10)
    res5 = f5._download_hls_parallel(
        f"{rng_base}/index.m3u8", out_e, title="e",
        encode_opts={"target_height": 360, "crf": "这不是数字",
                     "preset": "totally-bogus-preset", "target_bitrate": ""},
    )
    p5 = Path(res5["path"])
    check("E1 回退后仍拿到可用文件",
          p5.exists() and probe_duration(p5) > 15, f"{p5.stat().st_size/1024/1024:.2f}MB")
    check("E2 pre_encoded 标记为 False（回退了）", res5.get("pre_encoded") is False)

    rng_srv.shutdown()
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
