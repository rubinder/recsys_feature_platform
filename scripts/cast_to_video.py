"""Render an asciicast (terminal recording) to MP4: play it in asciinema-player, record with Playwright.

    uv run --group docs python scripts/cast_to_video.py docs/eks-session/session.cast docs/eks-session/eks-session-terminal.mp4
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

PLAYER = "https://cdn.jsdelivr.net/npm/asciinema-player@3.8.0/dist/bundle/asciinema-player"
SPEED = 4


def duration(cast: Path, idle: float) -> float:
    t_prev, total = 0.0, 0.0
    for line in cast.read_text().splitlines()[1:]:
        t = json.loads(line)[0]
        total += min(t - t_prev, idle)
        t_prev = t
    return total


def main(cast_path: str, out_path: str) -> None:
    cast, out = Path(cast_path).resolve(), Path(out_path).resolve()
    header = json.loads(cast.read_text().splitlines()[0])
    idle = header.get("idle_time_limit", 1.5)
    seconds = duration(cast, idle) / SPEED + 3
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        (tmp / "session.cast").write_text(cast.read_text())
        (tmp / "index.html").write_text(f"""<!doctype html><html><head><meta charset="utf-8">
<link rel="stylesheet" href="{PLAYER}.css">
<style>body{{margin:0;background:#0f161d}} #p{{width:1280px}}</style></head><body><div id="p"></div>
<script src="{PLAYER}.min.js"></script>
<script>AsciinemaPlayer.create('session.cast', document.getElementById('p'),
 {{autoPlay:true, speed:{SPEED}, idleTimeLimit:{idle}, cols:{header['width']}, rows:{header['height']},
   fit:'width', terminalFontSize:'15px', theme:'monokai', controls:false}});</script></body></html>""")
        server = subprocess.Popen([sys.executable, "-m", "http.server", "8765", "--bind", "127.0.0.1"], cwd=tmp,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            time.sleep(1)
            with sync_playwright() as p:
                b = p.chromium.launch()
                ctx = b.new_context(viewport={"width": 1280, "height": 760}, record_video_dir=str(tmp / "v"),
                                    record_video_size={"width": 1280, "height": 760})
                page = ctx.new_page()
                page.goto("http://127.0.0.1:8765/index.html")
                page.wait_for_timeout(int(seconds * 1000))
                page.screenshot(path=str(out.with_suffix(".png")))
                ctx.close()
                b.close()
            webm = next((tmp / "v").glob("*.webm"))
            if not shutil.which("ffmpeg"):
                shutil.copy(webm, out.with_suffix(".webm"))
                return
            subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(webm), "-vf", "fps=24,format=yuv420p",
                            "-movflags", "+faststart", str(out)], check=True)
        finally:
            server.terminate()
    print(f"wrote {out} ({seconds:.0f}s of video at {SPEED}x)")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
