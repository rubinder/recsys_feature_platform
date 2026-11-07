"""Screen captures of the live EKS endpoint while scripts/eks_session.py holds it up.

Waits for docs/eks-session/endpoint.txt, then with Playwright:
  - screenshots the endpoint's /health and a /recommendations response
  - runs Locust (web UI) against the load balancer for ~70 s and records a video
    of its live charts, plus screenshots of the statistics and charts tabs
Converts the videos to MP4 with ffmpeg and writes captures.done so the session
can tear down immediately.

Note: these requests travel from this machine over the internet to us-east-1, so
their latency includes the network round trip; the SLO measurement is the
in-cluster load test in the session log.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "eks-session"
RAW = OUT / "video-raw"


def pretty_json_page(url: str) -> str:
    import httpx

    body = httpx.get(url, timeout=15).json()
    text = json.dumps(body, indent=2)
    if len(text) > 4000:
        text = text[:4000] + "\n..."
    esc = text.replace("&", "&amp;").replace("<", "&lt;")
    return (f"<html><body style='font:14px ui-monospace,Menlo,monospace;background:#0f161d;color:#e4ebf1;padding:24px'>"
            f"<div style='color:#5cbccc;margin-bottom:12px'>GET {url.split('.com', 1)[-1] or '/'} &nbsp;·&nbsp; EKS load balancer"
            f" (us-east-1)</div><pre style='white-space:pre-wrap'>{esc}</pre></body></html>")


def main() -> None:
    ep_file = OUT / "endpoint.txt"
    for _ in range(720):
        if ep_file.exists():
            break
        time.sleep(5)
    base = ep_file.read_text().strip()
    RAW.mkdir(parents=True, exist_ok=True)
    locust = subprocess.Popen(
        ["uv", "run", "--group", "loadtest", "locust", "-f", "loadtest/locustfile.py", "--host", base,
         "--web-host", "127.0.0.1", "--web-port", "8089", "-u", "12", "-r", "3", "--autostart", "--run-time", "75s"],
        cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    try:
        with sync_playwright() as p:
            b = p.chromium.launch()
            ctx = b.new_context(viewport={"width": 1280, "height": 760}, record_video_dir=str(RAW),
                                record_video_size={"width": 1280, "height": 760})
            page = ctx.new_page()
            for name, path in (("eks-health", "/health"), ("eks-recommendations", "/recommendations/6040?log_impression=false")):
                page.set_content(pretty_json_page(base + path))
                page.wait_for_timeout(2500)
                page.screenshot(path=str(OUT / f"{name}.png"))
            for _ in range(60):
                try:
                    page.goto("http://127.0.0.1:8089", timeout=3000)
                    break
                except Exception:
                    time.sleep(1)
            page.wait_for_timeout(4000)
            charts = page.get_by_role("tab", name="Charts")
            if charts.count():
                charts.first.click()
            page.wait_for_timeout(55000)
            page.screenshot(path=str(OUT / "eks-locust-charts.png"))
            stats = page.get_by_role("tab", name="Statistics")
            if stats.count():
                stats.first.click()
                page.wait_for_timeout(3000)
                page.screenshot(path=str(OUT / "eks-locust-statistics.png"))
            ctx.close()
            b.close()
    finally:
        locust.terminate()
        (OUT / "captures.done").write_text("ok\n")
    vids = sorted(RAW.glob("*.webm"))
    if vids and shutil.which("ffmpeg"):
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(vids[-1]), "-vf", "fps=24,format=yuv420p",
                        "-movflags", "+faststart", str(OUT / "eks-live-loadtest.mp4")], check=True)
    print("captured:", sorted(p.name for p in OUT.glob("eks-*")))


if __name__ == "__main__":
    sys.exit(main())
