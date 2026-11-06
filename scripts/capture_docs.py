"""Reproducible doc artifacts for docs/img/ (run with `make docs-capture PHASE=N`).

Phase 1 (needs `make serve`): MLflow run screenshots, sample API response, demo
output, latency report, parity check.
Phase 2 (needs `make demo-streaming` to have run, stack up): Flink job graph
screenshot, metrics report, streaming parity, latency with and without the
item-feature cache, and the live freshness demo output.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import httpx

from recsys.config import ARTIFACTS, MLRUNS, ROOT

IMG = ROOT / "docs" / "img"
PHASE = "phase" + (sys.argv[1] if len(sys.argv) > 1 else "1")


def run(cmd: list[str]) -> str:
    return subprocess.run(cmd, cwd=ROOT, check=True, capture_output=True, text=True).stdout


def text_artifacts() -> None:
    (IMG / f"{PHASE}-recommendation-response.json").write_text(
        json.dumps(httpx.get("http://localhost:8000/recommendations/6040", timeout=10).json(), indent=2) + "\n"
    )
    (IMG / f"{PHASE}-demo-output.txt").write_text(run([sys.executable, "scripts/demo.py"]))
    (IMG / f"{PHASE}-latency-report.txt").write_text(run([sys.executable, "-m", "recsys.latency.loadgen"]))
    (IMG / f"{PHASE}-parity-check.txt").write_text(run([sys.executable, "-m", "recsys.features.parity"]))


def mlflow_screenshot() -> None:
    from playwright.sync_api import sync_playwright

    run_id = (ARTIFACTS / "latest_run_id").read_text().strip()
    ui = subprocess.Popen(
        [sys.executable, "-m", "mlflow", "ui", "--backend-store-uri", f"sqlite:///{MLRUNS / 'mlflow.db'}", "--port", "5055"],
        cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(60):
            try:
                httpx.get("http://localhost:5055", timeout=1)
                break
            except httpx.HTTPError:
                time.sleep(1)
        exp = httpx.get("http://localhost:5055/api/2.0/mlflow/runs/get", params={"run_id": run_id}).json()
        exp_id = exp["run"]["info"]["experiment_id"]
        with sync_playwright() as p:
            page = p.chromium.launch().new_page(viewport={"width": 1440, "height": 1000})
            page.goto(f"http://localhost:5055/#/experiments/{exp_id}/runs/{run_id}")
            page.wait_for_timeout(4000)
            page.screenshot(path=str(IMG / f"{PHASE}-mlflow-run.png"), full_page=True)
            page.goto(f"http://localhost:5055/#/experiments/{exp_id}/runs/{run_id}/model-metrics")
            page.wait_for_timeout(4000)
            page.screenshot(path=str(IMG / f"{PHASE}-mlflow-metrics.png"), full_page=True)
    finally:
        ui.terminate()


def phase2() -> None:
    from playwright.sync_api import sync_playwright

    out = lambda name, cmd: (IMG / f"phase2-{name}").write_text(run(cmd))  # noqa: E731
    py = sys.executable
    out("metrics-report.txt", [py, "-m", "recsys.eval.report"])
    (IMG / "phase2-stream-parity.txt").write_text(
        run([py, "-m", "recsys.features.parity"]) + "\n" + run([py, "-m", "recsys.streaming.parity"]))
    out("latency-no-cache.txt", [py, "-m", "recsys.latency.loadgen", "--variant", "streaming_session", "--no-cache",
                                 "--out", "latency_no_cache.json"])
    out("latency-cache.txt", [py, "-m", "recsys.latency.loadgen", "--variant", "streaming_session",
                              "--out", "latency_cache.json"])
    with sync_playwright() as p:
        page = p.chromium.launch().new_page(viewport={"width": 1440, "height": 900})
        jobs = httpx.get("http://localhost:8081/jobs/overview").json()["jobs"]
        jid = next(j["jid"] for j in jobs if j["state"] == "RUNNING")
        page.goto(f"http://localhost:8081/#/job/running/{jid}/overview")
        page.wait_for_timeout(4000)
        page.screenshot(path=str(IMG / "phase2-flink-job.png"))


if __name__ == "__main__":
    IMG.mkdir(parents=True, exist_ok=True)
    if PHASE == "phase2":
        phase2()
        print(f"wrote {sorted(p.name for p in IMG.glob('phase2-*'))}")
        sys.exit(0)
    text_artifacts()
    mlflow_screenshot()
    print(f"wrote {sorted(p.name for p in IMG.glob(PHASE + '-*'))}")
