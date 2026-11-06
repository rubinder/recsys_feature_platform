"""make latency: send N requests, report p50/p95/p99 and throughput.

Users are a seeded sample of warm users (known to the model) so runs are
comparable. Reports client-side latency (includes HTTP) and the server's
per-stage breakdown from /latency, and writes data/artifacts/latency_report.json.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time

import httpx
import numpy as np
import pandas as pd

from recsys.config import ARTIFACTS, RAW, load_split


def sample_users(n: int, seed: int) -> list[int]:
    split = load_split()
    ev = pd.read_parquet(RAW / "events.parquet", columns=["user_id", "timestamp"])
    warm = np.sort(ev[pd.to_datetime(ev.timestamp, utc=True) < split.validation_end].user_id.unique())
    return np.random.default_rng(seed).choice(warm, size=n, replace=True).tolist()


async def run(base: str, users: list[int], concurrency: int) -> tuple[list[float], float, int]:
    sem = asyncio.Semaphore(concurrency)
    lat: list[float] = []
    errors = 0
    async with httpx.AsyncClient(base_url=base, timeout=10) as client:
        await client.get(f"/recommendations/{users[0]}")  # warm-up
        await client.post("/latency/reset")

        async def one(u: int) -> None:
            nonlocal errors
            async with sem:
                t = time.perf_counter()
                r = await client.get(f"/recommendations/{u}")
                lat.append((time.perf_counter() - t) * 1000)
                errors += r.status_code != 200

        start = time.perf_counter()
        await asyncio.gather(*(one(u) for u in users))
        wall = time.perf_counter() - start
    return lat, wall, errors


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--requests", type=int, default=500)
    ap.add_argument("--concurrency", type=int, nargs="+", default=[1, 8])
    args = ap.parse_args()
    users = sample_users(args.requests, load_split().seed)
    report = {"requests": args.requests, "runs": []}
    for c in args.concurrency:
        lat, wall, errors = asyncio.run(run(args.base, users, c))
        p50, p95, p99 = np.percentile(lat, [50, 95, 99])
        server = httpx.get(f"{args.base}/latency").json()
        report["runs"].append({"concurrency": c, "client_p50_ms": p50, "client_p95_ms": p95, "client_p99_ms": p99,
                               "throughput_rps": len(lat) / wall, "errors": errors, "server_stages": server})
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    (ARTIFACTS / "latency_report.json").write_text(json.dumps(report, indent=2))

    print(f"{args.requests} requests per run, warm users, seeded sample")
    print(f"{'concurrency':>11} {'p50 ms':>8} {'p95 ms':>8} {'p99 ms':>8} {'req/s':>8} {'errors':>6}")
    for r in report["runs"]:
        print(f"{r['concurrency']:>11} {r['client_p50_ms']:>8.2f} {r['client_p95_ms']:>8.2f} {r['client_p99_ms']:>8.2f} "
              f"{r['throughput_rps']:>8.1f} {r['errors']:>6}")
    print("\nserver-side stages (concurrency 1):")
    for stage, s in report["runs"][0]["server_stages"].items():
        print(f"  {stage:<16} p50 {s['p50_ms']:7.2f} ms   p95 {s['p95_ms']:7.2f} ms   p99 {s['p99_ms']:7.2f} ms")


if __name__ == "__main__":
    main()
