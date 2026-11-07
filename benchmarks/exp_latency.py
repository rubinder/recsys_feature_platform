"""Experiment 3: serving latency vs the item-feature cache and the feature-fetch batch size.

Knobs (one at a time): cache off/on; candidates fetched per request K in
{50, 100, 200, 400}. Needs the API running (`make serve`). Latency is a physical
measurement, so each configuration runs 3 times and the median is reported;
the request sample is seeded and identical across configurations.
"""

from __future__ import annotations

import asyncio
import statistics

import httpx
import numpy as np

from benchmarks.common import mlflow_run, save
from recsys.config import load_split
from recsys.latency.loadgen import run as load_run
from recsys.latency.loadgen import sample_users

BASE = "http://localhost:8000"
VARIANT = "streaming_session"
KS = [50, 100, 200, 400]
REQUESTS = 300
REPEATS = 3


def one(params: dict, users: list[int], concurrency: int) -> dict:
    reps = []
    for _ in range(REPEATS):
        if params["cache"] == "true":
            httpx.post(f"{BASE}/cache/clear")
            asyncio.run(load_run(BASE, users, 1, params))  # warm the cache once, not measured
        lat, wall, errors = asyncio.run(load_run(BASE, users, concurrency, params))
        p50, p95, p99 = np.percentile(lat, [50, 95, 99])
        reps.append({"p50": p50, "p95": p95, "p99": p99, "rps": len(lat) / wall, "errors": errors})
    return {k: statistics.median(r[k] for r in reps) for k in reps[0]} | {"runs": reps}


def run() -> dict:
    health = httpx.get(f"{BASE}/health", timeout=5).json()
    if VARIANT not in health["variants"]:
        raise SystemExit(f"API does not serve {VARIANT}; run `make train-streaming` and restart the API")
    users = sample_users(REQUESTS, load_split().seed)
    out = {"variant": VARIANT, "requests": REQUESTS, "repeats": REPEATS, "configs": []}
    for cache in ("false", "true"):
        for k in KS:
            for conc in (1, 8):
                params = {"variant": VARIANT, "cache": cache, "k": str(k)}
                r = one(params, users, conc)
                r.update({"cache": cache == "true", "k": k, "concurrency": conc})
                r["mlflow_run_id"] = mlflow_run("latency", f"cache={cache} k={k} c={conc}",
                                                {"cache": cache, "k": k, "concurrency": conc, "variant": VARIANT},
                                                {k2: r[k2] for k2 in ("p50", "p95", "p99", "rps")})
                out["configs"].append(r)
                print(f"cache={cache:<5} k={k:<4} c={conc}: p50 {r['p50']:.2f} ms  p99 {r['p99']:.2f} ms  {r['rps']:.0f} req/s")
    save("latency", out)
    return out


if __name__ == "__main__":
    run()
