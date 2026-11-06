"""Stream progress marker written by the Flink job (max seq fully processed and pushed)."""

from __future__ import annotations

import os
import time

import redis

KEY = "recsys:stream:progress"


def client() -> redis.Redis:
    return redis.Redis.from_url(os.environ.get("REDIS_URL_HOST", "redis://localhost:6379/0"))


def get(r: redis.Redis) -> int:
    v = r.get(KEY)
    return int(v) if v is not None else -1


def _job_failed() -> str | None:
    import httpx

    try:
        jobs = httpx.get("http://localhost:8081/jobs/overview", timeout=5).json()["jobs"]
    except Exception:
        return None
    if jobs and not any(j["state"] == "RUNNING" for j in jobs):
        return ", ".join(f"{j['name']}={j['state']}" for j in jobs)
    return None


def wait_for(r: redis.Redis, seq: int, timeout_s: float = 600, poll_s: float = 0.002) -> float:
    """Block until Flink has processed `seq`; returns seconds waited. Fails fast if the job died."""
    t0 = last_check = time.monotonic()
    while get(r) < seq:
        now = time.monotonic()
        if now - t0 > timeout_s:
            raise TimeoutError(f"stream progress stuck at {get(r)}, waiting for {seq}")
        if now - last_check > 5:
            last_check = now
            failed = _job_failed()
            if failed:
                raise RuntimeError(f"Flink job is not running ({failed}); see http://localhost:8081")
        time.sleep(poll_s)
    return time.monotonic() - t0
