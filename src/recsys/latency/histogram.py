"""Reusable per-stage latency histogram.

Keeps Prometheus-style cumulative bucket counts (exported in Phase 5) plus a
bounded reservoir of recent samples for exact percentiles.
"""

from __future__ import annotations

import threading
from collections import deque

import numpy as np

BUCKETS_MS = (1, 2, 5, 10, 20, 30, 50, 75, 100, 150, 250, 500, 1000, 2500)


class LatencyHistogram:
    def __init__(self, max_samples: int = 50_000, buckets_ms: tuple[float, ...] = BUCKETS_MS) -> None:
        self.buckets_ms = buckets_ms
        self._lock = threading.Lock()
        self._stages: dict[str, dict] = {}
        self._max = max_samples

    def observe(self, stage: str, ms: float) -> None:
        with self._lock:
            s = self._stages.setdefault(
                stage, {"samples": deque(maxlen=self._max), "counts": [0] * len(self.buckets_ms), "count": 0, "sum": 0.0}
            )
            s["samples"].append(ms)
            s["count"] += 1
            s["sum"] += ms
            for i, b in enumerate(self.buckets_ms):
                if ms <= b:
                    s["counts"][i] += 1

    def summary(self) -> dict[str, dict[str, float]]:
        with self._lock:
            out = {}
            for stage, s in self._stages.items():
                a = np.fromiter(s["samples"], dtype=float)
                p50, p95, p99 = np.percentile(a, [50, 95, 99]) if a.size else (0.0, 0.0, 0.0)
                out[stage] = {"count": s["count"], "mean_ms": s["sum"] / max(s["count"], 1),
                              "p50_ms": float(p50), "p95_ms": float(p95), "p99_ms": float(p99)}
            return out

    def buckets(self) -> dict[str, list[tuple[float, int]]]:
        with self._lock:
            return {stage: list(zip(self.buckets_ms, s["counts"])) for stage, s in self._stages.items()}

    def reset(self) -> None:
        with self._lock:
            self._stages.clear()
