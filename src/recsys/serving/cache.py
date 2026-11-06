"""In-process TTL cache for item features.

Item features are batch-computed weekly, so caching them for minutes changes
nothing about freshness while turning a 200-row online read into a 1-row read.
User features (including streaming ones) are never cached.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterable


class TTLCache:
    def __init__(self, ttl_s: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.ttl_s = ttl_s
        self._clock = clock
        self._data: dict[int, tuple[float, dict]] = {}
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get_many(self, keys: Iterable[int]) -> tuple[dict[int, dict], list[int]]:
        now = self._clock()
        found, missing = {}, []
        with self._lock:
            for k in keys:
                entry = self._data.get(k)
                if entry and entry[0] > now:
                    found[k] = entry[1]
                else:
                    missing.append(k)
            self.hits += len(found)
            self.misses += len(missing)
        return found, missing

    def put_many(self, values: dict[int, dict]) -> None:
        expires = self._clock() + self.ttl_s
        with self._lock:
            for k, v in values.items():
                self._data[k] = (expires, v)

    def stats(self) -> dict[str, float]:
        with self._lock:
            total = self.hits + self.misses
            return {"hits": self.hits, "misses": self.misses, "hit_rate": self.hits / total if total else 0.0,
                    "size": len(self._data), "ttl_s": self.ttl_s}

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
            self.hits = self.misses = 0
