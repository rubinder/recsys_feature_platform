"""Streaming user features: one implementation, two runners.

`UserStreamState` is pure Python with no I/O. The Flink job wraps it in a keyed
process function for live events; `backfill` runs it over history to build the
offline table that training reads. Same code, same values, so there is no
train/serve skew between streaming and backfilled features.

Feast's online store ignores a write whose event timestamp equals the stored
one, and MovieLens has many same-second events per user. Each update therefore
gets a strictly increasing feature timestamp: the event time plus a microsecond
per earlier event from that user in the same second.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

SESSION_GAP_S = 30 * 60
LAST_N = 10
WINDOW_1H = 3600
WINDOW_24H = 86400

STREAM_COLUMNS = [
    "user_id",
    "event_timestamp",
    "stream_session_events",
    "stream_session_duration_s",
    "stream_last_items",
    "stream_count_1h",
    "stream_count_24h",
    "stream_gap_s",
    "stream_last_event_ts",
]


@dataclass
class UserStreamState:
    last_ts: float | None = None
    same_second: int = 0
    session_start: float | None = None
    session_events: int = 0
    window: list[float] = field(default_factory=list)  # event times in the last 24h, ascending
    last_items: list[int] = field(default_factory=list)  # most recent first

    def update(self, user_id: int, item_id: int, ts: float) -> dict:
        """Apply one event (ts = epoch seconds) and return the feature row after it."""
        if self.last_ts is not None and ts < self.last_ts:
            raise ValueError(f"out-of-order event for user {user_id}: {ts} < {self.last_ts}")
        gap = None if self.last_ts is None else ts - self.last_ts
        self.same_second = self.same_second + 1 if gap == 0 else 0
        if gap is None or gap > SESSION_GAP_S:
            self.session_start, self.session_events = ts, 0
        self.session_events += 1
        self.window = [t for t in self.window if t > ts - WINDOW_24H] + [ts]
        self.last_items = ([item_id] + [i for i in self.last_items if i != item_id])[:LAST_N]
        self.last_ts = ts
        return {
            "user_id": user_id,
            "event_timestamp": datetime.fromtimestamp(ts, tz=timezone.utc) + timedelta(microseconds=self.same_second),
            "stream_session_events": self.session_events,
            "stream_session_duration_s": ts - self.session_start,
            "stream_last_items": list(self.last_items),
            "stream_count_1h": sum(1 for t in self.window if t > ts - WINDOW_1H),
            "stream_count_24h": len(self.window),
            "stream_gap_s": gap,
            "stream_last_event_ts": int(ts),
        }

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> UserStreamState:
        return cls(**d)
