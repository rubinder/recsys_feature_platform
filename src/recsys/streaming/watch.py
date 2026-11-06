"""Print a user's streaming features from Redis whenever they change (demo of freshness)."""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

from recsys.feature_repo.store import get_store

REFS = ["user_stream:stream_session_events", "user_stream:stream_count_1h", "user_stream:stream_count_24h",
        "user_stream:stream_last_event_ts", "user_stream:stream_last_items"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", default="")
    ap.add_argument("--seconds", type=float, default=12)
    args = ap.parse_args()
    users = [int(u) for u in args.users.split(",") if u]
    store = get_store()
    last: dict[int, tuple] = {}
    t0 = time.monotonic()
    print(f"{'wall s':>6} {'user':>5} {'event time (UTC)':<20} {'session':>7} {'1h':>4} {'24h':>4}  last items")
    while time.monotonic() - t0 < args.seconds:
        got = store.get_online_features(features=REFS, entity_rows=[{"user_id": u} for u in users]).to_dict()
        for i, u in enumerate(users):
            ts = got["stream_last_event_ts"][i]
            key = (ts, got["stream_session_events"][i])
            if ts is not None and last.get(u) != key:
                last[u] = key
                when = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                items = (got["stream_last_items"][i] or [])[:5]
                print(f"{time.monotonic() - t0:6.2f} {u:>5} {when:<20} {got['stream_session_events'][i]:>7} "
                      f"{got['stream_count_1h'][i]:>4} {got['stream_count_24h'][i]:>4}  {items}")
        time.sleep(0.1)


if __name__ == "__main__":
    main()
