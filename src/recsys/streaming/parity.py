"""Streaming parity: what Flink wrote to Redis == the shared logic recomputed from events.

After a replay, recompute UserStreamState over every event up to the stream's
progress marker (the backfill code path, in plain Python) and compare the final
feature row of each replayed user with the value Feast serves from Redis. Also
checks the offline store: a point-in-time read at each user's last event
returns the same row. Any difference is streaming train/serve skew.
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from recsys.feature_repo.store import get_store
from recsys.features.features import STREAM_REFS
from recsys.streaming import progress
from recsys.streaming.backfill import compute
from recsys.streaming.events import history_end_seq, ordered_events

COLS = [r.split(":")[1] for r in STREAM_REFS]


def _eq(a, b) -> bool:
    if isinstance(a, (list, tuple, np.ndarray)) or isinstance(b, (list, tuple, np.ndarray)):
        return a is not None and b is not None and list(a) == list(b)
    if a is None or (isinstance(a, float) and np.isnan(a)):
        return b is None or (isinstance(b, float) and np.isnan(b))
    return b is not None and np.isclose(float(a), float(b))


def check(sample: int = 300) -> dict:
    ev = ordered_events()
    h = history_end_seq(ev)
    done = progress.get(progress.client())
    if done < h:
        raise SystemExit("no replayed events processed yet; run `make simulate` first")
    upto = ev[ev.seq <= done]
    rows, _ = compute(upto)
    expected = pd.DataFrame(rows).groupby("user_id").tail(1).set_index("user_id")
    replayed = sorted(upto[upto.seq >= h].user_id.unique())
    users = replayed if len(replayed) <= sample else list(np.random.default_rng(0).choice(replayed, sample, replace=False))

    store = get_store()
    online = pd.DataFrame(store.get_online_features(features=STREAM_REFS, entity_rows=[{"user_id": int(u)} for u in users]).to_dict())
    online = online.set_index("user_id")
    online_bad = [(u, c) for u in users for c in COLS if not _eq(online.at[u, c], expected.at[u, c])]

    ent = pd.DataFrame({"user_id": users, "event_timestamp": [expected.at[u, "event_timestamp"] for u in users]})
    offline = store.get_historical_features(entity_df=ent, features=STREAM_REFS).to_df().set_index("user_id")
    offline_bad = [(u, c) for u in users for c in COLS if not _eq(offline.at[u, c], expected.at[u, c])]
    return {"users": len(users), "replayed_events": int((upto.seq >= h).sum()), "progress": done,
            "online_mismatches": online_bad[:10], "offline_mismatches": offline_bad[:10],
            "n_online_bad": len(online_bad), "n_offline_bad": len(offline_bad)}


def main() -> None:
    r = check()
    print(f"streaming parity: {r['users']} replayed users x {len(COLS)} features, "
          f"{r['replayed_events']} replayed events (progress seq {r['progress']})")
    print(f"  Redis (Flink)        vs recomputed: {r['n_online_bad']} mismatches")
    print(f"  offline parts (PIT)  vs recomputed: {r['n_offline_bad']} mismatches")
    if r["n_online_bad"] or r["n_offline_bad"]:
        print("  examples:", r["online_mismatches"][:3], r["offline_mismatches"][:3])
        sys.exit(1)
    print("  Flink output, Redis, the offline store and the shared logic all agree")


if __name__ == "__main__":
    main()
