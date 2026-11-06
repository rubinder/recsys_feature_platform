"""Online/offline parity check: Redis vs point-in-time offline at the serving time.

For a seeded sample of (user, item) pairs, compares every feature served from
Redis with the value Feast's offline store returns at validation_end. Any
mismatch means train/serve skew, so this exits non-zero.
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd

from recsys.config import OFFLINE, load_split
from recsys.feature_repo.store import get_store
from recsys.features.features import BATCH_REFS, STREAM_REFS


def check(n: int = 200) -> tuple[int, int, list[str]]:
    split = load_split()
    rng = np.random.default_rng(split.seed)
    t = pd.Timestamp(split.validation_end)
    users = pd.read_parquet(OFFLINE / "user_stats.parquet", columns=["user_id", "event_timestamp"])
    items = pd.read_parquet(OFFLINE / "item_stats.parquet", columns=["item_id", "event_timestamp"])
    users = users[pd.to_datetime(users.event_timestamp, utc=True) == t].user_id.unique()
    items = items[pd.to_datetime(items.event_timestamp, utc=True) == t].item_id.unique()
    pairs = pd.DataFrame({"user_id": rng.choice(users, n), "item_id": rng.choice(items, n)}).drop_duplicates()

    store = get_store()
    refs = BATCH_REFS + ([] if _replayed() else STREAM_REFS)
    online = pd.DataFrame(store.get_online_features(features=refs, entity_rows=pairs.to_dict("records")).to_dict())
    offline = store.get_historical_features(entity_df=pairs.assign(event_timestamp=t), features=refs).to_df()
    merged = online.merge(offline, on=["user_id", "item_id"], suffixes=("_on", "_off"))
    cols = [r.split(":")[1] for r in refs]
    bad = [c for c in cols if not all(_same(a, b) for a, b in zip(merged[f"{c}_on"], merged[f"{c}_off"]))]
    # "Identical" is meaningless if both sides are empty: require real coverage.
    coverage = {c: float(merged[f"{c}_on"].notna().mean()) for c in cols}
    empty = [c for c, v in coverage.items() if v < 0.5]
    return len(merged), len(cols), bad + [f"{c} (online coverage {coverage[c]:.0%})" for c in empty]


def _replayed() -> bool:
    """After a stream replay, Redis streaming features are legitimately newer than
    the cutoff; they are checked by `make stream-parity` instead."""
    try:
        from recsys.streaming import progress
        from recsys.streaming.events import history_end_seq, ordered_events

        return progress.get(progress.client()) >= history_end_seq(ordered_events())
    except Exception:
        return False


def _same(a, b) -> bool:
    """Scalar or list equality with NaN/None treated as equal-missing."""
    if a is None or (isinstance(a, float) and np.isnan(a)):
        return b is None or (isinstance(b, float) and np.isnan(b))
    if isinstance(a, (list, tuple, np.ndarray)) or isinstance(b, (list, tuple, np.ndarray)):
        return b is not None and list(a) == list(b)
    return b is not None and not (isinstance(b, float) and np.isnan(b)) and np.isclose(float(a), float(b))


def main() -> None:
    rows, cols, bad = check()
    note = " (batch features only: stream has been replayed, see `make stream-parity`)" if _replayed() else ""
    print(f"online/offline parity: {rows} (user, item) pairs x {cols} features at serving time{note}")
    if bad:
        print(f"MISMATCH in: {bad}")
        sys.exit(1)
    print("all features identical in Redis and the point-in-time offline store (>=50% non-null each)")


if __name__ == "__main__":
    main()
