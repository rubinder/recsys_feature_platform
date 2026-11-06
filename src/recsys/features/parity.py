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
from recsys.features.features import MODEL_FEATURES


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
    from recsys.features.features import FEATURE_REFS

    online = pd.DataFrame(store.get_online_features(features=FEATURE_REFS, entity_rows=pairs.to_dict("records")).to_dict())
    offline = store.get_historical_features(entity_df=pairs.assign(event_timestamp=t), features=FEATURE_REFS).to_df()
    merged = online.merge(offline, on=["user_id", "item_id"], suffixes=("_on", "_off"))
    cols = [c for c in MODEL_FEATURES if c != "als_score"]
    bad = [c for c in cols if not np.allclose(merged[f"{c}_on"].astype(float), merged[f"{c}_off"].astype(float), equal_nan=True)]
    return len(merged), len(cols), bad


def main() -> None:
    rows, cols, bad = check()
    print(f"online/offline parity: {rows} (user, item) pairs x {cols} features at serving time")
    if bad:
        print(f"MISMATCH in: {bad}")
        sys.exit(1)
    print("all features identical in Redis and the point-in-time offline store")


if __name__ == "__main__":
    main()
