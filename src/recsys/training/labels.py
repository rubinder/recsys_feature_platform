"""Anchor times and 7-day labels: the entity dataframe for point-in-time joins.

A row (user_id, item_id, event_timestamp=t, als_score, label) says: at time t,
ALS (fit only on events before t) proposed this item; label = 1 if the user
interacted with it in [t, t + label_window). Feast then joins the feature
values that existed at t. Nothing at or after t leaks into the features.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pandas as pd

from recsys.training import candidates

log = logging.getLogger(__name__)


@dataclass
class AnchorStats:
    anchor: str
    users_with_future_events: int = 0
    users_cold_start: int = 0
    queries_with_positive: int = 0
    rows: int = 0
    positives: int = 0
    extra: dict = field(default_factory=dict)


def anchors(end: datetime, window_days: int, cadence_days: int) -> list[datetime]:
    """Anchors on the weekly grid in [end - window, end)."""
    n = window_days // cadence_days
    return [end - timedelta(days=window_days) + timedelta(days=cadence_days * i) for i in range(n)]


def build_rows(
    events: pd.DataFrame, t: datetime, horizon_end: datetime, k: int, seed: int
) -> tuple[pd.DataFrame, AnchorStats]:
    """Candidate rows with labels for one anchor. Keeps only queries with >= 1 positive."""
    past = events[events.timestamp < t]
    future = events[(events.timestamp >= t) & (events.timestamp < horizon_end)]
    model = candidates.fit(past, seed)
    seen = past.groupby("user_id").item_id.agg(set)
    stats = AnchorStats(anchor=t.date().isoformat())

    frames = []
    for user_id, items in future.groupby("user_id").item_id:
        stats.users_with_future_events += 1
        positives = set(items) - seen.get(user_id, set())
        res = candidates.top_k(model, user_id, k)
        if res is None:
            stats.users_cold_start += 1
            continue
        item_ids, scores = res
        label = [int(i in positives) for i in item_ids]
        if not any(label):
            continue
        stats.queries_with_positive += 1
        frames.append(pd.DataFrame({"user_id": user_id, "item_id": item_ids, "als_score": scores, "label": label}))

    rows = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    rows["event_timestamp"] = pd.Timestamp(t)
    stats.rows, stats.positives = len(rows), int(rows.label.sum()) if len(rows) else 0
    log.info("anchor %s: %s", stats.anchor, stats)
    return rows, stats
