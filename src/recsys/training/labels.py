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
) -> tuple[pd.DataFrame, AnchorStats, candidates.ALSModel]:
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
    return rows, stats, model


SESSION_ANCHOR_AFTER = 5  # anchor right after the 5th event of a session (needs >= 6 events)


def build_session_rows(
    events: pd.DataFrame, week_start: datetime, week_end: datetime, horizon_end: datetime, label_days: int, k: int, seed: int
) -> tuple[pd.DataFrame, AnchorStats, candidates.ALSModel]:
    """Mid-session anchors: mirrors serving during the live replay.

    ALS is fit on events before `week_start` (frozen for the week, like the
    served model), and each user gets at most one anchor in the week: the
    timestamp of the 5th event of their first session with >= 6 events.
    Seen = everything up to and including the anchor; label = unseen items in
    (anchor, anchor + label_days], capped at horizon_end.
    """
    from recsys.streaming.logic import SESSION_GAP_S

    model = candidates.fit(events[events.timestamp < week_start], seed)
    stats = AnchorStats(anchor=f"{week_start.date().isoformat()} (session)")
    week = events[(events.timestamp >= week_start) & (events.timestamp < week_end)]
    week = week.sort_values(["user_id", "timestamp", "item_id"], kind="stable")
    seen_before = events[events.timestamp < week_start].groupby("user_id").item_id.agg(set)
    frames = []
    for user_id, ev in week.groupby("user_id", sort=True):
        stats.users_with_future_events += 1
        if user_id not in model.uidx:
            stats.users_cold_start += 1
            continue
        ts = ev.timestamp.to_numpy()
        gaps = (ts[1:] - ts[:-1]) / pd.Timedelta(seconds=1)
        session_id = (gaps > SESSION_GAP_S).cumsum()
        session_id = [0, *session_id]
        sizes = pd.Series(session_id).value_counts()
        eligible = [s for s in sorted(set(session_id)) if sizes[s] > SESSION_ANCHOR_AFTER]
        if not eligible:
            continue
        pos_in_session = [i for i, s in enumerate(session_id) if s == eligible[0]]
        anchor = ev.timestamp.iloc[pos_in_session[SESSION_ANCHOR_AFTER - 1]]
        in_week_seen = set(ev[ev.timestamp <= anchor].item_id)
        end = min(anchor + pd.Timedelta(days=label_days), pd.Timestamp(horizon_end))
        future = events[(events.user_id == user_id) & (events.timestamp > anchor) & (events.timestamp <= end)]
        positives = set(future.item_id) - in_week_seen - seen_before.get(user_id, set())
        item_ids, scores = candidates.top_k(model, user_id, k, extra_seen=in_week_seen)
        label = [int(i in positives) for i in item_ids]
        if not any(label):
            continue
        stats.queries_with_positive += 1
        frames.append(pd.DataFrame({"user_id": user_id, "item_id": item_ids, "als_score": scores, "label": label,
                                    "event_timestamp": anchor}))
    rows = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    stats.rows, stats.positives = len(rows), int(rows.label.sum()) if len(rows) else 0
    log.info("session anchors %s: %s", stats.anchor, stats)
    return rows, stats, model
