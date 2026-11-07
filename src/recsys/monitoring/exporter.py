"""Drift + freshness exporter for Prometheus (port 9108), recomputed every INTERVAL_S.

Feature drift compares like with like (same population definition on both sides):
- users: features of *active* users at the training cutoff (users with events in
  the ranker training window; streaming features = each user's latest state) vs
  what the online store serves now for users in the latest impressions;
- items: features of items weighted by interactions in the training window vs
  items weighted by interactions in the latest replayed events.
(An earlier version compared all items with shown items, which measured
popularity selection bias, not drift.)
Prediction drift: top-1 ranker score of the latest impressions vs the first
impressions of the current run, per variant. Freshness: Phase 3 SLA gauges.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import timedelta

import numpy as np
import pandas as pd
from prometheus_client import Gauge, start_http_server

from recsys.config import OFFLINE, load_split
from recsys.feature_repo.store import get_store
from recsys.monitoring.drift import psi
from recsys.quality import freshness
from recsys.serving.impressions import LOGS

log = logging.getLogger("recsys.monitoring")
INTERVAL_S = float(os.environ.get("DRIFT_INTERVAL_S", "30"))
WINDOW = int(os.environ.get("DRIFT_WINDOW", "500"))

FEATURE_PSI = Gauge("recsys_feature_psi", "PSI of an online feature vs the training reference", ["feature", "entity"])
PRED_PSI = Gauge("recsys_prediction_psi", "PSI of served top-1 scores, latest window vs first window", ["variant"])
TOP_SCORE = Gauge("recsys_top_score_mean", "Mean top-1 ranker score in the latest window", ["variant"])
BATCH_AGE = Gauge("recsys_batch_feature_age_days", "Age of the newest batch features in Redis vs the event clock")
STREAM_LAG = Gauge("recsys_stream_lag_seconds", "Event-time lag between newest produced and processed event")
STREAM_LAG_EVENTS = Gauge("recsys_stream_lag_events", "Events produced but not yet processed by Flink")
LAST_RUN = Gauge("recsys_monitor_last_run_timestamp", "Unix time of the last successful drift computation")

USER_FEATURES = {"user_stats:user_interaction_count": "user_interaction_count",
                 "user_stats:user_days_since_last": "user_days_since_last",
                 "user_stream:stream_count_24h": "stream_count_24h",
                 "user_stream:stream_session_events": "stream_session_events"}
ITEM_FEATURES = {"item_popularity:item_interaction_count": "item_interaction_count",
                 "item_popularity:item_interaction_count_30d": "item_interaction_count_30d"}


def reference() -> dict[str, np.ndarray]:
    """Training-time distributions over the same populations the live side measures."""
    from recsys.config import RAW

    split = load_split()
    t = pd.Timestamp(split.train_end)
    lo = t - timedelta(days=split.ranker_window_days)
    ev = pd.read_parquet(RAW / "events.parquet", columns=["user_id", "item_id", "timestamp"])
    ev = ev[pd.to_datetime(ev.timestamp, utc=True).between(lo, t, inclusive="left")]
    active = set(ev.user_id)
    ref = {}
    us = pd.read_parquet(OFFLINE / "user_stats.parquet")
    us = us[(pd.to_datetime(us.event_timestamp, utc=True) == t) & us.user_id.isin(active)]
    for col in ("user_interaction_count", "user_days_since_last"):
        ref[col] = us[col].to_numpy(float)
    st = pd.read_parquet(OFFLINE / "user_stream", columns=["user_id", "event_timestamp", "stream_count_24h", "stream_session_events"])
    st = st[(pd.to_datetime(st.event_timestamp, utc=True) < t) & st.user_id.isin(active)]
    latest = st.sort_values("event_timestamp").groupby("user_id").tail(1)  # each active user's state at the cutoff
    for col in ("stream_count_24h", "stream_session_events"):
        ref[col] = latest[col].to_numpy(float)
    ip = pd.read_parquet(OFFLINE / "item_popularity.parquet")
    ip = ip[pd.to_datetime(ip.event_timestamp, utc=True) == t].set_index("item_id")
    weighted = ip.reindex(ev.item_id).dropna()  # one row per training-window interaction
    for col in ("item_interaction_count", "item_interaction_count_30d"):
        ref[col] = weighted[col].to_numpy(float)
    return ref


def recent_interaction_items(n: int) -> list[int]:
    import duckdb

    if not list((LOGS / "interactions").glob("*/*.parquet")):
        return []
    c = duckdb.connect()
    return [r[0] for r in c.sql(f"SELECT item_id FROM read_parquet('{LOGS}/interactions/*/*.parquet', hive_partitioning=false) "
                                f"ORDER BY seq DESC LIMIT {n}").fetchall()]


def latest_impressions() -> pd.DataFrame:
    import duckdb

    files = list((LOGS / "impressions").glob("*/*.parquet"))
    if not files:
        return pd.DataFrame()
    c = duckdb.connect()
    return c.sql(f"SELECT user_id, items, scores, variant, timestamp FROM read_parquet('{LOGS}/impressions/*/*.parquet', "
                 "hive_partitioning=false, union_by_name=true) ORDER BY timestamp").df()


def compute(ref: dict[str, np.ndarray]) -> None:
    imp = latest_impressions()
    store = get_store()
    PRED_PSI.clear()  # drop series of variants that are no longer logged (e.g. a previous run)
    TOP_SCORE.clear()
    if not imp.empty:
        recent = imp.tail(WINDOW)
        users = [{"user_id": int(u)} for u in recent.user_id.unique()]
        got = store.get_online_features(features=list(USER_FEATURES), entity_rows=users).to_dict()
        for ref_name, col in USER_FEATURES.items():
            FEATURE_PSI.labels(col, "user").set(psi(ref[col], [v for v in got[col] if v is not None]))
        recent_items = recent_interaction_items(WINDOW * 4)
        if recent_items:
            uniq = sorted(set(recent_items))
            got = store.get_online_features(features=list(ITEM_FEATURES), entity_rows=[{"item_id": int(i)} for i in uniq]).to_dict()
            for ref_name, col in ITEM_FEATURES.items():
                val = dict(zip(got["item_id"], got[col]))
                weighted = [val[i] for i in recent_items if val.get(i) is not None]  # interaction-weighted, like the reference
                FEATURE_PSI.labels(col, "item").set(psi(ref[col], weighted))
        for variant, g in imp.groupby("variant"):
            top = g.scores.map(lambda s: s[0] if len(s) else np.nan).to_numpy(float)
            if len(top) >= 2 * min(WINDOW, len(top) // 2) and len(top) >= 50:
                w = min(WINDOW, len(top) // 2)
                PRED_PSI.labels(variant).set(psi(top[:w], top[-w:]))
                TOP_SCORE.labels(variant).set(float(np.nanmean(top[-w:])))
    b = freshness.batch_freshness()
    if "user_age_days" in b.metadata:
        BATCH_AGE.set(max(b.metadata.get("user_age_days", 0), b.metadata.get("item_age_days", 0)))
    s = freshness.stream_freshness()
    if s.metadata.get("lag_event_time_s") is not None:
        STREAM_LAG.set(s.metadata["lag_event_time_s"])
        STREAM_LAG_EVENTS.set(s.metadata["lag_events"])
    LAST_RUN.set(time.time())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    start_http_server(int(os.environ.get("EXPORTER_PORT", "9108")))
    ref = reference()
    log.info("drift reference loaded: %s", {k: len(v) for k, v in ref.items()})
    while True:
        try:
            compute(ref)
        except Exception:
            log.exception("drift computation failed; retrying next interval")
        time.sleep(INTERVAL_S)


if __name__ == "__main__":
    main()
