"""Freshness SLA checks on the online store.

The system runs on an event-time clock: "now" is the serving cutoff, advanced by
the stream (the event time of Flink's progress marker) during a replay.
RECSYS_CLOCK overrides it, e.g. to show what the check reports if the weekly
batch job stopped running.

- Batch views: newest feature timestamp in Redis vs now; SLA 8 days (weekly
  snapshots plus one day of grace).
- Streaming: lag between the newest event published to Kafka and the newest
  event Flink has processed and pushed; SLA 300 s of event time.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from recsys.config import OFFLINE, load_split
from recsys.feature_repo.store import get_store
from recsys.quality.checks import CheckOutcome

BATCH_SLA = timedelta(days=8)
STREAM_SLA = timedelta(seconds=300)
BATCH_PROBES = {"user": "user_stats:user_interaction_count", "item": "item_popularity:item_interaction_count"}


def clock() -> datetime:
    if os.environ.get("RECSYS_CLOCK"):
        return datetime.fromisoformat(os.environ["RECSYS_CLOCK"]).replace(tzinfo=timezone.utc)
    now = load_split().validation_end
    try:
        from recsys.streaming import progress
        from recsys.streaming.events import ordered_events

        seq = progress.get(progress.client())
        ev = ordered_events()
        if 0 <= seq < len(ev):
            now = max(now, ev.timestamp.iloc[seq].to_pydatetime())
    except Exception:
        pass
    return now


def _as_dt(t) -> datetime:
    """Feast returns online event timestamps as epoch seconds (or datetimes, by version)."""
    if isinstance(t, (int, float)):
        return datetime.fromtimestamp(t, tz=timezone.utc)
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def batch_freshness(sample: int = 200) -> CheckOutcome:
    store = get_store()
    now = clock()
    rng = np.random.default_rng(0)
    users = pd.read_parquet(OFFLINE / "user_stats.parquet", columns=["user_id"]).user_id.unique()
    items = pd.read_parquet(OFFLINE / "item_popularity.parquet", columns=["item_id"]).item_id.unique()
    newest = {}
    for entity, ref, ids in (("user", BATCH_PROBES["user"], users), ("item", BATCH_PROBES["item"], items)):
        key = f"{entity}_id"
        rows = [{key: int(i)} for i in rng.choice(ids, min(sample, len(ids)), replace=False)]
        resp = store.get_online_features(features=[ref], entity_rows=rows).to_dict(include_event_timestamps=True)
        ts = [_as_dt(t) for t in resp.get(f"{ref.split(':')[1]}__ts", []) if t is not None]
        newest[entity] = max(ts) if ts else None
    ages = {k: (now - v) if v else None for k, v in newest.items()}
    missing = [k for k, v in newest.items() if v is None]
    stale = [k for k, a in ages.items() if a is not None and a > BATCH_SLA]
    return CheckOutcome("online.batch_freshness", not (missing or stale), metadata={
        "clock": now.isoformat(), "sla_days": BATCH_SLA.days,
        **{f"{k}_newest": str(v) for k, v in newest.items()},
        **{f"{k}_age_days": round(a.total_seconds() / 86400, 2) for k, a in ages.items() if a is not None},
        "missing": ",".join(missing) or "none", "stale": ",".join(stale) or "none"})


def stream_freshness() -> CheckOutcome:
    try:
        from recsys.streaming import kafka_io, progress
        from recsys.streaming.events import history_end_seq, ordered_events

        produced = kafka_io.topic_size(kafka_io.TOPIC) - 1  # seq of the newest event in the topic (1 message per seq)
        done = progress.get(progress.client())
    except Exception as exc:
        return CheckOutcome("online.stream_freshness", True, "WARN", {"status": f"stream not running ({type(exc).__name__})"})
    ev = ordered_events()
    h = history_end_seq(ev)
    if produced < h:
        return CheckOutcome("online.stream_freshness", True, "WARN", {"status": "no live events produced yet"})
    produced = min(produced, len(ev) - 1)
    lag_events = max(0, produced - done)
    lag = ev.timestamp.iloc[produced] - ev.timestamp.iloc[max(done, 0)] if done >= 0 else timedelta.max
    return CheckOutcome("online.stream_freshness", lag <= STREAM_SLA, metadata={
        "newest_produced_seq": int(produced), "processed_seq": int(done), "lag_events": int(lag_events),
        "lag_event_time_s": round(lag.total_seconds(), 1) if done >= 0 else None, "sla_s": STREAM_SLA.total_seconds()})


ALL = {"batch_freshness": batch_freshness, "stream_freshness": stream_freshness}
