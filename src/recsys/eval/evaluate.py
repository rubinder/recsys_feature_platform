"""Offline evaluation on the fixed test split: ALS-only vs two-stage.

Ground truth: items a user interacts with at or after validation_end that they
had not seen before it. Users must be known to ALS (warm start). Features come
from Feast point-in-time at validation_end, the same values Redis serves.
"""

from __future__ import annotations

from datetime import datetime

import lightgbm as lgb
import pandas as pd
from feast import FeatureStore

from recsys import scoring
from recsys.eval.metrics import evaluate
from recsys.features.derived import add_stream_derived
from recsys.features.features import BATCH, FeatureSet
from recsys.training import candidates
from recsys.training.candidates import ALSModel


def ground_truth(events: pd.DataFrame, t: datetime) -> dict[int, set[int]]:
    seen = events[events.timestamp < t].groupby("user_id").item_id.agg(set)
    future = events[events.timestamp >= t].groupby("user_id").item_id.agg(set)
    return {int(u): items - seen.get(u, set()) for u, items in future.items() if items - seen.get(u, set())}


def candidate_frame(model: ALSModel, users: list[int], t: datetime, k: int) -> pd.DataFrame:
    frames = []
    for u in users:
        res = candidates.top_k(model, u, k)
        if res is not None:
            frames.append(pd.DataFrame({"user_id": u, "item_id": res[0], "als_score": res[1]}))
    df = pd.concat(frames, ignore_index=True)
    df["event_timestamp"] = pd.Timestamp(t)
    return df


def with_features(store: FeatureStore, rows: pd.DataFrame, fs: FeatureSet = BATCH) -> pd.DataFrame:
    """Point-in-time join, merged back on keys (Feast does not preserve row order)."""
    keys = ["user_id", "item_id", "event_timestamp"]
    feats = store.get_historical_features(entity_df=rows[keys], features=fs.refs).to_df()
    feats["event_timestamp"] = pd.to_datetime(feats["event_timestamp"], utc=True)
    rows = rows.assign(event_timestamp=pd.to_datetime(rows["event_timestamp"], utc=True))
    out = rows.merge(feats, on=keys, how="left", validate="one_to_one")
    if len(out) != len(rows):
        raise RuntimeError("feature join changed row count")
    return out


def prepare(store: FeatureStore, rows: pd.DataFrame, fs: FeatureSet, als: ALSModel, t: datetime | None) -> pd.DataFrame:
    """Feature join plus request-time derived features (t=None: per-row event_timestamp)."""
    rows = with_features(store, rows, fs)
    if not fs.uses_stream:
        return rows
    return add_stream_derived(rows, als, None if t is None else pd.Timestamp(t))


def run(
    events: pd.DataFrame, model: ALSModel, booster: lgb.Booster, store: FeatureStore, t: datetime, k: int, ks: list[int],
    fs: FeatureSet = BATCH,
) -> dict[str, float]:
    truth = ground_truth(events, t)
    users = sorted(u for u in truth if u in model.uidx)
    rows = prepare(store, candidate_frame(model, users, t, k), fs, model, t)
    rows["ranker_score"] = scoring.score(booster, rows, fs.model_features)

    def ranked(col: str) -> dict[int, list[int]]:
        ordered = rows.sort_values(["user_id", col, "item_id"], ascending=[True, False, True])
        return ordered.groupby("user_id").item_id.agg(list).to_dict()

    als_only = evaluate(ranked("als_score"), truth, ks)
    two_stage = evaluate(ranked("ranker_score"), truth, ks)
    out = {f"test_als_{m}": v for m, v in als_only.items()}
    out.update({f"test_two_stage_{m}": v for m, v in two_stage.items()})
    out["test_candidate_recall_at_k"] = evaluate(ranked("als_score"), truth, [k])[f"recall_at_{k}"]
    out["test_users_total"] = float(len(truth))
    return out
