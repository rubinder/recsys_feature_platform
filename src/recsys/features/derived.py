"""Features derived at request (or anchor) time from streaming features.

Shared by training and serving. `as_of` is the anchor time in training and the
request's event-time "now" in serving; ALS factors come from whichever ALS model
produced the candidates.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from recsys.streaming.logic import SESSION_GAP_S
from recsys.training.candidates import ALSModel


def _unit(m: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(m, axis=-1, keepdims=True)
    return np.divide(m, n, out=np.zeros_like(m), where=n > 0)


def add_stream_derived(df: pd.DataFrame, als: ALSModel, as_of: pd.Timestamp | None = None) -> pd.DataFrame:
    """Adds stream_secs_since_last, stream_in_session, recent_item_sim.

    `as_of` is one time for all rows, or None to use each row's event_timestamp
    (training rows with per-user anchors).
    """
    df = df.copy()
    last = pd.to_numeric(df["stream_last_event_ts"], errors="coerce")
    if as_of is None:
        ref = (pd.to_datetime(df["event_timestamp"], utc=True) - pd.Timestamp(0, tz="UTC")) / pd.Timedelta(seconds=1)
    else:
        ref = as_of.timestamp()
    secs = ref - last
    df["stream_secs_since_last"] = secs
    df["stream_in_session"] = np.where(secs.isna(), np.nan, (secs <= SESSION_GAP_S).astype(float))

    item_vecs = _unit(als.item_factors)
    sims = np.full(len(df), np.nan)
    for _, idx in df.groupby("user_id", sort=False).indices.items():
        recent = df["stream_last_items"].iloc[idx[0]]
        rec_idx = [als.iidx[i] for i in (recent if recent is not None else []) if i in als.iidx]
        if not rec_idx:
            continue
        profile = _unit(item_vecs[rec_idx].mean(axis=0))
        cand = [als.iidx.get(int(i), -1) for i in df["item_id"].iloc[idx]]
        cand = np.array(cand)
        ok = cand >= 0
        out = np.full(len(idx), np.nan)
        out[ok] = item_vecs[cand[ok]] @ profile
        sims[idx] = out
    df["recent_item_sim"] = sims
    return df
