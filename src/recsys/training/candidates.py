"""Stage 1: implicit ALS candidate generation.

`top_k` is the only scoring path, used by training, offline evaluation, and the
serving API, so candidates and ALS scores are computed identically everywhere.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")

import numpy as np
import pandas as pd
import scipy.sparse as sp
from implicit.cpu.als import AlternatingLeastSquares

ALS_PARAMS = {"factors": 64, "regularization": 0.05, "iterations": 20, "alpha": 1.0}


@dataclass
class ALSModel:
    user_ids: np.ndarray  # index -> user_id
    item_ids: np.ndarray  # index -> item_id
    user_factors: np.ndarray
    item_factors: np.ndarray
    seen: sp.csr_matrix  # user x item interactions used to fit (filtered at recommend time)

    def __post_init__(self) -> None:
        self.uidx = {int(u): i for i, u in enumerate(self.user_ids)}
        self.iidx = {int(v): i for i, v in enumerate(self.item_ids)}

    def save(self, path: Path) -> None:
        np.savez_compressed(
            path,
            user_ids=self.user_ids,
            item_ids=self.item_ids,
            user_factors=self.user_factors,
            item_factors=self.item_factors,
            seen_data=self.seen.data,
            seen_indices=self.seen.indices,
            seen_indptr=self.seen.indptr,
            seen_shape=np.array(self.seen.shape),
        )

    @classmethod
    def load(cls, path: Path) -> ALSModel:
        z = np.load(path)
        seen = sp.csr_matrix((z["seen_data"], z["seen_indices"], z["seen_indptr"]), shape=tuple(z["seen_shape"]))
        return cls(z["user_ids"], z["item_ids"], z["user_factors"], z["item_factors"], seen)


def fit(events: pd.DataFrame, seed: int, params: dict = ALS_PARAMS) -> ALSModel:
    """Fit ALS on (user_id, item_id, weight) events. Confidence = 1 + alpha * weight."""
    agg = events.groupby(["user_id", "item_id"], sort=True)["weight"].sum().reset_index()
    user_ids = np.sort(agg.user_id.unique())
    item_ids = np.sort(agg.item_id.unique())
    rows = np.searchsorted(user_ids, agg.user_id.to_numpy())
    cols = np.searchsorted(item_ids, agg.item_id.to_numpy())
    conf = 1.0 + params["alpha"] * agg.weight.to_numpy(dtype=np.float32)
    ui = sp.csr_matrix((conf.astype(np.float32), (rows, cols)), shape=(len(user_ids), len(item_ids)))
    model = AlternatingLeastSquares(
        factors=params["factors"],
        regularization=params["regularization"],
        iterations=params["iterations"],
        random_state=seed,
        num_threads=4,
    )
    model.fit(ui, show_progress=False)
    return ALSModel(user_ids, item_ids, np.asarray(model.user_factors), np.asarray(model.item_factors), ui)


def top_k(model: ALSModel, user_id: int, k: int, extra_seen: set[int] | None = None):
    """Top-k unseen items for a user as (item_ids, scores), or None for an unknown user."""
    u = model.uidx.get(int(user_id))
    if u is None:
        return None
    scores = model.item_factors @ model.user_factors[u]
    scores[model.seen.indices[model.seen.indptr[u] : model.seen.indptr[u + 1]]] = -np.inf
    if extra_seen:
        idx = [model.iidx[i] for i in extra_seen if i in model.iidx]
        scores[idx] = -np.inf
    k = min(k, int(np.isfinite(scores).sum()))
    top = np.argpartition(-scores, k - 1)[:k]
    top = top[np.lexsort((model.item_ids[top], -scores[top]))]  # deterministic tie-break
    return model.item_ids[top], scores[top]
