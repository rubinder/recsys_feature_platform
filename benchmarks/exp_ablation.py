"""Experiment 4: feature-group ablation (remove one group at a time).

Knob: which feature groups the ranker sees. Rows, labels, candidates, folds,
boosting rounds and seed are identical across configurations. Uses the
session-anchored streaming feature set (all groups available) and 5-fold
cross-validation grouped by user over every training + validation query, so
each query is scored out-of-fold and configurations are compared pairwise over
~700 queries instead of a small held-out week.
"""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd

import lightgbm as lgb

from benchmarks.common import coverage_fn, mlflow_run, per_query, save
from benchmarks.compare import paired_diff, statistic_diff
from recsys.config import RAW, load_split
from recsys.features.features import STREAMING, to_matrix
from recsys.training.ranker import RANKER_PARAMS
from recsys.training.train import datasets

FOLDS = 5
ROUNDS = 50  # ~ the early-stopped iteration count of the production rankers

GROUPS = {
    "popularity": ["item_interaction_count", "item_interaction_count_30d"],
    "recency": ["user_days_since_last", "user_days_since_first", "item_days_since_last", "item_days_since_first"],
    "affinity": ["genre_affinity"],
    "session": ["stream_session_events", "stream_session_duration_s", "stream_count_1h", "stream_count_24h",
                "stream_secs_since_last", "stream_in_session", "recent_item_sim"],
    "collaborative": ["als_score"],
}


def _groups(rows: pd.DataFrame) -> np.ndarray:
    return rows.groupby(["event_timestamp", "user_id"], sort=False).size().to_numpy()


def oof_scores(rows: pd.DataFrame, cols: list[str], seed: int) -> np.ndarray:
    """Out-of-fold ranker scores; folds by user so no user is in train and test at once."""
    fold = rows.user_id.to_numpy() % FOLDS
    scores = np.empty(len(rows))
    params = {k: v for k, v in RANKER_PARAMS.items() if k not in ("metric", "eval_at")} | {"seed": seed}
    for f in range(FOLDS):
        tr, te = rows[fold != f], rows[fold == f]
        b = lgb.train(params, lgb.Dataset(to_matrix(tr, cols), tr.label.to_numpy(), group=_groups(tr),
                                          feature_name=cols), num_boost_round=ROUNDS)
        scores[fold == f] = b.predict(to_matrix(te, cols), num_threads=1)
    return scores


def run() -> dict:
    split = load_split()
    tr, va, _ = datasets(STREAMING, "session")
    rows = pd.concat([tr, va], ignore_index=True).sort_values(
        ["event_timestamp", "user_id", "als_score", "item_id"], ascending=[True, True, False, True], ignore_index=True)
    ev = pd.read_parquet(RAW / "events.parquet", columns=["item_id", "timestamp"])
    catalog = int(ev[pd.to_datetime(ev.timestamp, utc=True) < split.validation_end].item_id.nunique())

    configs = {"all features": STREAMING.model_features}
    configs.update({f"- {g}": [c for c in STREAMING.model_features if c not in cols] for g, cols in GROUPS.items()})
    results, per_q = {}, {}
    for name, cols in configs.items():
        scored = rows.assign(score=oof_scores(rows, cols, split.seed))
        per_q[name] = per_query(scored, "score")
    per_q["ALS only (no ranker)"] = per_query(rows, "als_score")

    base = per_q["all features"]
    for name, pq in per_q.items():
        d = paired_diff("ndcg@10", base["ndcg@10"], pq["ndcg@10"])
        cov = statistic_diff("coverage@10", coverage_fn(base.top.tolist(), pq.top.tolist(), catalog), len(base), n_boot=300)
        r = {"ndcg@10": float(pq["ndcg@10"].mean()), "hit@10": float(pq["hit@10"].mean()), "coverage@10": cov.b,
             "delta_ndcg": d.as_dict(), "delta_coverage": cov.as_dict(), "removed": name}
        r["mlflow_run_id"] = mlflow_run("ablation", name, {"config": name, "queries": len(pq)},
                                        {"ndcg_at_10": r["ndcg@10"], "coverage_at_10": r["coverage@10"]})
        results[name] = r
    out = {"configs": results, "queries": int(len(base)), "catalog": catalog,
           "design": f"{FOLDS}-fold CV grouped by user, {ROUNDS} boosting rounds, session anchors "
                     f"{(split.train_end - timedelta(days=28)).date()} .. {split.validation_end.date()}"}
    save("ablation", out)
    return out


if __name__ == "__main__":
    import json

    print(json.dumps(run(), indent=2, default=float))
    _ = np
