"""Experiment 2: point-in-time join vs naive "latest feature value" join.

Knob: how training rows get their features. Everything else (anchors, labels,
candidates, model, seed) is identical. Both models are then evaluated the way
they would be served: features as of the serving cutoff, on the test period.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from benchmarks.common import mlflow_run, save
from benchmarks.compare import paired_diff
from recsys import scoring
from recsys.config import load_split
from recsys.eval import evaluate
from recsys.eval.metrics import ndcg_at_k
from recsys.feature_repo.store import get_store
from recsys.features.features import BATCH
from recsys.training import candidates
from recsys.training.train import datasets, fit, load_events

LEAKY = ["user_interaction_count", "user_interaction_count_30d", "item_interaction_count", "item_interaction_count_30d",
         "user_days_since_last", "item_days_since_last"]


def run() -> dict:
    split, store, events = load_split(), get_store(), load_events()
    cols = BATCH.model_features
    models, data = {}, {}
    for join in ("pit", "latest"):
        tr, va, _ = datasets(BATCH, "weekly", join, events)
        models[join], data[join] = fit(tr, va, cols, split.seed), (tr, va)

    # How much future information did the naive join inject?
    tr_pit, tr_latest = data["pit"][0], data["latest"][0]
    key = ["user_id", "item_id", "event_timestamp"]
    m = tr_pit[key + LEAKY + ["label"]].merge(tr_latest[key + LEAKY], on=key, suffixes=("_pit", "_latest"))
    changed = np.zeros(len(m), bool)
    for c in LEAKY:
        changed |= ~np.isclose(m[f"{c}_pit"].astype(float), m[f"{c}_latest"].astype(float), equal_nan=True)
    pos = m.label == 1
    leak = {
        "train_rows_with_future_values": float(changed.mean()),
        "item_count_gain_positive_rows": float((m.item_interaction_count_latest - m.item_interaction_count_pit)[pos].mean()),
        "item_count_gain_negative_rows": float((m.item_interaction_count_latest - m.item_interaction_count_pit)[~pos].mean()),
    }

    # Serve both on the test period with correct (cutoff-time) features.
    final_als = candidates.fit(events[events.timestamp < split.validation_end], split.seed)
    truth = evaluate.ground_truth(events, split.validation_end)
    users = sorted(u for u in truth if u in final_als.uidx)
    rows = evaluate.prepare(store, evaluate.candidate_frame(final_als, users, split.validation_end, split.candidates_k),
                            BATCH, final_als, split.validation_end)
    per_user = {}
    for join, b in models.items():
        rows[f"score_{join}"] = scoring.score(b, rows, cols)
        ranked = rows.sort_values(["user_id", f"score_{join}", "item_id"], ascending=[True, False, True])
        lists = ranked.groupby("user_id").item_id.agg(list)
        per_user[join] = np.array([ndcg_at_k(lists[u], truth[u], 10) for u in lists.index])

    res = {"configs": {}, "leakage": leak}
    for join, b in models.items():
        cfg = {"val_ndcg@10": float(b.best_score["validation"]["ndcg@10"]), "test_ndcg@10": float(per_user[join].mean()),
               "best_iteration": int(b.best_iteration)}
        cfg["optimism"] = cfg["val_ndcg@10"] - cfg["test_ndcg@10"]
        cfg["mlflow_run_id"] = mlflow_run("pit", join, {"join": join, "feature_set": "batch", "anchors": "weekly"}, cfg)
        res["configs"][join] = cfg
    res["test_delta"] = paired_diff("test_ndcg@10", per_user["pit"], per_user["latest"]).as_dict()
    res["users"] = int(len(per_user["pit"]))
    save("pit", res)
    return res


if __name__ == "__main__":
    import json

    print(json.dumps(run(), indent=2, default=float))
    _ = pd
