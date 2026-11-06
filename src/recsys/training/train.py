"""End-to-end training with MLflow tracking.

1. Ranker training rows: weekly anchors in the last `ranker_window_days` of the
   training period; validation rows: weekly anchors in the validation period.
   ALS is refit per anchor on events before it, exactly as serving would.
2. Feast get_historical_features joins point-in-time features onto every row.
3. LightGBM LambdaRank trains with early stopping on validation NDCG.
4. Final ALS is fit on everything before validation_end (serving "now"), and
   both ALS-only and two-stage are evaluated on the test period.
5. Everything is logged to one MLflow run; serving loads models by its run id.
"""

from __future__ import annotations

import json
import logging
import tempfile
from dataclasses import asdict
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd

from recsys import tracking
from recsys.config import ARTIFACTS, OFFLINE, RAW, load_split
from recsys.eval import evaluate
from recsys.feature_repo.store import get_store
from recsys.features.features import FEATURE_REFS, MODEL_FEATURES, to_matrix
from recsys.training import candidates, labels, ranker

log = logging.getLogger(__name__)
LATEST_RUN = ARTIFACTS / "latest_run_id"


def _rows(events, anchor_list, horizon_end, split):
    frames, stats = [], []
    for t in anchor_list:
        end = min(t + pd.Timedelta(days=split.label_window_days), horizon_end)
        rows, s = labels.build_rows(events, t, end, split.candidates_k, split.seed)
        frames.append(rows)
        stats.append(s)
    rows = pd.concat(frames, ignore_index=True).sort_values(["event_timestamp", "user_id", "als_score", "item_id"], ascending=[True, True, False, True], ignore_index=True)
    return rows, stats


def _groups(rows: pd.DataFrame) -> np.ndarray:
    return rows.groupby(["event_timestamp", "user_id"], sort=False).size().to_numpy()


def popular_items(t, n: int = 100) -> list[int]:
    """Cold-start fallback: most interacted items in the snapshot at t."""
    pop = pd.read_parquet(OFFLINE / "item_popularity.parquet")
    pop = pop[pd.to_datetime(pop.event_timestamp, utc=True) == pd.Timestamp(t)]
    return pop.sort_values(["item_interaction_count", "item_id"], ascending=[False, True]).item_id.head(n).tolist()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    split = load_split()
    store = get_store()
    events = pd.read_parquet(RAW / "events.parquet")
    events["timestamp"] = pd.to_datetime(events["timestamp"], utc=True)

    train_anchors = labels.anchors(split.train_end, split.ranker_window_days, split.snapshot_cadence_days)
    val_anchors = labels.anchors(split.validation_end, split.ranker_window_days, split.snapshot_cadence_days)
    train_rows, train_stats = _rows(events, train_anchors, split.train_end, split)
    val_rows, val_stats = _rows(events, val_anchors, split.validation_end, split)

    log.info("point-in-time feature join: %d train rows, %d validation rows", len(train_rows), len(val_rows))
    train_rows = evaluate.with_features(store, train_rows).sort_values(["event_timestamp", "user_id", "als_score", "item_id"], ascending=[True, True, False, True], ignore_index=True)
    val_rows = evaluate.with_features(store, val_rows).sort_values(["event_timestamp", "user_id", "als_score", "item_id"], ascending=[True, True, False, True], ignore_index=True)

    booster = ranker.fit(
        to_matrix(train_rows), train_rows.label.to_numpy(), _groups(train_rows),
        to_matrix(val_rows), val_rows.label.to_numpy(), _groups(val_rows),
        MODEL_FEATURES, split.seed,
    )
    best = booster.best_score["validation"]
    log.info("ranker best iteration %d, validation %s", booster.best_iteration, dict(best))

    final_als = candidates.fit(events[events.timestamp < split.validation_end], split.seed)
    metrics = evaluate.run(events, final_als, booster, store, split.validation_end, split.candidates_k, split.eval_k)
    metrics.update({f"val_{k.replace('@', '_at_')}": v for k, v in best.items()})
    metrics.update({"ranker_best_iteration": booster.best_iteration, "train_rows": len(train_rows), "val_rows": len(val_rows),
                    "train_queries": float(len(_groups(train_rows))), "train_positive_rate": float(train_rows.label.mean())})

    tracking.setup()
    with mlflow.start_run(run_name="two-stage") as run:
        mlflow.log_params({f"split.{k}": v for k, v in split.model_dump(mode="json").items()})
        mlflow.log_params({f"als.{k}": v for k, v in candidates.ALS_PARAMS.items()})
        mlflow.log_params({f"ranker.{k}": v for k, v in ranker.RANKER_PARAMS.items()})
        mlflow.log_metrics(metrics)
        mlflow.set_tags({"feast.feature_refs": ",".join(FEATURE_REFS), "phase": "1"})
        importance = dict(zip(MODEL_FEATURES, booster.feature_importance("gain").round(1).tolist()))
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            final_als.save(d / "als.npz")
            ranker.save(booster, d / "ranker.txt")
            manifest = {
                "feature_refs": FEATURE_REFS,
                "model_features": MODEL_FEATURES,
                "serving_as_of": split.validation_end.isoformat(),
                "candidates_k": split.candidates_k,
                "top_n": split.top_n,
                "popular_items": popular_items(split.validation_end),
                "feature_importance_gain": importance,
                "label_stats": [asdict(s) for s in train_stats + val_stats],
                "genres": json.loads((OFFLINE / "genres.json").read_text()),
            }
            (d / "manifest.json").write_text(json.dumps(manifest, indent=2))
            mlflow.log_artifacts(str(d), artifact_path="model")
        run_id = run.info.run_id

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    LATEST_RUN.write_text(run_id)
    log.info("MLflow run %s", run_id)
    for k, v in sorted(metrics.items()):
        log.info("  %-40s %s", k, round(v, 4) if isinstance(v, float) else v)


if __name__ == "__main__":
    main()
