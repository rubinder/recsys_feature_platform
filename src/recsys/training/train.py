"""End-to-end training with MLflow tracking.

1. Ranker training rows: weekly anchors in the last `ranker_window_days` of the
   training period; validation rows: weekly anchors in the validation period.
   ALS is refit per anchor on events before it, exactly as serving would.
2. Feast get_historical_features joins point-in-time features onto every row;
   with the streaming feature set, request-time derived features are added per
   anchor from that anchor's ALS model.
3. LightGBM LambdaRank trains with early stopping on validation NDCG.
4. Final ALS is fit on everything before validation_end (serving "now"), and
   both ALS-only and two-stage are evaluated on the test period.
5. Everything is logged to one MLflow run; serving loads models by its run id.

Usage: python -m recsys.training.train [--feature-set batch|streaming]
"""

from __future__ import annotations

import argparse
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
from recsys.features.features import FEATURE_SETS, FeatureSet, to_matrix
from recsys.training import candidates, labels, ranker

log = logging.getLogger(__name__)
SORT = (["event_timestamp", "user_id", "als_score", "item_id"], [True, True, False, True])


def variant_name(feature_set: str, anchor_mode: str) -> str:
    return feature_set if anchor_mode == "weekly" else f"{feature_set}_session"


def run_id_file(variant: str) -> Path:
    """Where the latest run id per variant is recorded (Phase 1's batch/weekly keeps its name)."""
    return ARTIFACTS / ("latest_run_id" if variant == "batch" else f"latest_run_id_{variant}")


def _rows(events, anchor_list, horizon_end, split, store, fs: FeatureSet, anchor_mode: str, latest=None):
    frames, stats = [], []
    for t in anchor_list:
        if anchor_mode == "weekly":
            end = min(t + pd.Timedelta(days=split.label_window_days), horizon_end)
            rows, s, model = labels.build_rows(events, t, end, split.candidates_k, split.seed)
            frames.append(evaluate.prepare(store, rows, fs, model, t, latest))
        else:
            week_end = t + pd.Timedelta(days=split.snapshot_cadence_days)
            rows, s, model = labels.build_session_rows(
                events, t, week_end, horizon_end, split.label_window_days, split.candidates_k, split.seed)
            frames.append(evaluate.prepare(store, rows, fs, model, None, latest))
        stats.append(s)
    rows = pd.concat(frames, ignore_index=True)
    return rows.sort_values(SORT[0], ascending=SORT[1], ignore_index=True), stats


def _groups(rows: pd.DataFrame) -> np.ndarray:
    return rows.groupby(["event_timestamp", "user_id"], sort=False).size().to_numpy()


def popular_items(t, n: int = 100) -> list[int]:
    """Cold-start fallback: most interacted items in the snapshot at t."""
    pop = pd.read_parquet(OFFLINE / "item_popularity.parquet")
    pop = pop[pd.to_datetime(pop.event_timestamp, utc=True) == pd.Timestamp(t)]
    return pop.sort_values(["item_interaction_count", "item_id"], ascending=[False, True]).item_id.head(n).tolist()


def load_events() -> pd.DataFrame:
    events = pd.read_parquet(RAW / "events.parquet")
    events["timestamp"] = pd.to_datetime(events["timestamp"], utc=True)
    return events


def datasets(fs: FeatureSet, anchor_mode: str = "weekly", join: str = "pit", events: pd.DataFrame | None = None):
    """Training and validation rows with features. join="latest" is the naive (leaky) join, for benchmarks."""
    split = load_split()
    store = get_store()
    events = load_events() if events is None else events
    latest = split.validation_end if join == "latest" else None
    train_anchors = labels.anchors(split.train_end, split.ranker_window_days, split.snapshot_cadence_days)
    val_anchors = labels.anchors(split.validation_end, split.ranker_window_days, split.snapshot_cadence_days)
    train_rows, train_stats = _rows(events, train_anchors, split.train_end, split, store, fs, anchor_mode, latest)
    val_rows, val_stats = _rows(events, val_anchors, split.validation_end, split, store, fs, anchor_mode, latest)
    log.info("feature set %s (%s anchors, %s join): %d train rows, %d validation rows",
             fs.name, anchor_mode, join, len(train_rows), len(val_rows))
    return train_rows, val_rows, train_stats + val_stats


def fit(train_rows: pd.DataFrame, val_rows: pd.DataFrame, cols: list[str], seed: int):
    return ranker.fit(
        to_matrix(train_rows, cols), train_rows.label.to_numpy(), _groups(train_rows),
        to_matrix(val_rows, cols), val_rows.label.to_numpy(), _groups(val_rows), cols, seed)


def train(fs: FeatureSet, anchor_mode: str = "weekly") -> tuple[str, dict]:
    split = load_split()
    store = get_store()
    events = load_events()
    train_rows, val_rows, all_stats = datasets(fs, anchor_mode, "pit", events)
    n_train_anchors = len(labels.anchors(split.train_end, split.ranker_window_days, split.snapshot_cadence_days))
    train_stats, val_stats = all_stats[:n_train_anchors], all_stats[n_train_anchors:]

    cols = fs.model_features
    booster = fit(train_rows, val_rows, cols, split.seed)
    best = booster.best_score["validation"]
    log.info("ranker best iteration %d, validation %s", booster.best_iteration, dict(best))

    final_als = candidates.fit(events[events.timestamp < split.validation_end], split.seed)
    metrics = evaluate.run(events, final_als, booster, store, split.validation_end, split.candidates_k, split.eval_k, fs)
    metrics.update({f"val_{k.replace('@', '_at_')}": v for k, v in best.items()})
    metrics.update({"ranker_best_iteration": booster.best_iteration, "train_rows": len(train_rows), "val_rows": len(val_rows),
                    "train_queries": float(len(_groups(train_rows))), "train_positive_rate": float(train_rows.label.mean())})

    tracking.setup()
    variant = variant_name(fs.name, anchor_mode)
    with mlflow.start_run(run_name=f"two-stage-{variant}") as run:
        mlflow.log_params({f"split.{k}": v for k, v in split.model_dump(mode="json").items()})
        mlflow.log_params({f"als.{k}": v for k, v in candidates.ALS_PARAMS.items()})
        mlflow.log_params({f"ranker.{k}": v for k, v in ranker.RANKER_PARAMS.items()})
        mlflow.log_params({"feature_set": fs.name, "anchor_mode": anchor_mode})
        mlflow.log_metrics(metrics)
        mlflow.set_tags({"feast.feature_refs": ",".join(fs.refs), "feature_set": fs.name, "variant": variant, "phase": "2"})
        importance = dict(zip(cols, booster.feature_importance("gain").round(1).tolist()))
        with tempfile.TemporaryDirectory() as d:
            d = Path(d)
            final_als.save(d / "als.npz")
            ranker.save(booster, d / "ranker.txt")
            manifest = {
                "feature_set": fs.name,
                "anchor_mode": anchor_mode,
                "variant": variant,
                "feature_refs": fs.refs,
                "model_features": cols,
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
    run_id_file(variant).write_text(run_id)
    return run_id, metrics


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--feature-set", default="batch", choices=sorted(FEATURE_SETS))
    ap.add_argument("--anchors", default="weekly", choices=["weekly", "session"])
    args = ap.parse_args()
    run_id, metrics = train(FEATURE_SETS[args.feature_set], args.anchors)
    log.info("MLflow run %s (variant %s)", run_id, variant_name(args.feature_set, args.anchors))
    for k, v in sorted(metrics.items()):
        log.info("  %-40s %s", k, round(v, 4) if isinstance(v, float) else v)


if __name__ == "__main__":
    main()
