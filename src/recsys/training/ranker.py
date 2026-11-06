"""Stage 2: LightGBM LambdaRank over Feast features + ALS score."""

from __future__ import annotations

from pathlib import Path

import lightgbm as lgb
import numpy as np

RANKER_PARAMS = {
    "objective": "lambdarank",
    "metric": "ndcg",
    "eval_at": [10, 50],
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_data_in_leaf": 50,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "lambdarank_truncation_level": 50,
    "num_threads": 4,
    "deterministic": True,
    "force_row_wise": True,
    "verbosity": -1,
}
NUM_ROUNDS = 500
EARLY_STOPPING = 30


def fit(X, y, group, X_val, y_val, group_val, feature_names: list[str], seed: int) -> lgb.Booster:
    params = {**RANKER_PARAMS, "seed": seed}
    train = lgb.Dataset(X, y, group=group, feature_name=feature_names, free_raw_data=False)
    val = lgb.Dataset(X_val, y_val, group=group_val, reference=train)
    return lgb.train(
        params,
        train,
        num_boost_round=NUM_ROUNDS,
        valid_sets=[val],
        valid_names=["validation"],
        callbacks=[lgb.early_stopping(EARLY_STOPPING, first_metric_only=True, verbose=False)],
    )


def save(booster: lgb.Booster, path: Path) -> None:
    booster.save_model(str(path), num_iteration=booster.best_iteration)


def load(path: Path) -> lgb.Booster:
    return lgb.Booster(model_file=str(path))


def predict(booster: lgb.Booster, X: np.ndarray) -> np.ndarray:
    return booster.predict(X, num_threads=1)
