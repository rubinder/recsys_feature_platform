"""Ranker scoring shared by offline evaluation and the serving API."""

from __future__ import annotations

import lightgbm as lgb
import numpy as np
import pandas as pd

from recsys.features.features import MODEL_FEATURES, to_matrix
from recsys.training import ranker


def score(booster: lgb.Booster, features: pd.DataFrame, columns: list[str] = MODEL_FEATURES) -> np.ndarray:
    """Ranker scores for candidate rows that carry the model's feature columns."""
    return ranker.predict(booster, to_matrix(features, columns))
