"""Feature references and model inputs, shared by training and serving.

Both paths import these constants and `to_matrix`, so the ranker always sees
the same columns in the same order. That shared code is half of the
train/serve consistency story; Feast's shared definitions are the other half.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

FEATURE_REFS: list[str] = [
    "user_stats:user_interaction_count",
    "user_stats:user_interaction_count_30d",
    "user_stats:user_avg_weight",
    "user_stats:user_days_since_last",
    "user_stats:user_days_since_first",
    "item_popularity:item_interaction_count",
    "item_popularity:item_interaction_count_30d",
    "item_stats:item_avg_weight",
    "item_stats:item_days_since_last",
    "item_stats:item_days_since_first",
    "user_item_affinity:genre_affinity",
]

# Feature groups for ablation (Phase 4). Values are model-input column names.
GROUPS: dict[str, list[str]] = {
    "user_activity": [
        "user_interaction_count",
        "user_interaction_count_30d",
        "user_avg_weight",
        "user_days_since_last",
        "user_days_since_first",
    ],
    "popularity": ["item_interaction_count", "item_interaction_count_30d"],
    "item_quality_recency": ["item_avg_weight", "item_days_since_last", "item_days_since_first"],
    "affinity": ["genre_affinity"],
    "collaborative": ["als_score"],
}

MODEL_FEATURES: list[str] = [ref.split(":")[1] for ref in FEATURE_REFS] + ["als_score"]


def to_matrix(df: pd.DataFrame, columns: list[str] = MODEL_FEATURES) -> np.ndarray:
    """Model input matrix in a fixed column order; missing values become NaN."""
    return df[columns].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=np.float64)
