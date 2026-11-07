"""Feature sets and model inputs, shared by training and serving.

Both paths import these definitions and `to_matrix`, so a ranker always sees the
same columns in the same order. That shared code is half of the train/serve
consistency story; Feast's shared definitions are the other half.

A model is trained on one FeatureSet; its name is stored in the MLflow manifest
and the serving layer uses the same set for that model.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

BATCH_REFS: list[str] = [
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
STREAM_REFS: list[str] = [
    "user_stream:stream_session_events",
    "user_stream:stream_session_duration_s",
    "user_stream:stream_last_items",
    "user_stream:stream_count_1h",
    "user_stream:stream_count_24h",
    "user_stream:stream_gap_s",
    "user_stream:stream_last_event_ts",
]
# Item-keyed refs: batch-computed, slow-changing, safe to cache at serving time.
ITEM_REFS: list[str] = [r for r in BATCH_REFS if r.startswith(("item_popularity:", "item_stats:"))] + [
    "item_stats:item_genre_vector"
]
# Derived at request/anchor time from streaming features (recsys.features.derived).
DERIVED_STREAM: list[str] = ["stream_secs_since_last", "stream_in_session", "recent_item_sim"]

# Backwards-compatible names (Phase 1 code and tests).
FEATURE_REFS = BATCH_REFS


def _names(refs: list[str]) -> list[str]:
    return [r.split(":")[1] for r in refs]


@dataclass(frozen=True)
class FeatureSet:
    name: str
    refs: list[str]
    model_features: list[str]

    @property
    def uses_stream(self) -> bool:
        return any(r.startswith("user_stream:") for r in self.refs)


BATCH = FeatureSet("batch", BATCH_REFS, _names(BATCH_REFS) + ["als_score"])
STREAMING = FeatureSet(
    "streaming",
    BATCH_REFS + STREAM_REFS,
    _names(BATCH_REFS)
    + ["stream_session_events", "stream_session_duration_s", "stream_count_1h", "stream_count_24h"]
    + DERIVED_STREAM
    + ["als_score"],
)
FEATURE_SETS = {fs.name: fs for fs in (BATCH, STREAMING)}
MODEL_FEATURES = BATCH.model_features

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
    "session": ["stream_session_events", "stream_session_duration_s", "stream_count_1h", "stream_count_24h"]
    + DERIVED_STREAM,
    "collaborative": ["als_score"],
}


ARRAY_COLUMNS = ("user_genre_share", "item_genre_vector", "stream_last_items")


def normalize_arrays(df: pd.DataFrame) -> pd.DataFrame:
    """Array features as Python lists whatever the offline store: Snowflake returns them as JSON text."""
    import json

    for col in ARRAY_COLUMNS:
        if col in df.columns and df[col].map(lambda v: isinstance(v, str)).any():
            df[col] = df[col].map(lambda v: json.loads(v) if isinstance(v, str) else v)
    return df


def to_matrix(df: pd.DataFrame, columns: list[str] = MODEL_FEATURES) -> np.ndarray:
    """Model input matrix in a fixed column order; missing values become NaN."""
    return df[columns].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=np.float64)
