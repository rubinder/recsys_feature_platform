"""Feast entities, sources, and feature views (grouped for later ablation).

These definitions are the single source of truth for every feature. Training
reads them point-in-time from the offline store (get_historical_features);
serving reads the same views from Redis (get_online_features). The on-demand
view `user_item_affinity` runs the same Python transform in both paths.
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Array, Float32, Float64, Int64

from recsys.config import OFFLINE

TTL = timedelta(days=14)  # snapshots are weekly; 14d tolerates one missed run

user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64, description="End user")
item = Entity(name="item", join_keys=["item_id"], value_type=ValueType.INT64, description="Catalog item")


def _source(name: str) -> FileSource:
    return FileSource(name=f"{name}_source", path=str(OFFLINE / f"{name}.parquet"), timestamp_field="event_timestamp")


user_stats = FeatureView(
    name="user_stats",
    entities=[user],
    ttl=TTL,
    schema=[
        Field(name="user_interaction_count", dtype=Int64),
        Field(name="user_interaction_count_30d", dtype=Int64),
        Field(name="user_avg_weight", dtype=Float64),
        Field(name="user_days_since_last", dtype=Float64),
        Field(name="user_days_since_first", dtype=Float64),
    ],
    source=_source("user_stats"),
    tags={"group": "user_activity"},
)

user_genre_affinity = FeatureView(
    name="user_genre_affinity",
    entities=[user],
    ttl=TTL,
    schema=[Field(name="user_genre_share", dtype=Array(Float32))],
    source=_source("user_genre_affinity"),
    tags={"group": "affinity"},
)

item_popularity = FeatureView(
    name="item_popularity",
    entities=[item],
    ttl=TTL,
    schema=[
        Field(name="item_interaction_count", dtype=Int64),
        Field(name="item_interaction_count_30d", dtype=Int64),
    ],
    source=_source("item_popularity"),
    tags={"group": "popularity"},
)

item_stats = FeatureView(
    name="item_stats",
    entities=[item],
    ttl=TTL,
    schema=[
        Field(name="item_avg_weight", dtype=Float64),
        Field(name="item_days_since_last", dtype=Float64),
        Field(name="item_days_since_first", dtype=Float64),
        Field(name="item_genre_vector", dtype=Array(Float32)),
    ],
    source=_source("item_stats"),
    tags={"group": "item_quality_recency"},
)


@on_demand_feature_view(
    sources=[user_genre_affinity, item_stats],
    schema=[Field(name="genre_affinity", dtype=Float64)],
    mode="pandas",
)
def user_item_affinity(inputs: pd.DataFrame) -> pd.DataFrame:
    """Share of the user's history in this item's genres (NaN if either side is missing).

    Self-contained on purpose: Feast serializes this function's source, so it
    cannot call module-level helpers.
    """
    import numpy as np
    import pandas as pd

    def dot(share, onehot):
        if share is None or onehot is None:
            return float("nan")
        s, o = np.asarray(share, dtype=float), np.asarray(onehot, dtype=float)
        return float(s @ o) if s.size and s.size == o.size else float("nan")

    return pd.DataFrame(
        {"genre_affinity": [dot(s, o) for s, o in zip(inputs["user_genre_share"], inputs["item_genre_vector"])]}
    )


OBJECTS = [user, item, user_stats, user_genre_affinity, item_popularity, item_stats, user_item_affinity]
