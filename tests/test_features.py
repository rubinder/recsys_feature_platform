import math
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pytest
from pyspark.sql import functions as F

from recsys import schema
from recsys.feature_repo.definitions import user_item_affinity
from recsys.features.batch import item_features, user_features
from recsys.features.features import FEATURE_REFS, MODEL_FEATURES, to_matrix

UTC = timezone.utc


def test_model_features_follow_feature_refs():
    assert MODEL_FEATURES[:-1] == [r.split(":")[1] for r in FEATURE_REFS]
    assert MODEL_FEATURES[-1] == "als_score"


def test_to_matrix_column_order_and_nan():
    df = pd.DataFrame({c: [1.0] for c in MODEL_FEATURES}).assign(genre_affinity=[None])
    X = to_matrix(df)
    assert X.shape == (1, len(MODEL_FEATURES))
    assert math.isnan(X[0, MODEL_FEATURES.index("genre_affinity")])


def test_affinity_transform_handles_missing():
    udf = user_item_affinity.feature_transformation.udf
    out = udf(pd.DataFrame({"user_genre_share": [[0.5, 0.5], None], "item_genre_vector": [[1.0, 0.0], [1.0, 0.0]]}))
    assert out.genre_affinity[0] == 0.5 and math.isnan(out.genre_affinity[1])


def test_schema_validate_rejects_missing_columns():
    with pytest.raises(ValueError):
        schema.validate(pa.table({"user_id": [1]}), schema.EVENTS)


def test_snapshots_never_see_events_at_or_after_snapshot(spark):
    """The point-in-time guarantee at the source: snapshot d only aggregates events < d."""
    d1, d2 = datetime(2000, 1, 8, tzinfo=UTC), datetime(2000, 1, 15, tzinfo=UTC)
    events = spark.createDataFrame(
        [(1, 10, 5.0, datetime(2000, 1, 1, tzinfo=UTC)),
         (1, 11, 1.0, d1),  # exactly at d1: must only count in d2
         (1, 12, 3.0, datetime(2000, 1, 20, tzinfo=UTC))],  # after both
        "user_id long, item_id long, weight double, timestamp timestamp",
    )
    items = spark.createDataFrame([(10, ["A"]), (11, ["B"]), (12, ["A"])], "item_id long, genres array<string>")
    snaps = spark.createDataFrame([(d1,), (d2,)], "event_timestamp timestamp")
    stats, share = user_features(events, items, snaps, ["A", "B"])
    epoch = F.col("event_timestamp").cast("long").alias("ts")
    e1, e2 = int(d1.timestamp()), int(d2.timestamp())
    got = {r.ts: r for r in stats.select("*", epoch).collect()}
    assert got[e1].user_interaction_count == 1 and got[e1].user_avg_weight == 5.0
    assert got[e2].user_interaction_count == 2 and got[e2].user_avg_weight == 3.0
    shares = {r.ts: list(r.user_genre_share) for r in share.select("*", epoch).collect()}
    assert shares[e1] == [1.0, 0.0] and shares[e2] == [0.5, 0.5]
    pop, _ = item_features(events, items, snaps, ["A", "B"])
    assert {(r.item_id, r.ts) for r in pop.select("item_id", epoch).collect()} == {(10, e1), (10, e2), (11, e2)}
