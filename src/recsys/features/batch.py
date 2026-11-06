"""PySpark weekly user/item feature snapshots, each stamped with event_timestamp.

For every snapshot date d on the weekly grid, each feature is computed only from
events with timestamp < d and stamped event_timestamp = d. That history is what
lets Feast do point-in-time joins: a training row at time t only ever sees the
latest snapshot at or before t.
"""

from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime, timedelta

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from recsys.config import OFFLINE, RAW, load_split

log = logging.getLogger(__name__)
DAY = 86400.0


def snapshot_dates(first_event: datetime, last: datetime, cadence_days: int) -> list[datetime]:
    """Weekly grid ending exactly at `last`, going back to just after the first event."""
    dates, d = [], last
    while d > first_event:
        dates.append(d)
        d -= timedelta(days=cadence_days)
    return sorted(dates)


def genre_vocabulary(items: DataFrame) -> list[str]:
    rows = items.select(F.explode("genres").alias("g")).distinct().collect()
    return sorted(r.g for r in rows)


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[4]")
        .appName("recsys-batch-features")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def _as_of(events: DataFrame, snaps: DataFrame) -> DataFrame:
    """Every (snapshot, event) pair where the event happened strictly before the snapshot."""
    return events.join(F.broadcast(snaps), events.timestamp < snaps.event_timestamp)


def user_features(events: DataFrame, items: DataFrame, snaps: DataFrame, genres: list[str]):
    e = _as_of(events, snaps)
    age = (F.col("event_timestamp").cast("double") - F.col("timestamp").cast("double")) / DAY
    stats = e.groupBy("user_id", "event_timestamp").agg(
        F.count("*").cast("long").alias("user_interaction_count"),
        F.sum(F.when(age <= 30, 1).otherwise(0)).cast("long").alias("user_interaction_count_30d"),
        F.avg("weight").alias("user_avg_weight"),
        F.min(age).alias("user_days_since_last"),
        F.max(age).alias("user_days_since_first"),
    )

    per_genre = (
        e.join(items.select("item_id", "genres"), "item_id")
        .select("user_id", "event_timestamp", F.explode("genres").alias("genre"))
        .groupBy("user_id", "event_timestamp", "genre")
        .count()
    )
    totals = per_genre.groupBy("user_id", "event_timestamp").agg(F.sum("count").alias("total"))
    share = (
        per_genre.join(totals, ["user_id", "event_timestamp"])
        .groupBy("user_id", "event_timestamp")
        .agg(F.map_from_entries(F.collect_list(F.struct("genre", F.col("count") / F.col("total")))).alias("m"))
        .select(
            "user_id",
            "event_timestamp",
            F.array(*[F.coalesce(F.col("m")[g], F.lit(0.0)).cast("float") for g in genres]).alias(
                "user_genre_share"
            ),
        )
    )
    return stats, share


def item_features(events: DataFrame, items: DataFrame, snaps: DataFrame, genres: list[str]):
    e = _as_of(events, snaps)
    age = (F.col("event_timestamp").cast("double") - F.col("timestamp").cast("double")) / DAY
    agg = e.groupBy("item_id", "event_timestamp").agg(
        F.count("*").cast("long").alias("item_interaction_count"),
        F.sum(F.when(age <= 30, 1).otherwise(0)).cast("long").alias("item_interaction_count_30d"),
        F.avg("weight").alias("item_avg_weight"),
        F.min(age).alias("item_days_since_last"),
        F.max(age).alias("item_days_since_first"),
    )
    popularity = agg.select("item_id", "event_timestamp", "item_interaction_count", "item_interaction_count_30d")
    onehot = items.select(
        "item_id",
        F.array(*[F.when(F.array_contains("genres", g), 1.0).otherwise(0.0).cast("float") for g in genres]).alias(
            "item_genre_vector"
        ),
    )
    stats = agg.select(
        "item_id", "event_timestamp", "item_avg_weight", "item_days_since_last", "item_days_since_first"
    ).join(onehot, "item_id")
    return popularity, stats


def _write(df: DataFrame, name: str, key: str) -> None:
    """Write one deterministic, sorted Parquet file per feature view."""
    tmp = OFFLINE / f"_{name}"
    df.orderBy(key, "event_timestamp").coalesce(1).write.mode("overwrite").parquet(str(tmp))
    part = next(tmp.glob("part-*.parquet"))
    dest = OFFLINE / f"{name}.parquet"
    shutil.move(str(part), dest)
    shutil.rmtree(tmp)
    log.info("wrote %s: %d rows", dest.name, pq.ParquetFile(dest).metadata.num_rows)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    split = load_split()
    OFFLINE.mkdir(parents=True, exist_ok=True)
    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    events = spark.read.parquet(str(RAW / "events.parquet"))
    items = spark.read.parquet(str(RAW / "items.parquet"))
    first = events.agg(F.min("timestamp")).first()[0]
    dates = snapshot_dates(first.replace(tzinfo=split.validation_end.tzinfo), split.validation_end, split.snapshot_cadence_days)
    snaps = spark.createDataFrame([(d,) for d in dates], "event_timestamp timestamp")
    genres = genre_vocabulary(items)
    log.info("%d snapshots %s .. %s, %d genres", len(dates), dates[0].date(), dates[-1].date(), len(genres))

    user_stats, user_genre = user_features(events, items, snaps, genres)
    item_pop, item_stats = item_features(events, items, snaps, genres)
    _write(user_stats, "user_stats", "user_id")
    _write(user_genre, "user_genre_affinity", "user_id")
    _write(item_pop, "item_popularity", "item_id")
    _write(item_stats, "item_stats", "item_id")
    (OFFLINE / "genres.json").write_text(json.dumps(genres))
    spark.stop()


if __name__ == "__main__":
    main()
