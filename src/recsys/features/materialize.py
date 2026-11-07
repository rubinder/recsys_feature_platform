"""Materialize Feast features into Redis as of the serving "now" (validation_end).

Local profile: Feast's built-in `materialize`.
Cloud profile (Snowflake offline store): Feast's built-in materializer passes
Snowflake ARRAY columns through as JSON text, which breaks array features in
the online write. So the cloud path does the same steps explicitly through
Feast's public interfaces: pull the latest row per entity from the offline
store (the call `materialize` itself makes), normalize array columns to lists,
and `write_to_online_store` per feature view.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from recsys.config import load_split
from recsys.feature_repo.store import get_store

log = logging.getLogger(__name__)


def materialize_normalized(store, start: datetime, end: datetime) -> dict[str, int]:
    from recsys.features.features import normalize_arrays

    offline = store._get_provider().offline_store
    counts = {}
    for fv in store.list_batch_feature_views():
        if not fv.online:
            continue
        src = fv.batch_source
        job = offline.pull_latest_from_table_or_query(
            config=store.config, data_source=src, join_key_columns=fv.join_keys,
            feature_name_columns=[f.name for f in fv.features], timestamp_field=src.timestamp_field,
            created_timestamp_column=src.created_timestamp_column or None, start_date=start, end_date=end,
        )
        df = normalize_arrays(job.to_df())
        if src.timestamp_field != "event_timestamp" and src.timestamp_field in df:
            df = df.rename(columns={src.timestamp_field: "event_timestamp"})
        store.write_to_online_store(fv.name, df)
        counts[fv.name] = len(df)
        log.info("materialized %s: %d entities", fv.name, len(df))
    return counts


def main() -> None:
    from recsys.cloud import is_cloud

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    split = load_split()
    end = split.validation_end
    # Must cover the longest feature-view TTL so every entity that the offline
    # store returns at `end` is also loaded online (user_stream ttl = 365 days).
    start = end - timedelta(days=365)
    store = get_store()
    if is_cloud():
        materialize_normalized(store, start, end + timedelta(seconds=1))
    else:
        store.materialize(start_date=start, end_date=end + timedelta(seconds=1))
    log.info("materialized features as of %s into Redis", end.isoformat())


if __name__ == "__main__":
    main()
