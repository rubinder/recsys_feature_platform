"""Asset checks: data quality (blocking) and online freshness SLAs."""

from __future__ import annotations

import dagster as dg

from recsys.orchestration.assets import batch_features, online_store, raw_events, streaming_backfill
from recsys.quality import checks, freshness

DQ = {
    raw_events: (checks.raw_events, ["raw_events.schema", "raw_events.referential_integrity", "raw_events.duplicate_rate"]),
    batch_features: (checks.batch_features, [
        "user_stats.schema", "item_popularity.schema", "item_stats.schema", "features.avg_weight_within_event_range",
        "features.genre_vectors", "features.null_rate", "features.referential_integrity", "features.snapshot_grid"]),
    streaming_backfill: (checks.streaming_features, [
        "user_stream.schema", "user_stream.timestamps_within_data_range", "user_stream.unique_timestamp_per_user"]),
}


def _check_name(n: str) -> str:
    return n.replace(".", "__")


def _result(o: checks.CheckOutcome, asset_key: dg.AssetKey) -> dg.AssetCheckResult:
    meta = {k: (dg.MetadataValue.json(v) if isinstance(v, (list, dict)) else v) for k, v in o.metadata.items() if v is not None}
    return dg.AssetCheckResult(asset_key=asset_key, check_name=_check_name(o.name), passed=o.passed,
                               severity=dg.AssetCheckSeverity.ERROR if o.severity == "ERROR" else dg.AssetCheckSeverity.WARN,
                               metadata=meta)


def _dq_checks(asset, fn, names):
    specs = [dg.AssetCheckSpec(_check_name(n), asset=asset, blocking=True,
                               description="Pandera / invariant check; failure blocks downstream assets.") for n in names]

    @dg.multi_asset_check(specs=specs, name=f"{asset.key.path[-1]}_quality")
    def _check():
        for o in fn():
            yield _result(o, asset.key)

    return _check


DQ_CHECKS = [_dq_checks(a, fn, names) for a, (fn, names) in DQ.items()]


@dg.asset_check(asset=online_store, description="Batch views in Redis no older than 8 days vs the serving clock.")
def batch_freshness() -> dg.AssetCheckResult:
    return _named(freshness.batch_freshness(), "batch_freshness")


@dg.asset_check(asset=online_store, description="Streaming lag (last produced vs processed event) within 300 s of event time.")
def stream_freshness() -> dg.AssetCheckResult:
    return _named(freshness.stream_freshness(), "stream_freshness")


def _named(o: checks.CheckOutcome, name: str) -> dg.AssetCheckResult:
    r = _result(o, online_store.key)
    return dg.AssetCheckResult(asset_key=r.asset_key, check_name=name, passed=r.passed, severity=r.severity, metadata=r.metadata)


FRESHNESS_CHECKS = [batch_freshness, stream_freshness]
