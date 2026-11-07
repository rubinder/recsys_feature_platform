"""Assets: thin wrappers over the Phase 1/2 functions (nothing is reimplemented here).

No `from __future__ import annotations`: Dagster resolves Config classes from
real annotations at definition time.
"""

import shutil
from pathlib import Path
from typing import Optional

import dagster as dg
import pandas as pd

from recsys.config import DATA, OFFLINE, RAW, load_split

RETRY = dg.RetryPolicy(max_retries=3, delay=5, backoff=dg.Backoff.EXPONENTIAL)
LANDING = DATA / "landing"


class RawConfig(dg.Config):
    landed_zip: Optional[str] = None  # set by the landing sensor; default: download / cached zip


def _rows(path: Path) -> int:
    import pyarrow.parquet as pq

    return sum(pq.ParquetFile(p).metadata.num_rows for p in ([path] if path.is_file() else path.glob("*.parquet")))


@dg.asset(group_name="ingest", retry_policy=RETRY, kinds={"python", "parquet"},
          description="MovieLens 1M normalized into the generic events / items / users schema (data/raw).")
def raw_events(context: dg.AssetExecutionContext, config: RawConfig) -> dg.MaterializeResult:
    from recsys.ingest import movielens

    if config.landed_zip:
        RAW.mkdir(parents=True, exist_ok=True)
        shutil.copy(config.landed_zip, RAW / "ml-1m.zip")
        context.log.info("using landed file %s", config.landed_zip)
    movielens.main()
    ev = pd.read_parquet(RAW / "events.parquet", columns=["timestamp"])
    return dg.MaterializeResult(metadata={
        "events": _rows(RAW / "events.parquet"), "items": _rows(RAW / "items.parquet"), "users": _rows(RAW / "users.parquet"),
        "min_timestamp": str(ev.timestamp.min()), "max_timestamp": str(ev.timestamp.max())})


@dg.asset(group_name="features", deps=[raw_events], retry_policy=RETRY, kinds={"spark", "parquet"},
          description="PySpark weekly point-in-time user/item feature snapshots (Feast offline store).")
def batch_features() -> dg.MaterializeResult:
    from recsys.features import batch

    batch.main()
    return dg.MaterializeResult(metadata={n: _rows(OFFLINE / f"{n}.parquet") for n in
                                          ("user_stats", "user_genre_affinity", "item_popularity", "item_stats")})


@dg.asset(group_name="features", deps=[raw_events], retry_policy=RETRY, kinds={"python", "parquet"},
          description="Streaming features over history with the same logic Flink runs (offline parts).")
def streaming_backfill() -> dg.MaterializeResult:
    from recsys.streaming import backfill

    backfill.main()
    return dg.MaterializeResult(metadata={"rows": _rows(OFFLINE / "user_stream")})


@dg.asset(group_name="feature_store", deps=[batch_features, streaming_backfill], retry_policy=RETRY, kinds={"feast"},
          description="Feast definitions registered (entities, batch views, push source, on-demand view).")
def feast_registry() -> dg.MaterializeResult:
    from recsys.feature_repo.store import apply, get_store

    apply()
    store = get_store()
    return dg.MaterializeResult(metadata={"feature_views": ", ".join(sorted(fv.name for fv in store.list_feature_views())),
                                          "on_demand_views": ", ".join(fv.name for fv in store.list_on_demand_feature_views())})


@dg.asset(group_name="feature_store", deps=[feast_registry], retry_policy=RETRY, kinds={"feast", "redis"},
          description="Redis online store materialized as of the serving cutoff.")
def online_store() -> dg.MaterializeResult:
    from recsys.features import materialize

    materialize.main()
    return dg.MaterializeResult(metadata={"as_of": load_split().validation_end.isoformat()})


def _ranker(name: str, feature_set: str, anchors: str, description: str):
    @dg.asset(name=name, group_name="training", deps=[feast_registry, raw_events], retry_policy=RETRY,
              kinds={"lightgbm", "mlflow"}, description=description)
    def _asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        from recsys.features.features import FEATURE_SETS
        from recsys.training.train import train

        run_id, m = train(FEATURE_SETS[feature_set], anchors)
        context.log.info("MLflow run %s", run_id)
        keys = ["test_two_stage_ndcg_at_10", "test_als_ndcg_at_10", "test_two_stage_hit_rate_at_10", "val_ndcg_at_10",
                "train_queries", "ranker_best_iteration"]
        return dg.MaterializeResult(metadata={"mlflow_run_id": run_id, **{k: float(m[k]) for k in keys}})

    return _asset


ranker_batch = _ranker("ranker_batch", "batch", "weekly", "Phase 1 baseline: batch features, weekly anchors.")
ranker_batch_session = _ranker("ranker_batch_session", "batch", "session", "Batch features, mid-session anchors.")
ranker_streaming_session = _ranker("ranker_streaming_session", "streaming", "session",
                                   "Batch + streaming features, mid-session anchors.")
RANKERS = [ranker_batch, ranker_batch_session, ranker_streaming_session]


@dg.asset(group_name="training", deps=RANKERS, kinds={"mlflow"},
          description="Offline test metrics of every ranker variant, compared side by side.")
def offline_metrics(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    import mlflow

    from recsys import tracking
    from recsys.serving.recommender import discover_variants

    tracking.setup()
    rows = []
    for variant, run_id in discover_variants().items():
        m = mlflow.get_run(run_id).data.metrics
        rows.append({"variant": variant, "run_id": run_id[:8], "ndcg@10": m.get("test_two_stage_ndcg_at_10"),
                     "hit@10": m.get("test_two_stage_hit_rate_at_10"), "val_ndcg@10": m.get("val_ndcg_at_10"),
                     "als_ndcg@10": m.get("test_als_ndcg_at_10")})
    table = pd.DataFrame(rows).round(4)
    return dg.MaterializeResult(metadata={"variants": dg.MetadataValue.md(table.to_markdown(index=False))})


@dg.observable_source_asset(group_name="measurement", description="Impression + interaction logs written by the API and simulator.")
def impression_logs() -> dg.ObserveResult:
    from recsys.serving.impressions import LOGS

    files = sorted((LOGS / "impressions").glob("*/*.parquet"))
    version = f"{len(files)}:{max((f.stat().st_mtime_ns for f in files), default=0)}"
    return dg.ObserveResult(data_version=dg.DataVersion(version), metadata={"files": len(files)})


@dg.asset(group_name="measurement", deps=[impression_logs], kinds={"duckdb"},
          description="Rec quality (CTR, hit-rate, NDCG, coverage) per variant from the logs. Simulated outcomes.")
def rec_quality_report() -> dg.MaterializeResult:
    from recsys.eval import report
    from recsys.serving.impressions import LOGS

    if not any((LOGS / "impressions").glob("*/*.parquet")):
        return dg.MaterializeResult(metadata={"status": "no impression logs yet (run `make simulate`)"})
    imp = report.load(report.ATTRIBUTION_HOURS)
    ev = pd.read_parquet(RAW / "events.parquet", columns=["item_id", "timestamp"])
    catalog = int(ev[pd.to_datetime(ev.timestamp, utc=True) < load_split().validation_end].item_id.nunique())
    s = report.summarize(imp, catalog).round(4)
    return dg.MaterializeResult(metadata={"impressions": len(imp), "outcomes": "SIMULATED (counterfactual replay)",
                                          "report": dg.MetadataValue.md(s.drop(columns=["model_version"]).to_markdown())})


ALL_ASSETS = [raw_events, batch_features, streaming_backfill, feast_registry, online_store, *RANKERS, offline_metrics,
              impression_logs, rec_quality_report]
