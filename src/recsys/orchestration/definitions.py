"""Dagster Definitions: assets, checks, jobs, schedule, sensor.

    dagster dev -m recsys.orchestration.definitions        (make dagster-dev)
"""

from __future__ import annotations

import dagster as dg

from recsys.orchestration.assets import ALL_ASSETS, LANDING, impression_logs, online_store, rec_quality_report
from recsys.orchestration.checks import DQ_CHECKS, FRESHNESS_CHECKS
from recsys.orchestration.registry_assets import REGISTRY_ASSETS, fresh_features_retrain, retrain_job

batch_pipeline = dg.define_asset_job(
    "batch_pipeline",
    selection=dg.AssetSelection.all() - dg.AssetSelection.assets(impression_logs.key, rec_quality_report, *REGISTRY_ASSETS),
    description="raw ingest -> features -> Feast registry -> online store -> rankers -> offline metrics",
    config={"execution": {"config": {"multiprocess": {"max_concurrent": 2}}}},
)
freshness_job = dg.define_asset_job(
    "freshness_checks", selection=dg.AssetSelection.checks(*FRESHNESS_CHECKS),
    description="Online freshness SLAs only (no materialization).",
)
measurement_job = dg.define_asset_job("measurement", selection=dg.AssetSelection.assets(rec_quality_report))

weekly = dg.ScheduleDefinition(job=batch_pipeline, cron_schedule="0 3 * * 1", name="weekly_batch",
                               description="Weekly, matching the feature snapshot cadence.")
hourly_freshness = dg.ScheduleDefinition(job=freshness_job, cron_schedule="15 * * * *", name="hourly_freshness")


@dg.sensor(job=batch_pipeline, minimum_interval_seconds=30,
           description="New dataset dropped in data/landing/*.zip -> rerun the batch pipeline on it.")
def new_raw_data(context: dg.SensorEvaluationContext):
    LANDING.mkdir(parents=True, exist_ok=True)
    seen = set((context.cursor or "").split("|")) - {""}
    new = sorted(p for p in LANDING.glob("*.zip") if f"{p.name}:{p.stat().st_mtime_ns}" not in seen)
    for p in new:
        key = f"{p.name}:{p.stat().st_mtime_ns}"
        seen.add(key)
        yield dg.RunRequest(run_key=key, run_config={"ops": {"raw_events": {"config": {"landed_zip": str(p)}}}})
    context.update_cursor("|".join(sorted(seen)))
    if not new:
        yield dg.SkipReason("no new files in data/landing")


@dg.sensor(job=measurement_job, minimum_interval_seconds=60, description="Impression logs changed -> refresh the report.")
def logs_changed(context: dg.SensorEvaluationContext):
    from recsys.serving.impressions import LOGS

    files = sorted((LOGS / "impressions").glob("*/*.parquet"))
    version = f"{len(files)}:{max((f.stat().st_mtime_ns for f in files), default=0)}"
    if files and version != context.cursor:
        context.update_cursor(version)
        yield dg.RunRequest(run_key=version)


defs = dg.Definitions(
    assets=ALL_ASSETS + REGISTRY_ASSETS,
    asset_checks=DQ_CHECKS + FRESHNESS_CHECKS,
    jobs=[batch_pipeline, freshness_job, measurement_job, retrain_job],
    schedules=[weekly, hourly_freshness],
    sensors=[new_raw_data, logs_changed, fresh_features_retrain],
)
