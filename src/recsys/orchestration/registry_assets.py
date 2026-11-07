"""Continuous training: retrain a challenger on fresh features, gate it, promote or reject.

No `from __future__ import annotations` (Dagster resolves Config annotations at definition time).
"""

import dagster as dg

from recsys.orchestration.assets import RETRY, feast_registry, raw_events


class RetrainConfig(dg.Config):
    feature_set: str = "streaming"
    anchors: str = "session"
    demo_bad_challenger: bool = False


@dg.asset(group_name="registry", deps=[feast_registry, raw_events], retry_policy=RETRY, kinds={"mlflow"},
          description="Challenger trained on the current features, registered in MLflow with alias `challenger`.")
def challenger_model(context: dg.AssetExecutionContext, config: RetrainConfig) -> dg.MaterializeResult:
    from recsys import registry
    from recsys.features.features import FEATURE_SETS
    from recsys.training.train import train, variant_name

    fs = FEATURE_SETS[config.feature_set]
    run_id, m = train(fs, config.anchors, record=False, demo_shuffle_labels=config.demo_bad_challenger)
    variant = variant_name(fs.name, config.anchors) + ("_shuffled" if config.demo_bad_challenger else "")
    v = registry.register(run_id, {"variant": variant, "val_ndcg_at_10": round(m["val_ndcg_at_10"], 4)})
    registry.set_alias("challenger", v)
    context.log.info("challenger v%s (%s)", v, variant)
    return dg.MaterializeResult(metadata={"registry_version": v, "mlflow_run_id": run_id, "variant": variant,
                                          "val_ndcg_at_10": float(m["val_ndcg_at_10"])})


@dg.asset(group_name="registry", deps=[challenger_model], kinds={"mlflow"},
          description="Gate (data quality + held-out non-inferiority) and the resulting promotion or rejection.")
def promotion_decision(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    from recsys import registry
    from recsys.registry.cli import _notify_serving, gate_and_decide

    v = registry.alias_version("challenger")
    promoted = gate_and_decide(v)
    if promoted:
        _notify_serving()
    info = next(x for x in registry.versions() if x["version"] == v)
    return dg.MaterializeResult(metadata={"challenger": v, "promoted": promoted,
                                          "champion": registry.alias_version("champion"),
                                          "gate": str(info.get("gate")), "stage": str(info.get("stage"))})


retrain_job = dg.define_asset_job(
    "retrain_and_promote", selection=dg.AssetSelection.assets(challenger_model, promotion_decision),
    description="Train challenger -> register -> gate -> promote or reject",
)


@dg.asset_sensor(asset_key=dg.AssetKey("batch_features"), job=retrain_job, minimum_interval_seconds=60,
                 description="Fresh batch features materialized -> retrain and gate a challenger.")
def fresh_features_retrain(context: dg.SensorEvaluationContext, asset_event: dg.EventLogEntry):
    yield dg.RunRequest(run_key=str(asset_event.storage_id))


REGISTRY_ASSETS = [challenger_model, promotion_decision]
