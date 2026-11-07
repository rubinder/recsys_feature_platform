# 0017: Model registry, champion/challenger gate, rollback, hot reload

- Status: accepted
- Phase: 5

## Decision
- **One registered model, `recsys-ranker`**, whose versions point at a training
  run's `model/` artifact directory, so a version is the whole two-stage bundle
  (ALS factors, LightGBM ranker, manifest with feature refs). Aliases
  `champion` and `challenger` replace the deprecated MLflow stages.
- **Gate** (`recsys.registry.gate`): a challenger is promoted only if
  1. every ERROR-severity Phase 3 data-quality check passes, and
  2. it is non-inferior to the champion on held-out mid-session queries: the
     paired bootstrap 95% CI of (challenger − champion) NDCG@10 has a lower bound
     above −0.005 (Phase 4 `paired_diff`).
  Every decision is an MLflow run in the `promotions` experiment and is tagged on
  the version (`gate`, `gate_reason`, `stage`).
- **Rollback:** each promotion appends `{version, previous, reason, at}` to a
  `champion_history` tag on the registered model; `rollback` restores the
  previous champion and tags the bad version `rolled-back`.
- **Serving follows the registry without a restart:** variants may be alias
  specs (`models:/recsys-ranker@champion`); the API re-resolves them on
  `POST /admin/reload` (called by promote/rollback; bearer-token protected,
  disabled without a token) and every 30 s, swapping the model bundle
  atomically. Reload drops pooled registry DB connections so it never reads a
  stale snapshot.
- **Continuous training:** Dagster `retrain_and_promote` (challenger → gate →
  promote/reject); an asset sensor runs it whenever `batch_features` is
  re-materialized.

## Consequences
- The held-out set is the validation period, which the production rankers also
  used for early stopping; it is shared by champion and challenger, so the
  comparison is fair but slightly optimistic in absolute terms.
- A deliberately broken challenger (`make retrain-bad`, shuffled labels) is
  rejected by the gate, demonstrating the failure path.
