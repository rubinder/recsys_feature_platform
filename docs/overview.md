# Recommendation feature platform: one-page overview

**Problem.** A recommender is only as good as the features it sees at request
time, and only trustworthy if training saw the same features. This project
builds the data infrastructure for that (feature store, streaming, orchestration,
quality, infrastructure, measurement, and the production loop) around an
off-the-shelf two-stage model, on MovieLens 1M with a dataset-agnostic schema.

## Architecture (five phases)

| Phase | What it adds | Core technology |
|---|---|---|
| 1 | Point-in-time features, one definition for training and serving, two-stage ranking, MLflow tracking | PySpark, Feast (DuckDB / Redis), implicit ALS, LightGBM LambdaRank, FastAPI |
| 2 | Streaming features with contracts and a DLQ, item-feature cache, impression logging, rec metrics | Kafka, PyFlink, Feast push, pydantic, DuckDB |
| 3 | Orchestration with lineage, blocking data-quality checks, freshness SLAs, cloud infrastructure | Dagster, Pandera, Terraform (AWS, Snowflake) |
| 4 | Reproducible one-knob benchmarks with confidence intervals | paired bootstrap, MLflow, matplotlib |
| 5 | A/B testing, model registry with gated promotion and rollback, drift monitoring and alerting | MLflow registry, Prometheus, Grafana |

## Key decisions

- **Two-stage ranking** so features matter: ALS proposes 200 candidates,
  LambdaRank reranks them with Feast features (ADR 0001).
- **Point-in-time correctness by construction:** weekly feature snapshots,
  anchored labels, Feast historical joins (ADR 0003, 0004).
- **One streaming transformation, two runners:** the same Python state machine
  runs in Flink and in the backfill; Flink bootstraps its state from history
  (ADR 0006).
- **Deterministic evaluation:** a lockstep simulator that waits for the stream
  makes replay metrics reproducible (ADR 0008).
- **Train on the serving distribution:** mid-session anchors for freshness-
  sensitive models (ADR 0010).
- **Blocking quality checks and freshness SLAs** in the orchestrator (ADR 0011, 0012).
- **Cost-gated, credential-free infrastructure as code** (ADR 0013).
- **Statistics where claims are made:** paired bootstrap CIs, user-level A/B
  analysis with SRM and guardrails, non-inferiority promotion gate
  (ADR 0015–0017).

## Headline results

| | Result |
|---|---|
| Two-stage vs ALS only | NDCG@10 0.238 → **0.283** (+19%) on a strictly later test period |
| Online/offline parity | 18 features identical in Redis and the point-in-time store; streaming parity 0 mismatches over 300 users |
| Streaming features (paired replay, simulated) | CTR **+12.6%** [CI +0.035, +0.065], coverage ×2.7 |
| Naive vs point-in-time join | offline validation **+20%** (illusory), served **−3%** |
| Item-feature cache | p50 20.7 → **6.2 ms**, throughput ×4.6 at concurrency 8 |
| Ablation | popularity: most accuracy, least diversity; session features: improve both |
| A/B test (565 users) | same direction, **inconclusive** (p = 0.43): paired offline evaluation is far more powerful than a small live test |
| Promotion gate | good challenger promoted, shuffled-label challenger rejected, rollback in one command |

## Limitations

- Outcomes are simulated by replaying real future behaviour (counterfactual,
  no feedback loop); every report says so.
- Cloud resources are planned against real accounts (38 resources; 46 with
  MSK + ElastiCache; 62 with EKS) but not applied; the Snowflake demo and the
  Kubernetes paths have not been executed.
- Single-node Flink without checkpointing; local Dagster and MLflow storage.
- Small dataset: the ranker trains on hundreds of queries, so some effects need
  cross-validation to be measurable.

Details: [README](../README.md) · phase write-ups [1](phase-1.md) [2](phase-2.md)
[3](phase-3.md) [4](phase-4.md) [5](phase-5.md) · [decisions](decisions/) ·
[walkthrough](walkthrough.md) · [talking points](talking-points.md)
