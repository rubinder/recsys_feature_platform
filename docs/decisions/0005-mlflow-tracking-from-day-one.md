# 0005: MLflow tracking from Phase 1

- Status: accepted
- Phase: 1

## Context
The MLflow model registry arrives in Phase 5, but a model that cannot be traced
to its data, split, and features undermines the data-infra story from the start.

## Decision
Every training run logs to MLflow (local SQLite metadata + file artifacts under
`mlruns/`, no server): split config, ALS and ranker params, test and validation
metrics, the Feast feature refs used (as a tag), and both models plus a manifest
(feature list, serving time, label statistics, feature importances) as
artifacts. Serving loads models by MLflow run id (`MODEL_RUN_ID`, defaulting to
the latest training run) and returns that id as `model_version` in every
response. At startup the service refuses to load a model whose feature refs
differ from what it serves.

## Consequences
- Phase 2 impression logs record the run id, giving lineage from each shown item
  back to the exact model and features.
- Phase 3 moves artifacts to S3; Phase 5 adds the registry on top of these runs.
- MLflow stores absolute artifact paths, so docker-compose mounts `mlruns/` at the
  same absolute path inside the container.
