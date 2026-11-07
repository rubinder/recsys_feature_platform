# 0011: Dagster software-defined assets with blocking quality checks

- Status: accepted
- Phase: 3

## Context
Phases 1-2 sequenced the batch path in a Makefile: no lineage, no retries, no
record of what ran on which data, and nothing stopping bad data from flowing
into training and the online store.

## Options considered
1. Keep make, add shell retries.
2. Airflow DAG of tasks.
3. **Dagster software-defined assets**: each dataset (raw events, feature
   tables, registry, online store, models, reports) is an asset with lineage,
   metadata and checks.

## Decision
Option 3. Assets wrap the existing functions without rewriting them. Data
quality checks are Dagster asset checks marked `blocking=True`, so a failure
stops every downstream asset in the run (tested). Every asset has an
exponential-backoff `RetryPolicy`. A weekly schedule matches the snapshot
cadence; a sensor reruns the pipeline when a new dataset lands in
`data/landing/`; a second sensor refreshes the rec-quality report when the
impression logs change. The Makefile's batch targets became thin wrappers
over `dagster asset materialize` / `dagster job execute`.

The streaming job is deliberately **not** a Dagster asset: it is a long-running
service, not a batch computation. Its health is covered by the streaming
freshness check, and its outputs (impression logs) enter the graph as an
observable source asset.

## Consequences
- The asset graph is the lineage: raw → features → registry → online store →
  rankers → metrics, visible in the UI with check status per asset.
- Model training records MLflow run ids as asset metadata, linking the two
  lineage systems.
- Runs execute in subprocesses (multiprocess executor, max 2 concurrent) so
  Spark and LightGBM memory is released between steps.
