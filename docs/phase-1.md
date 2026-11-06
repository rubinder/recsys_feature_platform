# Phase 1: Core feature platform

**Goal:** one set of feature definitions serving both training (offline,
point-in-time) and inference (online, Redis), with no train/serve skew, running
end to end on a laptop with `make demo`.

## What was built

| Layer | Implementation |
|---|---|
| Ingest | `recsys.ingest.movielens`: downloads MovieLens 1M (checksum pinned) and normalizes it into a generic `events / items / users` schema ([ADR 0002](decisions/0002-generic-event-schema.md)) |
| Batch features | `recsys.features.batch`: PySpark computes 36 weekly snapshots; each snapshot uses only events strictly before its timestamp. 90,656 user rows and 118,640 item rows in about 20 s |
| Feature store | Feast 0.67: entities `user` / `item`, 4 feature views, 1 on-demand view (`genre_affinity`), DuckDB offline store over Parquet, Redis online store ([ADR 0004](decisions/0004-duckdb-parquet-offline-store.md)) |
| Training data | Weekly anchors with 7-day labels, joined point-in-time via `get_historical_features` ([ADR 0003](decisions/0003-time-based-split-and-labels.md)) |
| Models | ALS top-200 candidates, then LightGBM LambdaRank over 11 Feast features + ALS score ([ADR 0001](decisions/0001-two-stage-ranking.md)) |
| Tracking | MLflow run per training: params, metrics, feature refs, both models and a manifest ([ADR 0005](decisions/0005-mlflow-tracking-from-day-one.md)) |
| Serving | FastAPI `GET /recommendations/{user_id}`: ALS candidates, one batched Feast online read, ranker, top-N; popular-items fallback for users unknown to ALS; per-stage latency histogram |
| Checks | `make parity` (Redis vs offline at serving time), 18 pytest tests including a Spark test that no event leaks into a snapshot at or before its own timestamp |

### Features

| View | Features | Group (for Phase 4 ablation) |
|---|---|---|
| `user_stats` | interaction count, 30-day count, average weight, days since last / first interaction | user_activity |
| `user_genre_affinity` | share of history per genre (18-dim vector) | affinity |
| `item_popularity` | interaction count, 30-day count | popularity |
| `item_stats` | average weight, days since last / first interaction, genre one-hot | item_quality_recency |
| `user_item_affinity` (on-demand) | `genre_affinity` = user genre share · item genre one-hot | affinity |

## Evaluation protocol

Fixed in `config/split.yaml` and reused by every later phase:

| Period | Range | Used for |
|---|---|---|
| Train | before 2000-12-01 | ranker training rows from 4 weekly anchors in the last 28 days |
| Validation | 2000-12-01 to 2000-12-29 | 4 weekly anchors; ranker early stopping |
| Test | after 2000-12-29 | final metrics; also the serving "now" (features in Redis are as of this date) |

ALS is refit at every anchor on events before it, the same way the served model
is fit on everything before serving time.

## Results

From MLflow run `f0d27af47de1418a9e59789476968f83`. Two clean runs from scratch
produced identical metrics and identical recommendations.

**Offline quality, test period** (1,176 warm users with new interactions after
the cutoff; ground truth = every unseen item they rate afterwards):

| Metric | ALS only | Two-stage | Change |
|---|---:|---:|---:|
| NDCG@10 | 0.2379 | **0.2832** | +19.0% |
| Hit-rate@10 | 0.6777 | **0.7577** | +11.8% |
| Recall@10 | 0.0459 | **0.0590** | +28.5% |
| NDCG@50 | 0.2213 | **0.2558** | +15.6% |
| Recall@50 | 0.1545 | **0.1752** | +13.4% |

Candidate recall@200 is 0.375: the ranker can only reorder what ALS retrieves,
so this is the ceiling for the second stage.

**Training data:** 510 training queries (102,000 rows, 6.1% positive) and 761
validation queries (152,200 rows). The ranker early-stopped at 51 trees
(validation NDCG@10 0.194).

**Online/offline parity:** 200 random (user, item) pairs × 11 features, all
identical between Redis and the point-in-time offline store at serving time
([captured output](img/phase1-parity-check.txt)).

**Serving latency** (API in Docker, Redis in Docker, Apple Silicon laptop, 500
requests from a seeded sample of warm users; [report](img/phase1-latency-report.txt)):

| Concurrency | p50 | p95 | p99 | Throughput |
|---:|---:|---:|---:|---:|
| 1 | 14.5 ms | 16.2 ms | 20–79 ms (noisy) | 63–66 req/s |
| 8 | 118.6 ms | 196.2 ms | 216.5 ms | 62.6 req/s |

Server-side breakdown at concurrency 1 (p50): ALS candidates 0.08 ms, **Feast
online read for 200 candidates 11.6 ms**, ranking 0.8 ms, total 12.4 ms.

## What the numbers say

- **The feature store earns its place.** Reranking ALS candidates with Feast
  features improves NDCG@10 by 19% on a strictly later test period.
- **Latency is all in the online feature read**, about 93% of server time. Redis
  itself is fast; the cost is Feast's Python-side request building and response
  deserialization for 200 entity rows.
- **Throughput doesn't scale with concurrency**: about 63 req/s at both 1 and 8
  concurrent clients, because one CPU-bound Python worker serializes everything
  and extra concurrency only adds queueing. Fixes, in order of leverage: cache
  item features (Phase 2), more uvicorn workers, Feast's Go/Java feature server.
  The Phase 4 benchmarks measure the first.
- **p99 is noisy** run to run (20 ms vs 79 ms) on a laptop. p50 and p95 are
  stable. Treat the p99 range as indicative only.

## Limitations and simplifications

- **Small ranker training set.** MovieLens users rate most of their history in
  their first session, so at each anchor most users with future activity are
  unknown to ALS (e.g. 1,323 of 1,443 at the 2000-11-17 anchor) and are skipped.
- **Hit-rate looks high** because test ground truth spans about two years of
  activity. NDCG against the ALS-only baseline is the meaningful comparison.
- **"Now" is frozen at 2000-12-29.** Redis holds features as of the validation
  cutoff; Phase 2's streaming features move it forward.
- **Cold-start users** get a popularity list. There is no content-based fallback.
- **Single uvicorn worker** and the Python Feast SDK on the request path; fine for
  a demo, not a production serving tier.
- **Dataset quirk:** ratings are explicit (1–5) and used as implicit confidence
  for ALS. A listening dataset would use play counts or listen fractions.

## Artifacts

- [MLflow run](img/phase1-mlflow-run.png), [metrics view](img/phase1-mlflow-metrics.png)
- [Demo output](img/phase1-demo-output.txt), [sample API response](img/phase1-recommendation-response.json)
- [Latency report](img/phase1-latency-report.txt), [parity check](img/phase1-parity-check.txt)

All regenerated by `make docs-capture` (needs `make serve`).
