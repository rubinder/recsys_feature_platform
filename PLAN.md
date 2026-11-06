# PLAN

Recommendation feature platform built in five phases, strictly in order:
1 → 2 → 3 → 4 → 5. The build prompts live in `phase_N_prompt.txt`.

---

## Phase 1: Core feature platform

### Goal

An end-to-end recommender that runs locally, where one set of Feast feature
definitions feeds both training (offline, point-in-time) and serving (online,
Redis), so there is no train/serve skew.

### Architecture

```
MovieLens 1M ──> ingest ──> data/raw/ (generic schema: events, items, users)
                               │
                               ▼
                    PySpark batch features ──> data/offline/*.parquet
                    (user + item snapshots,     (Feast offline store,
                     each with event_timestamp)  read via DuckDB)
                               │
               ┌───────────────┴───────────────┐
               ▼                               ▼
   Feast get_historical_features      Feast materialize ──> Redis
   (point-in-time join on labels)                          (online store)
               │                                               │
               ▼                                               ▼
   Stage 1: implicit ALS (candidates)              FastAPI /recommendations/{user_id}
   Stage 2: LightGBM LambdaRank (ranker)  ──MLflow──>  loads models by run id,
   logged to MLflow (params, metrics,     run id      online features from Feast,
   feature refs, split, artifacts)                    ALS candidates -> rank -> top-N
```

### Key decisions (each gets an ADR in `docs/decisions/`)

| Decision | Choice | Why |
|---|---|---|
| Model | Two-stage: ALS top-K (K=200) candidates, then LightGBM LambdaRank over Feast features + ALS score | ALS alone ignores features, which would make the feature store irrelevant. Two-stage is how production recsys works. |
| Generic schema | `events(user_id, item_id, event_type, weight, timestamp)`, `items(item_id, title, attributes)`, `users(user_id, attributes)` | MovieLens is `rate` with weight = rating; music would be `play` with weight = plays. Genres live in item attributes. |
| Split | Fixed global timestamp cutoffs for train / validation / test, in `config/split.yaml`, chosen from event quantiles (~80/10/10) and committed as literal dates | Time-based splits match production and avoid leakage. Reused by every later phase. |
| Labels | Rows of `(user_id, item_id, event_timestamp, label)`; label = 1 if the user interacts with that candidate within 7 days after `event_timestamp` | Gives the ranker a real target and makes point-in-time joins necessary. |
| Offline store | Parquet read through Feast's DuckDB offline store | Zero setup, fast, and swaps to Snowflake in Phase 3. |
| Online store | Redis 7 via docker-compose | Standard Feast online store; ElastiCache equivalent in Phase 3. |
| Tracking | MLflow tracking, local file store (`mlruns/`), no server | Every model traceable to its data and features from day one. Registry comes in Phase 5. |
| Environment | One uv-managed Python 3.11 env with a lockfile; Java 17 for Spark | Phase 1's deps resolve together. Later phases may add separate envs (PyFlink). |

### Feature timeline and point-in-time correctness

- Batch features are computed as **weekly snapshots**: for each snapshot date
  `d`, every feature is computed from events strictly before `d` and stamped
  `event_timestamp = d`. This gives Feast a feature history to join against.
- **Ranker training rows**: weekly anchor times `t` inside the training period.
  For each user active before `t`: candidates = ALS top-K (ALS refit on events
  before `t`, mirroring serving; changed from "fit once before the ranker window"
  during implementation because that dropped every user who joined in November),
  label = interaction within `[t, t + 7d)`, features = Feast point-in-time join
  at `t`.
- **Evaluation**: ALS refit on all training events; the two-stage system is
  evaluated against held-out test-period interactions (hit-rate@k, NDCG@k),
  compared with ALS-only.
- **Serving**: Redis is materialized as of the training cutoff, so the API
  answers as if "now" is that cutoff. Phase 2's streaming features move "now"
  forward.

### Repo layout

```
config/split.yaml            split cutoffs, label window, K, N, seeds
src/recsys/
  config.py                  typed config + paths
  schema.py                  generic schema definitions
  ingest/movielens.py        download ML-1M -> data/raw/ (generic schema)
  features/batch.py          PySpark user/item feature snapshots -> data/offline/
  feature_repo/              Feast repo (feature_store.yaml, definitions.py)
  training/
    labels.py                anchors + labels (entity dataframe)
    candidates.py            implicit ALS wrapper
    ranker.py                LightGBM LambdaRank wrapper
    train.py                 end-to-end training, MLflow logging
  eval/metrics.py            hit_rate_at_k, ndcg_at_k (extended in Phase 2)
  eval/evaluate.py           offline evaluation on the test split
  serving/app.py             FastAPI app
  serving/recommender.py     online feature fetch -> candidates -> rank
  latency/histogram.py       per-stage latency histogram (reused later)
  latency/loadgen.py         `make latency`: N requests -> p50/p95/p99, QPS
scripts/                     demo + doc capture scripts
tests/                       pytest suite
docs/phase-1.md, docs/decisions/, docs/img/
data/, mlruns/               generated, gitignored
```

### Makefile targets

| Target | Does |
|---|---|
| `setup` | `uv sync`, check Docker and Java |
| `data` | download and normalize ML-1M into `data/raw/` |
| `features` | PySpark feature snapshots into `data/offline/`, register Feast definitions |
| `materialize` | start Redis, materialize features as of serving time (validation_end) |
| `parity` | fail if any Redis feature differs from the offline point-in-time value |
| `train` | build labels, PIT training set, train ALS + ranker, evaluate, log to MLflow |
| `serve` | build and start the API container (docker-compose: Redis + app) |
| `latency` | load script against the running API, prints p50/p95/p99 and QPS |
| `demo` | all of the above on a fresh checkout, then prints recs for a sample user |
| `test` | pytest |
| `docs-capture` | regenerate `docs/img/` (Playwright + captured output) |

### Docs and presentation (Phase 1)

- `docs/phase-1.md`: what was built, decisions, real results, limitations.
- `docs/decisions/`: ADR template plus ADRs for the five decisions in the table
  above (model, schema, split and labels, offline store, MLflow from day one).
- `docs/img/`: MLflow run page (Playwright), sample `/recommendations` response,
  `make latency` report (saved text output).
- README: front page with the mermaid diagram, how to run, headline results.

### Out of scope for Phase 1

Streaming and the feature cache, impression logging and CTR/coverage (Phase 2);
Dagster, Pandera, Terraform, AWS, Snowflake (Phase 3); benchmarks (Phase 4);
MLflow registry, A/B, drift monitoring, Prometheus/Grafana (Phase 5).

### Status

Done. See [docs/phase-1.md](docs/phase-1.md) for results.

### Definition of done

On a fresh checkout `make demo` brings up Redis, builds features, materializes,
trains both stages (logged to MLflow), starts the API and prints ranked recs for
a sample user. `make test` passes. README and docs explain online/offline
consistency, point-in-time correctness and the two-stage design, with measured
latency and offline metrics.
