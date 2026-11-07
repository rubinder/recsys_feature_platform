# Talking points: likely questions, short answers, and the evidence

**How do you guarantee online/offline consistency?**
One Feast definition per feature, read by `get_historical_features` for training
and `get_online_features` for serving; derived features are Feast on-demand views
or shared Python; a startup check refuses models trained on different feature
refs; and parity is *tested*: `make parity` compares 18 features for 200 pairs.
→ `src/recsys/feature_repo/definitions.py`, `src/recsys/features/parity.py`, ADR 0001/0004.

**Why does point-in-time correctness matter, concretely?**
In the benchmark, a latest-value join made validation NDCG look 20% better and
the served model 3% worse; positive rows' item counts grew more after the anchor
than negatives' (+254 vs +201): the label leaked into the feature.
→ README Benchmarks §2, `benchmarks/exp_pit.py`.

**How are streaming and batch features kept consistent?**
The streaming logic is one pure-Python state machine run by both Flink and the
backfill; Flink replays history into keyed state before pushing; `make
stream-parity` recomputes from events and compares Redis and the offline store
(0 mismatches). Two traps found on the way: Feast's Redis store skips equal-
timestamp writes (fixed with deterministic microsecond offsets), and a units bug
put backfilled rows in 1970 while a null-equals-null parity check still passed.
→ ADR 0006, docs/phase-2.md "Problems found".

**How fresh are features, and does it matter?**
About 100 ms from event to Redis. At the average request, batch features miss
~72 user events; adding streaming features lifts simulated CTR 12.6% with a CI
excluding zero, and coverage 2.7×.
→ README Benchmarks §1, `benchmarks/exp_freshness.py`.

**Your outcomes are simulated. How much should we trust them?**
They are counterfactual replays of real future behaviour, labelled everywhere.
They are reproducible and paired (both variants face the same state and the same
future) but cannot show feedback effects. That is why the A/B machinery exists:
it analyses at the user level with SRM and guardrails and would be correct on
live traffic. On this data it is inconclusive, which illustrates how much more
powerful paired offline evaluation is for sizing a live test.
→ ADR 0008, 0016, docs/phase-5.md.

**How do you stop bad data or a bad model from reaching users?**
Data: 14 Pandera/invariant checks are blocking Dagster asset checks (a failure
stops downstream assets, tested); freshness SLAs alert. Models: a challenger
must pass data quality and be non-inferior on held-out paired NDCG@10; a
shuffled-label challenger was rejected; rejected challengers lose their alias so
the A/B arm can't serve them; rollback is one command and serving reloads.
→ ADR 0011, 0012, 0017, `tests/test_orchestration.py`.

**Where does latency go, and how did you reduce it?**
93% of uncached server time was one 200-row Feast online read, linear in the
number of candidates. Item features change weekly, so they are cached in-process
(TTL 300 s); only a 1-row user read remains: p50 20.7 → 6.2 ms, throughput ×4.6.
A test proves cached and uncached paths give identical model inputs.
→ ADR 0009, README Benchmarks §3.

**How would this run in production?**
The cloud profile switches Feast's offline store to Snowflake and MLflow
artifacts to S3; Terraform defines S3, IAM, Snowflake (warehouse, resource
monitor, key-pair service user, storage integration, stage), and opt-in MSK,
ElastiCache and EKS in one VPC. `terraform plan` resolves against real accounts
(38 resources default, 62 with EKS + ElastiCache).
→ docs/phase-3.md "Local → cloud mapping", ADR 0013, 0019.

**What would you do next?**
Checkpointed, partitioned Flink with per-partition progress; Feast's Go feature
server or multi-worker serving; offline-part compaction; Alertmanager routing;
a feedback-aware simulator (or live traffic) for the A/B test; executing the
cloud and Kubernetes paths.

**What went wrong along the way?**
Each phase doc has a "Problems found and fixed" section: OOM from Feast's file
offline store, PyFlink at 90 events/s until thread mode, a shared timer flag
adding 1 s of latency, a noisy ablation fixed with grouped CV, biased bootstrap
CIs for coverage, drift that was really selection bias, and a rejected model
that briefly stayed in an experiment arm.
