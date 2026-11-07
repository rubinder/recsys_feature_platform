# 10-minute walkthrough

A demo script: what to run, what the viewer sees, and what each step proves.
Run the setup once beforehand (about 25 minutes, mostly the replays):

```bash
make demo && make demo-streaming && make demo-production-loop
```

Then open three tabs: Dagster `make dagster-dev` (http://localhost:3000),
Grafana http://localhost:3001, MLflow http://localhost:5000.

---

### 0:00 — The problem (1 min)

"Recommendations need features that are identical in training and serving and
fresh at request time. The model here is off-the-shelf; the deliverable is the
data platform around it." Show the README architecture diagram.

### 1:00 — One feature definition, two read paths (2 min)

```bash
make parity
```

**See:** `200 (user, item) pairs x 18 features ... identical in Redis and the
point-in-time offline store`.
**Proves:** online/offline consistency is checked, not assumed. Open
`src/recsys/feature_repo/definitions.py`: one Feast view per feature group, an
on-demand view for `genre_affinity` that runs the same code in both paths.

Then the point-in-time result from the README Benchmarks table: a naive
latest-value join makes offline validation look **20% better** and serves
**3% worse**, because features see the label window.

### 3:00 — Freshness (2 min)

```bash
make flink-ui                      # Flink job graph (Ctrl-C to close)
cat docs/img/phase2-live-freshness.txt
```

**See:** session counts, rolling counts and last-N items changing in Redis as
event time advances; 6/6 malformed events in the dead-letter topic.
**Proves:** a streaming path with contracts, event time, and the same feature
logic as the offline backfill (`make stream-parity`: 0 mismatches).
Result: CTR **+12.6%** [CI +0.035, +0.065] and coverage ×2.7 with streaming
features, because batch features miss ~72 user events at the average request.

### 5:00 — Production operation (2 min)

Dagster tab → `offline_metrics` → Lineage: ingest → features (8/8, 3/3 checks
passed) → Feast registry → online store → three rankers.

```bash
RECSYS_CLOCK=2001-01-15T00:00:00 make freshness
```

**See:** `batch_freshness` fails: features 17 days old vs an 8-day SLA.
**Proves:** blocking data-quality checks and freshness SLAs with a clear failure
signal; `make terraform-plan` shows the same platform as 38 AWS + Snowflake
resources (~$2/month by default, MSK/ElastiCache/EKS opt-in).

### 7:00 — The production loop (2.5 min)

```bash
cat data/artifacts/lift_report.txt
make registry-status
```

**See:** the A/B test is *inconclusive* (CTR +8.8%, permutation p = 0.43, SRM
ok, guardrails ok), the registry shows v5 promoted by the gate, v6 (shuffled
labels) rejected, and the rollback to v1.
**Proves:** experiments are analysed at the randomization unit with validity
checks; promotion requires data quality plus non-inferiority; rollback is one
command and serving follows within seconds.

Grafana tab: latency p99 vs the 50 ms SLO, cache hit rate, feature and
prediction drift with firing alerts.

### 9:30 — Close (30 s)

"Every number in the README is measured, seeded and reproducible, with
confidence intervals where they matter; where something is simulated or not
executed (outcomes, Snowflake apply, EKS), the docs say so."
