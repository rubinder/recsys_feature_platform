# Phase 2: Streaming features, feature cache, impression logging, rec metrics

**Goal:** make user features fresh in near-real-time without giving up
train/serve consistency, and close the measurement loop: log what the system
shows, attribute outcomes, and compute recommendation quality.

## What was built

| Piece | Implementation |
|---|---|
| Event contract | `recsys.contracts.InteractionEvent` (pydantic, `extra="forbid"`, aware timestamps, finite weights, `seq`) on the Kafka topic and in the interaction log |
| Streaming ingestion | Kafka 4.1 (KRaft, docker-compose). `make produce` bulk-publishes history, then replays the test period at a fixed speed-up (default one day of event time per second); `--malformed` injects contract violations |
| Streaming features | PyFlink 2.2 job (JobManager + TaskManager in docker-compose, thread mode): validate → event-time timestamps and watermarks → `keyBy(user)` → `UserStreamState` in keyed state. Invalid, late, out-of-order and duplicate (`seq` already applied) events go to `interactions-dlq` |
| Features | session events and duration (30-min gap), last 10 items, rolling 1h / 24h counts, gap since previous event, last event time; derived at request time: seconds since last event, in-session flag, `recent_item_sim` (candidate vs last-10 items in ALS space) |
| Write path | Online: micro-batched pushes through Feast's feature server `/push` into Redis. Offline: append-only Parquet parts in the push source's batch directory ([ADR 0007](decisions/0007-feast-push-online-append-only-offline-parts.md)) |
| One logic, two runners | The same pure-Python `UserStreamState` runs in Flink and in the bounded backfill that builds training history. Flink bootstraps its state by replaying history before pushing ([ADR 0006](decisions/0006-streaming-features-one-logic-two-runners.md)) |
| Feature cache | In-process TTL cache for item features; user features always fresh ([ADR 0009](decisions/0009-item-feature-cache.md)) |
| Serving | Multiple model variants by name, an event-time `as_of` clock, streaming variants filter items consumed since the cutoff, impressions logged through a pydantic contract (`variant`, `experiment_id` included now for Phase 5) |
| Simulation | Lockstep simulator: at each session start and every 10th event, wait until Flink has processed every earlier event, request every variant, then publish the event ([ADR 0008](decisions/0008-deterministic-lockstep-simulation.md)) |
| Metrics | `make metrics`: CTR, item CTR, hit-rate@k, NDCG@k, catalog coverage per variant from the logs, plus deltas; reuses Phase 1's metric functions |
| Training | `--anchors session` for mid-session anchors that mirror serving ([ADR 0010](decisions/0010-session-anchored-training.md)); `make train-streaming` trains `batch_session` and `streaming_session` |
| Checks | `make stream-parity`: Flink's Redis values and the offline parts equal the shared logic recomputed from events. 39 tests including contracts, streaming logic, cache, derived features, attribution |

## Batch vs streaming features, and freshness

| | Batch (Phase 1) | Streaming (Phase 2) |
|---|---|---|
| Computed by | PySpark, weekly snapshots | PyFlink, per event |
| Freshness at serving | up to 7 days old (frozen at the last snapshot) | ~100 ms after the event (Kafka → Flink → Feast push → Redis) |
| Describes | long-run taste and popularity | what the user is doing *right now*: this session, the last few items |
| Training history | the snapshots themselves | the backfill of the same logic, plus parts Flink appends |
| Online write | `feast materialize` | Feast push from Flink |

**Feature freshness** is the age of a feature value at the moment the model
uses it. A batch feature computed Sunday describes the user as of Sunday; on
Thursday it is four days stale and knows nothing about the session in progress.
Streaming features close that gap, which matters most for intent that changes
within a session.

## The impression-logging and measurement loop

```
request ─> API ─> impression log (contract: request, user, items, scores, ranks,
                  model_version = MLflow run id, variant, experiment_id, as_of)
replay  ─> interaction log (contract)
             └─> attribution: same user, shown item, at or after the triggering
                 event and within 24 h of event time  ─>  CTR · hit-rate · NDCG · coverage
```

**These outcomes are simulated.** Nobody saw these recommendations; a "click" is
a replayed interaction that the recommendation happened to anticipate. This is
counterfactual replay evaluation: it is reproducible and fair across variants
(both face the same state and the same future), but it cannot capture
recommendations changing behaviour.

## Results

### Rec quality on the replayed test period (simulated outcomes)

28 days after the cutoff (2000-12-29 to 2001-01-25): 22,417 events, 565 users,
3,054 request points, each served by both variants
([report](img/phase2-metrics-report.txt)).

| Metric | batch_session | streaming_session | Change |
|---|---:|---:|---:|
| CTR (impression with ≥1 click) | 0.396 | **0.446** | +12.6% |
| Item CTR | 0.095 | **0.105** | +10.7% |
| Hit-rate@5 | 0.289 | **0.324** | +12.0% |
| NDCG@5 | 0.094 | **0.107** | +14.1% |
| NDCG@10 | 0.098 | **0.110** | +11.9% |
| Catalog coverage | 8.2% | **21.9%** | ×2.7 |

Both rankers are trained on identical mid-session anchors; the only difference
is the feature set. Confidence intervals come with the Phase 4 benchmark harness.
Coverage rises because session-aware features (`recent_item_sim`) pull
recommendations toward what the user is browsing now, instead of the same
popular items for everyone.

### Offline (static) evaluation

| | Validation NDCG@10 (mid-session anchors) | Validation NDCG@50 | Test NDCG@10 (Dec 29 snapshot, nobody mid-session) |
|---|---:|---:|---:|
| batch_session | 0.193 | 0.271 | 0.240 |
| streaming_session | **0.209** | **0.306** | 0.220 |

Streaming features help when there is a live session (validation, replay) and
hurt slightly on a snapshot where every session is days old: the model learned
to rely on context that isn't there. Evaluate a model under the conditions it is
served in.

### Serving latency with the item-feature cache

`streaming_session` variant, API in Docker, 500 requests from a seeded sample of
warm users ([no cache](img/phase2-latency-no-cache.txt),
[cache](img/phase2-latency-cache.txt)):

| | p50 | p95 | p99 | req/s (c=1) | req/s (c=8) |
|---|---:|---:|---:|---:|---:|
| No cache | 20.7 ms | 24.2 ms | 28.4 ms | 45 | 43 |
| Item-feature cache | **6.2 ms** | **8.4 ms** | **12.2 ms** | **152** | **197** |

Cache hit rate 97.8% (TTL 300 s). The server-side online-feature stage drops from
16.1 ms to 2.1 ms at p50: a 1-row user read plus cached item rows replaces a
200-row read. Cached and uncached paths produce identical model inputs (tested).

### Freshness and correctness

- **End to end** (event published → feature in Redis): about 100 ms, measured
  with single-event probes. 20 ms of that is the micro-batch quiet timer, 3–6 ms
  the Feast push, the rest Kafka source and PyFlink overhead.
- **Live replay** ([output](img/phase2-live-freshness.txt)): three users' session
  counts, rolling counts and last items updating in Redis as event time advances
  at 12 hours per second.
- **Contract:** 6 of 6 injected malformed events (bad JSON, negative id, missing
  field, naive timestamp, NaN weight, unknown field) rejected to the DLQ.
- **Streaming parity** ([output](img/phase2-stream-parity.txt)): 300 replayed
  users × 7 features, 0 mismatches between Flink's Redis values, the offline
  parts (point-in-time), and the logic recomputed from events.
- **Bootstrap:** Flink replays 898,002 history events into keyed state in about
  60 s (~17k events/s) before pushing anything.
- **Simulation cost:** 28 days in about 5 minutes, 85% of it waiting on the
  stream so every request sees exactly the features it should.

## Problems found and fixed along the way

These are worth knowing because each one is a realistic production failure:

1. **Backfill timestamps in 1970.** pandas stored timestamps in microseconds and
   the code divided by 1e9; every streaming row fell outside the 365-day TTL and
   joined as null. The first parity check still passed, because null equals
   null. Fixed with unit-independent arithmetic, a regression test, and a
   parity check that now requires real coverage.
2. **Equal-timestamp writes dropped.** Feast's Redis store skips a write whose
   timestamp equals the stored one; same-second events would silently lose
   updates online but not offline. Fixed with deterministic microsecond offsets.
3. **Feature server OOM.** Feast's file offline store rewrites the whole file on
   each offline push ([ADR 0007](decisions/0007-feast-push-online-append-only-offline-parts.md)).
4. **PyFlink at 90 events/s.** Process mode's Java↔Python round trips; thread
   mode (embedded CPython via pemja) reached ~17k/s. Thread mode can initialise
   only once per TaskManager JVM on Java 17, so each submit restarts the
   TaskManager.
5. **Mixed timestamp formats in a batch.** `isoformat()` omits zero
   microseconds; a micro-batch mixing both failed pandas parsing in Feast.
6. **One shared timer flag** let a 1 s maintenance timer delay every online flush
   (measured 1 s end to end per event).
7. **Reset could not undo newer values.** Re-materializing can't overwrite Redis
   values with newer timestamps; reset now flushes Redis first.
8. **At-least-once duplicates.** A malformed-event generator accidentally sent a
   valid duplicate, which Flink applied twice; Flink now drops any `seq` it has
   already applied for that user.

## Limitations

- Outcomes are simulated (see above); there is no feedback loop.
- One Flink slot, parallelism 1, no checkpointing: the progress marker relies on
  a total order, and recovery means reset and replay. Scaling out would need
  per-partition progress and checkpointed state.
- Offline parts are never compacted.
- The candidate generator (ALS) is frozen at the cutoff; only features are fresh.
- Streaming variants know only the last 10 consumed items for filtering; items
  consumed earlier in the replay could be recommended again.
- Variants are compared on the same requests (counterfactual); per-user
  assignment and significance testing arrive in Phase 5, confidence intervals in
  Phase 4.

## Artifacts

- [Flink job graph](img/phase2-flink-job.png)
- [Live freshness demo](img/phase2-live-freshness.txt)
- [Metrics report](img/phase2-metrics-report.txt), [parity checks](img/phase2-stream-parity.txt)
- [Latency without cache](img/phase2-latency-no-cache.txt), [with cache](img/phase2-latency-cache.txt)

Regenerate with `make demo-streaming` then `make docs-capture PHASE=2`.
