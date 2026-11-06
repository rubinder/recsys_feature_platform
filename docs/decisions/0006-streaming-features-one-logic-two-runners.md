# 0006: Streaming features: one transformation, two runners, bootstrapped state

- Status: accepted
- Phase: 2

## Context
Streaming features (session, last-N items, rolling counts, recency) are computed
by Flink from live events. Training needs the same features point-in-time over
history. Implementing them twice (Flink for serving, SQL or Spark for training)
is the classic source of streaming train/serve skew.

## Options considered
1. Separate implementations for streaming and batch (Lambda architecture).
2. Run Flink over all history to build the offline table.
3. **One pure-Python transformation (`UserStreamState`), run by Flink for live
   events and by a bounded backfill over history** (Kappa-style).

## Decision
Option 3. `recsys.streaming.logic.UserStreamState` has no I/O and is imported
by both the PyFlink `KeyedProcessFunction` and `recsys.streaming.backfill`.
Flink also replays all history first, updating keyed state without pushing,
so its state at the replay start equals the backfill's; it then pushes from
validation_end onward. `make stream-parity` recomputes the logic from events
and checks that Redis and the offline table both match it.

## Consequences
- Session, rolling and last-N state carry across the history/replay boundary
  exactly as training saw them.
- Bootstrap costs about 60 s for 898k events (PyFlink thread mode).
- Feast's Redis store skips writes whose timestamp equals the stored one, and
  MovieLens has many same-second events. The logic offsets same-second updates
  by a microsecond each, deterministically, in both runners.
- Python-level logic is slower than a JVM implementation; at this data size it
  is irrelevant, at scale the logic would move to Java/SQL with the same tests.
