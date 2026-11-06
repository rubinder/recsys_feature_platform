# 0008: Deterministic lockstep simulation and counterfactual click attribution

- Status: accepted
- Phase: 2

## Context
Nobody clicks on replayed MovieLens data, yet Phase 2 must log impressions,
attribute outcomes, and compare variants, and Phase 4 must benchmark them
reproducibly. A free-running replay races the stream: whether a request sees
the features of the previous event depends on timing.

## Decision
- **Lockstep:** the simulator replays the test period in global order. At each
  request point (session start, then every 10th event in a session) it waits
  until Flink's progress marker in Redis covers every earlier event, then calls
  the API with `as_of` = the event's time, then publishes the event.
- **All variants per request point:** each variant is asked at the same state
  and logs its own impression (counterfactual replay). Phase 5 adds per-user
  assignment for a real A/B design.
- **Attribution:** a replayed interaction counts as a click on an impression if
  the same user interacts with a shown item at or after the triggering event
  (by seq) and within 24 h of event time.
- Every report labels these outcomes as simulated.

## Consequences
- Results are reproducible run to run, independent of machine speed.
- The metric measures how well a variant anticipates what the user actually did
  next; it cannot capture recommendations changing behaviour (no feedback loop).
- Waiting on the stream costs about 100 ms per request point (Kafka + PyFlink +
  push), which bounds a 28-day replay to minutes.
- `make produce` keeps a free-running replay for demonstrating live freshness.
