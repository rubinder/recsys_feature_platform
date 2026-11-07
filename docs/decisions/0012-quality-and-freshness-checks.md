# 0012: What the data-quality and freshness checks enforce

- Status: accepted
- Phase: 3

## Decision
Pandera schemas for column rules (types, ranges, nullability, uniqueness,
cross-column invariants), validated lazily so every violation is reported, plus
pandas checks for cross-table rules:

| Asset | Checks |
|---|---|
| raw_events | schema (positive ids, finite non-negative weight, aware timestamps after 1990 and not in the future, strict columns); referential integrity (every event's user and item exist); duplicate rate ≤ 0.1% |
| batch_features | per-view schemas (counts ≥ 1, 30-day ≤ total, last ≤ first, unique entity × snapshot); average weight within the event weight range; genre vectors well-formed and shares summing to 1; null rate 0; referential integrity (feature rows → users / items); snapshots on the weekly grid ending at the cutoff |
| streaming_backfill | schema (1h ≤ 24h, 1 ≤ last-N ≤ 10, gap ≥ 0, last-event epoch matches the row timestamp); every timestamp inside the data's time range (catches the Phase 2 1970 bug); one row per user per timestamp (Feast's Redis store would drop equal-timestamp writes) |
| online_store | batch freshness: newest batch timestamp in Redis ≤ 8 days behind the serving clock; stream freshness: last produced vs processed event ≤ 300 s of event time |

Freshness uses the system's event-time clock (the serving cutoff, advanced by
the stream during a replay); `RECSYS_CLOCK` overrides it to rehearse the
"weekly job stopped" failure, which reports 17 days against an 8-day SLA.

## Consequences
- Every check that caught a real bug during Phases 1-2 now runs on every
  materialization.
- Freshness checks run with the pipeline and hourly on their own schedule; when
  the stream is not running they report WARN rather than failing.
