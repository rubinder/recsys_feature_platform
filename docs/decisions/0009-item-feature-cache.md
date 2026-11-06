# 0009: Cache item features in-process, never user features

- Status: accepted
- Phase: 2

## Context
Phase 1 measured ~93% of serving time in one batched Feast online read for 200
(user, item) rows. Item features are batch-computed weekly; user features now
include streaming features that change every event.

## Decision
A TTL cache (default 300 s) in the API process holds item-keyed feature rows.
Per request: one 1-row Feast read for the user's features (always fresh), cached
item rows (misses fetched in one read), and `genre_affinity` computed by calling
the same Feast on-demand view transform the uncached path uses. A test checks
that cached and uncached paths produce identical model inputs for every variant.
Hits and misses are exposed at `/cache` next to the latency histogram.

## Consequences
- Staleness bound: item features can be up to TTL older than Redis, which is
  irrelevant for weekly batch features and must be revisited if item features
  become streaming.
- Per-process cache: multiple workers each warm their own copy.
- The ODFV transform is executed outside Feast on the cached path, so its code
  must stay self-contained (it already must be, for Feast serialization).
