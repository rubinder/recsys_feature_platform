# 0007: Online writes through Feast push; offline writes as append-only parts

- Status: accepted
- Phase: 2

## Context
The plan was for Flink to push every streaming feature row through Feast's
feature server with `to=online_and_offline`. With the local file/DuckDB offline
store, Feast implements an offline append by reading the entire existing Parquet
file, concatenating, and rewriting it. With ~900k backfilled rows the feature
server was OOM-killed (exit 137) during a replay.

## Options considered
1. Keep `online_and_offline` and push offline rarely (one huge rewrite).
2. **Push online through Feast; write offline rows as append-only Parquet part
   files into the push source's batch-source directory**, which Feast reads via
   a glob.
3. Swap the local offline store for a database that appends natively.

## Decision
Option 2 locally. Flink micro-batches online pushes to the feature server
(flush at 200 rows or after 20 ms without new events) and writes offline parts
of up to 2,000 rows or 10 s, atomically (temp file, then rename). The backfill
is one more part in the same directory, so training reads history and replayed
rows as one table. In Phase 3, with Snowflake as the offline store, Feast's
offline push appends natively and `to=offline` becomes viable again.

## Consequences
- Constant cost per offline write instead of O(history).
- Readers never see partial files.
- Many small parts accumulate on long replays; compaction is not implemented.
- The online path still goes through Feast's push API, never raw Redis keys.
