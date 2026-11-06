# 0004: Parquet + Feast DuckDB offline store, Redis online store

- Status: accepted
- Phase: 1

## Context
The offline store must support point-in-time joins, run with zero setup on a
laptop, and be swappable for a warehouse (Snowflake in Phase 3).

## Options considered
1. Feast `file` offline store (pandas/dask): simplest, slow on larger joins.
2. **Feast `duckdb` offline store over Parquet**: fast, in-process, no server.
3. A local Postgres or Spark offline store: more moving parts.

## Decision
PySpark writes one sorted Parquet file per feature view; Feast reads them with
the DuckDB offline store. Redis 7 (docker-compose) is the online store. The
FeatureStore is built from code (`feature_repo/store.py`) so the Redis address
and, in Phase 3, the offline store type come from configuration.

## Consequences
- The point-in-time join for ~250k training rows takes seconds.
- Switching to Snowflake is a config change plus loading the same Parquet
  through an S3 stage (Phase 3).
- Feast does not preserve entity-dataframe row order on historical retrieval;
  training always merges results back on `(user_id, item_id, event_timestamp)`
  and asserts the row count is unchanged.
