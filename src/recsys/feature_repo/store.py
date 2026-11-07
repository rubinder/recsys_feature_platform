"""Build the FeatureStore from code so the Redis address can come from the env."""

from __future__ import annotations

import os
from functools import lru_cache

from feast import FeatureStore, RepoConfig

from recsys.config import DATA, FEATURE_REPO


def offline_store_config() -> dict:
    """Local DuckDB over Parquet by default; Snowflake under RECSYS_PROFILE=cloud."""
    from recsys.cloud import env, is_cloud

    if not is_cloud():
        return {"type": "duckdb"}
    return {
        "type": "snowflake.offline",
        "account": f"{env('SNOWFLAKE_ORGANIZATION_NAME')}-{env('SNOWFLAKE_ACCOUNT_NAME')}",
        "user": env("RECSYS_SF_USER"),
        "role": env("RECSYS_SF_ROLE"),
        "warehouse": env("RECSYS_SF_WAREHOUSE"),
        "database": env("RECSYS_SF_DATABASE"),
        "schema": env("RECSYS_SF_SCHEMA"),
        "authenticator": "SNOWFLAKE_JWT",
        "private_key": env("RECSYS_SF_PRIVATE_KEY_PATH"),
        "convert_timestamp_columns": False,  # True makes Feast emit an unquoted timestamp column (breaks lowercase columns)
    }


def repo_config() -> RepoConfig:
    from recsys.cloud import profile

    registry = "registry.db" if profile() == "local" else f"registry_{profile()}.db"
    # RECSYS_FEAST_REGISTRY (e.g. s3://<lake>/feast/registry_cloud.db) shares the registry with pods in the cluster.
    registry_path = os.environ.get("RECSYS_FEAST_REGISTRY") or str(DATA / "feast" / registry)
    return RepoConfig(
        project="recsys",
        provider="local",
        registry={"path": registry_path, "cache_ttl_seconds": 600},
        offline_store=offline_store_config(),
        online_store={"type": "redis", "connection_string": os.environ.get("REDIS_CONNECTION", "localhost:6379")},
        entity_key_serialization_version=3,
        repo_path=str(FEATURE_REPO),
    )


@lru_cache(maxsize=1)
def get_store() -> FeatureStore:
    if not os.environ.get("RECSYS_FEAST_REGISTRY"):
        (DATA / "feast").mkdir(parents=True, exist_ok=True)
    return FeatureStore(config=repo_config())


def apply() -> None:
    from recsys.feature_repo.definitions import OBJECTS

    get_store().apply(OBJECTS)
