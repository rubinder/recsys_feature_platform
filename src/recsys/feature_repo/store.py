"""Build the FeatureStore from code so the Redis address can come from the env."""

from __future__ import annotations

import os
from functools import lru_cache

from feast import FeatureStore, RepoConfig

from recsys.config import DATA, FEATURE_REPO


def repo_config() -> RepoConfig:
    return RepoConfig(
        project="recsys",
        provider="local",
        registry={"path": str(DATA / "feast" / "registry.db"), "cache_ttl_seconds": 600},
        offline_store={"type": "duckdb"},
        online_store={"type": "redis", "connection_string": os.environ.get("REDIS_CONNECTION", "localhost:6379")},
        entity_key_serialization_version=3,
        repo_path=str(FEATURE_REPO),
    )


@lru_cache(maxsize=1)
def get_store() -> FeatureStore:
    (DATA / "feast").mkdir(parents=True, exist_ok=True)
    return FeatureStore(config=repo_config())


def apply() -> None:
    from recsys.feature_repo.definitions import OBJECTS

    get_store().apply(OBJECTS)
