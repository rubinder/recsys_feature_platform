"""Cloud profile (RECSYS_PROFILE=cloud): Snowflake offline store, S3 lake + MLflow artifacts."""

import os


def profile() -> str:
    return os.environ.get("RECSYS_PROFILE", "local")


def is_cloud() -> bool:
    return profile() == "cloud"


def env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"{name} is not set: run `make terraform-apply` (writes .env.cloud) and load it")
    return v
