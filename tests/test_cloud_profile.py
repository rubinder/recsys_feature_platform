"""Cloud profile config: switches offline store, sources and MLflow artifacts without connecting anywhere."""

import importlib
import os
import subprocess
import sys

CLOUD_ENV = {
    "RECSYS_PROFILE": "cloud", "SNOWFLAKE_ORGANIZATION_NAME": "ORG", "SNOWFLAKE_ACCOUNT_NAME": "ACCT",
    "RECSYS_SF_USER": "RECSYS_FEAST_SVC", "RECSYS_SF_ROLE": "RECSYS_FEAST", "RECSYS_SF_WAREHOUSE": "RECSYS_WH",
    "RECSYS_SF_DATABASE": "RECSYS", "RECSYS_SF_SCHEMA": "FEATURES", "RECSYS_SF_PRIVATE_KEY_PATH": "/tmp/k.p8",
    "RECSYS_MLFLOW_BUCKET": "recsys-mlflow-test", "RECSYS_SF_STAGE": "LAKE_OFFLINE", "RECSYS_LAKE_BUCKET": "lake",
}


def _run(code: str, extra: dict) -> str:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("RECSYS_", "SNOWFLAKE_"))}
    return subprocess.run([sys.executable, "-c", code], env={**env, **extra}, check=True,
                          capture_output=True, text=True).stdout.strip()


def test_local_profile_is_default():
    out = _run("from recsys.feature_repo.store import offline_store_config as c; print(c()['type'])", {})
    assert out == "duckdb"


def test_cloud_profile_uses_snowflake_offline_store_and_sources():
    code = ("from recsys.feature_repo.store import repo_config; from recsys.feature_repo import definitions as d;"
            "c = repo_config(); print(c.offline_store.type, c.offline_store.database, c.registry.path.endswith('registry_cloud.db'),"
            " type(d.user_stats.batch_source).__name__, d.user_stats.batch_source.table,"
            " type(d.user_stream_push.batch_source).__name__)")
    out = _run(code, CLOUD_ENV).split()
    assert out == ["snowflake.offline", "RECSYS", "True", "SnowflakeSource", "USER_STATS", "SnowflakeSource"]


def test_cloud_profile_sends_mlflow_artifacts_to_s3():
    out = _run("from recsys.tracking import experiment; print(*experiment())", CLOUD_ENV)
    assert out == "recsys-two-stage-cloud s3://recsys-mlflow-test/artifacts"


def test_cloud_profile_missing_env_fails_clearly():
    import pytest

    r = subprocess.run([sys.executable, "-c", "from recsys.feature_repo.store import offline_store_config as c; c()"],
                       env={**{k: v for k, v in os.environ.items() if not k.startswith(("RECSYS_", "SNOWFLAKE_"))},
                            "RECSYS_PROFILE": "cloud"}, capture_output=True, text=True)
    assert r.returncode != 0 and "terraform-apply" in r.stderr
    _ = (pytest, importlib)
