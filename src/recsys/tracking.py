"""MLflow tracking setup: local SQLite metadata + file artifacts under mlruns/."""

from __future__ import annotations

import os

os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")

import mlflow

from recsys.config import MLRUNS

EXPERIMENT = "recsys-two-stage"


def tracking_uri() -> str:
    return os.environ.get("MLFLOW_TRACKING_URI", f"sqlite:///{MLRUNS / 'mlflow.db'}")


def experiment() -> tuple[str, str]:
    """(name, artifact location): local files by default, the S3 MLflow bucket under the cloud profile."""
    from recsys.cloud import env, is_cloud

    if is_cloud():
        return f"{EXPERIMENT}-cloud", f"s3://{env('RECSYS_MLFLOW_BUCKET')}/artifacts"
    return EXPERIMENT, (MLRUNS / "artifacts").as_uri()


def setup() -> None:
    MLRUNS.mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(tracking_uri())
    name, location = experiment()
    if mlflow.get_experiment_by_name(name) is None:
        mlflow.create_experiment(name, artifact_location=location)
    mlflow.set_experiment(name)
