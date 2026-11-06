"""MLflow tracking setup: local SQLite metadata + file artifacts under mlruns/."""

from __future__ import annotations

import os

os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")

import mlflow

from recsys.config import MLRUNS

EXPERIMENT = "recsys-two-stage"


def tracking_uri() -> str:
    return os.environ.get("MLFLOW_TRACKING_URI", f"sqlite:///{MLRUNS / 'mlflow.db'}")


def setup() -> None:
    MLRUNS.mkdir(parents=True, exist_ok=True)
    mlflow.set_tracking_uri(tracking_uri())
    if mlflow.get_experiment_by_name(EXPERIMENT) is None:
        mlflow.create_experiment(EXPERIMENT, artifact_location=(MLRUNS / "artifacts").as_uri())
    mlflow.set_experiment(EXPERIMENT)
