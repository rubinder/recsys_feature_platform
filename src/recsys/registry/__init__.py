"""MLflow model registry: versions, champion/challenger aliases, rollback history.

A registered version points at a training run's `model/` artifact directory
(ALS factors + LightGBM ranker + manifest), so one version is the whole
two-stage bundle. Aliases: `champion` (what serving's default variant loads),
`challenger` (candidate under evaluation / in the A/B test). Every promotion
appends to a history tag so `rollback` can restore the previous champion.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import mlflow
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from recsys import tracking

MODEL = "recsys-ranker"
PREFIX = f"models:/{MODEL}@"


def client() -> MlflowClient:
    tracking.setup()
    return MlflowClient()


def ensure_model() -> None:
    c = client()
    try:
        c.get_registered_model(MODEL)
    except MlflowException:
        c.create_registered_model(MODEL, description="Two-stage recommender: ALS candidates + LightGBM LambdaRank")


def register(run_id: str, tags: dict | None = None) -> str:
    """New model version for a training run (idempotent per run)."""
    ensure_model()
    c = client()
    for v in c.search_model_versions(f"name='{MODEL}'"):
        if v.run_id == run_id:
            return v.version
    run = c.get_run(run_id)
    v = c.create_model_version(MODEL, source=f"{run.info.artifact_uri}/model", run_id=run_id,
                               tags={k: str(val) for k, val in (tags or {}).items()})
    return v.version


def fresh_client() -> MlflowClient:
    """A client whose registry store drops pooled DB connections first. A long-lived
    process (the API) can otherwise keep reading a stale SQLite snapshot and miss an
    alias another process just moved."""
    c = client()
    try:
        c._get_registry_client().store.engine.dispose()
    except Exception:  # non-SQL stores (e.g. an MLflow server URI) have no engine
        pass
    return c


def alias_version(alias: str, fresh: bool = False) -> str | None:
    try:
        return (fresh_client() if fresh else client()).get_model_version_by_alias(MODEL, alias).version
    except MlflowException:
        return None


def run_of(version: str) -> str:
    return client().get_model_version(MODEL, version).run_id


def resolve(spec: str, fresh: bool = False) -> tuple[str, str | None]:
    """'models:/recsys-ranker@alias', a run id, or an artifact URI (s3://.../model) -> (run id or URI, version)."""
    if "://" in spec:  # direct artifact location: serving without a tracking server (EKS)
        return spec, None
    if spec.startswith(PREFIX):
        v = alias_version(spec[len(PREFIX):], fresh=fresh)
        if v is None:
            raise LookupError(f"registry alias {spec} is not set")
        return run_of(v), v
    return spec, None


def _history() -> list[dict]:
    m = client().get_registered_model(MODEL)
    return json.loads(m.tags.get("champion_history", "[]"))


def set_alias(alias: str, version: str) -> None:
    client().set_registered_model_alias(MODEL, alias, version)


def promote(version: str, reason: str) -> str | None:
    """Make `version` the champion; returns the previous champion version."""
    prev = alias_version("champion")
    c = client()
    set_alias("champion", version)
    hist = _history() + [{"version": version, "previous": prev, "reason": reason,
                          "at": datetime.now(timezone.utc).isoformat()}]
    c.set_registered_model_tag(MODEL, "champion_history", json.dumps(hist))
    c.set_model_version_tag(MODEL, version, "stage", "champion")
    if prev and prev != version:
        c.set_model_version_tag(MODEL, prev, "stage", "previous-champion")
    return prev


def rollback(reason: str = "manual rollback") -> tuple[str, str]:
    """Restore the champion that preceded the current one; returns (from, to)."""
    cur = alias_version("champion")
    hist = _history()
    prev = next((h["previous"] for h in reversed(hist) if h["version"] == cur and h["previous"]), None)
    if not prev:
        raise RuntimeError("no previous champion to roll back to")
    promote(prev, reason)
    client().set_model_version_tag(MODEL, cur, "stage", "rolled-back")
    return cur, prev


def versions() -> list[dict]:
    c = client()
    aliases: dict[str, list[str]] = {}
    for a, v in c.get_registered_model(MODEL).aliases.items():
        aliases.setdefault(v, []).append(a)
    out = []
    for v in sorted(c.search_model_versions(f"name='{MODEL}'"), key=lambda x: int(x.version)):
        out.append({"version": v.version, "run_id": v.run_id, "aliases": aliases.get(v.version, []),
                    "variant": v.tags.get("variant"), "stage": v.tags.get("stage"), "gate": v.tags.get("gate")})
    return out


_ = mlflow
