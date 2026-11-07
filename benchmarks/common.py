"""Shared helpers: result files, MLflow logging, per-query ranking metrics."""

from __future__ import annotations

import json
import os
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd

from recsys import tracking
from recsys.eval.metrics import hit_rate_at_k, ndcg_at_k

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "benchmarks" / "results"
IMG = ROOT / "docs" / "img"


def save(name: str, payload: dict) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / f"{name}.json"
    path.write_text(json.dumps(payload, indent=2, default=float))
    return path


def load(name: str) -> dict | None:
    path = RESULTS / f"{name}.json"
    return json.loads(path.read_text()) if path.exists() else None


def mlflow_run(benchmark: str, run_name: str, params: dict, metrics: dict) -> str:
    """One MLflow run per benchmark configuration, in experiment benchmarks-<name>."""
    tracking.setup()
    exp = f"benchmarks-{benchmark}"
    if mlflow.get_experiment_by_name(exp) is None:
        mlflow.create_experiment(exp, artifact_location=(ROOT / "mlruns" / "artifacts").as_uri())
    mlflow.set_experiment(exp)
    with mlflow.start_run(run_name=run_name) as r:
        mlflow.log_params(params)
        clean = {k.replace("@", "_at_"): float(v) for k, v in metrics.items()
                 if isinstance(v, (int, float, np.floating)) and np.isfinite(v)}
        mlflow.log_metrics(clean)
        return r.info.run_id


def per_query(rows: pd.DataFrame, score_col: str, k: int = 10) -> pd.DataFrame:
    """Rank candidates within each (anchor, user) query; per-query NDCG@k / hit@k and the top-k items."""
    ranked = rows.sort_values(["event_timestamp", "user_id", score_col, "item_id"], ascending=[True, True, False, True])
    out = []
    for (t, u), g in ranked.groupby(["event_timestamp", "user_id"], sort=True):
        items = g.item_id.tolist()
        rel = set(g.item_id[g.label == 1])
        out.append({"event_timestamp": t, "user_id": u, f"ndcg@{k}": ndcg_at_k(items, rel, k),
                    f"hit@{k}": hit_rate_at_k(items, rel, k), "top": items[:k]})
    return pd.DataFrame(out)


def coverage_fn(tops_a: list[list[int]], tops_b: list[list[int]], catalog: int):
    def fn(idx: np.ndarray, side: str) -> float:
        tops = tops_a if side == "a" else tops_b
        return len({i for j in idx for i in tops[j]}) / catalog
    return fn


def env_flag(name: str) -> bool:
    return os.environ.get(name, "").lower() in ("1", "true", "yes")
