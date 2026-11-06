"""Online features -> ALS candidates -> ranker -> top-N."""

from __future__ import annotations

import json
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import mlflow
import pandas as pd
from feast import FeatureStore

from recsys import scoring, tracking
from recsys.config import ARTIFACTS
from recsys.features.features import FEATURE_REFS
from recsys.latency.histogram import LatencyHistogram
from recsys.training import candidates, ranker
from recsys.training.candidates import ALSModel


def resolve_run_id(explicit: str | None = None) -> str:
    if explicit:
        return explicit
    latest = ARTIFACTS / "latest_run_id"
    if not latest.exists():
        raise RuntimeError("no MODEL_RUN_ID set and no trained run found; run `make train`")
    return latest.read_text().strip()


@dataclass
class Recommendation:
    item_id: int
    score: float
    als_score: float | None


class Recommender:
    def __init__(self, run_id: str, store: FeatureStore, histogram: LatencyHistogram) -> None:
        tracking.setup()
        self.run_id = run_id
        self.store = store
        self.hist = histogram
        d = Path(tempfile.mkdtemp(prefix="recsys-model-"))
        path = Path(mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path="model", dst_path=str(d)))
        self.als: ALSModel = ALSModel.load(path / "als.npz")
        self.booster = ranker.load(path / "ranker.txt")
        self.manifest = json.loads((path / "manifest.json").read_text())
        if self.manifest["feature_refs"] != FEATURE_REFS:
            raise RuntimeError("model was trained on different feature refs than this service serves")

    def recommend(self, user_id: int, n: int) -> tuple[str, list[Recommendation], dict[str, float]]:
        timings: dict[str, float] = {}
        t0 = time.perf_counter()
        res = candidates.top_k(self.als, user_id, self.manifest["candidates_k"])
        timings["candidates"] = (time.perf_counter() - t0) * 1000
        if res is None:
            items = [Recommendation(int(i), 0.0, None) for i in self.manifest["popular_items"][:n]]
            timings["total"] = (time.perf_counter() - t0) * 1000
            self._observe(timings)
            return "popular_fallback", items, timings

        item_ids, als_scores = res
        t1 = time.perf_counter()
        rows = [{"user_id": int(user_id), "item_id": int(i)} for i in item_ids]
        feats = pd.DataFrame(self.store.get_online_features(features=FEATURE_REFS, entity_rows=rows).to_dict())
        timings["online_features"] = (time.perf_counter() - t1) * 1000

        t2 = time.perf_counter()
        feats["als_score"] = als_scores
        scores = scoring.score(self.booster, feats)
        order = sorted(range(len(item_ids)), key=lambda j: (-scores[j], int(item_ids[j])))[:n]
        timings["ranking"] = (time.perf_counter() - t2) * 1000
        timings["total"] = (time.perf_counter() - t0) * 1000
        self._observe(timings)
        return "two_stage", [Recommendation(int(item_ids[j]), float(scores[j]), float(als_scores[j])) for j in order], timings

    def _observe(self, timings: dict[str, float]) -> None:
        for stage, ms in timings.items():
            self.hist.observe(stage, ms)
