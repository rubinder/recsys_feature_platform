"""Online features -> ALS candidates -> ranker -> top-N, for one or more model variants."""

from __future__ import annotations

import json
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
from feast import FeatureStore

from recsys import scoring, tracking
from recsys.config import ARTIFACTS
from recsys.feature_repo.definitions import user_item_affinity
from recsys.features.derived import add_stream_derived
from recsys.features.features import FEATURE_SETS, ITEM_REFS, FeatureSet
from recsys.latency.histogram import LatencyHistogram
from recsys.serving.cache import TTLCache
from recsys.streaming.logic import LAST_N
from recsys.training import candidates, ranker
from recsys.training.candidates import ALSModel

AFFINITY_REF = "user_item_affinity:genre_affinity"


def discover_variants() -> dict[str, str]:
    """Variant name -> MLflow run id, from the run ids recorded by `make train*`."""
    out = {}
    if (ARTIFACTS / "latest_run_id").exists():
        out["batch"] = (ARTIFACTS / "latest_run_id").read_text().strip()
    for p in sorted(ARTIFACTS.glob("latest_run_id_*")):
        out[p.name.removeprefix("latest_run_id_")] = p.read_text().strip()
    if not out:
        raise RuntimeError("no trained runs found; run `make train`")
    return out


def parse_variants(spec: str | None) -> dict[str, str]:
    """MODEL_VARIANTS="name=run_id,name2=run_id2"; empty -> discover."""
    if not spec:
        return discover_variants()
    return dict(part.split("=", 1) for part in spec.split(",") if part)


@dataclass
class Recommendation:
    item_id: int
    score: float
    als_score: float | None


@dataclass
class Result:
    request_id: str
    variant: str
    model_version: str
    strategy: str
    items: list[Recommendation]
    timings: dict[str, float]
    as_of: datetime


class ModelBundle:
    def __init__(self, name: str, run_id: str) -> None:
        self.name, self.run_id = name, run_id
        d = Path(tempfile.mkdtemp(prefix=f"recsys-{name}-"))
        path = Path(mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path="model", dst_path=str(d)))
        self.als: ALSModel = ALSModel.load(path / "als.npz")
        self.booster = ranker.load(path / "ranker.txt")
        self.manifest = json.loads((path / "manifest.json").read_text())
        self.fs: FeatureSet = FEATURE_SETS[self.manifest.get("feature_set", "batch")]
        if self.manifest["feature_refs"] != self.fs.refs:
            raise RuntimeError(f"variant {name}: model feature refs differ from the served feature set")


class Recommender:
    def __init__(self, variants: dict[str, str], store: FeatureStore, histogram: LatencyHistogram,
                 cache: TTLCache | None, default_variant: str) -> None:
        tracking.setup()
        self.store, self.hist, self.cache = store, histogram, cache
        self.models = {name: ModelBundle(name, rid) for name, rid in variants.items()}
        self.default_variant = default_variant if default_variant in self.models else next(iter(self.models))

    # ---- online feature retrieval -------------------------------------------------
    def _features_direct(self, fs: FeatureSet, user_id: int, item_ids) -> pd.DataFrame:
        rows = [{"user_id": int(user_id), "item_id": int(i)} for i in item_ids]
        return pd.DataFrame(self.store.get_online_features(features=fs.refs, entity_rows=rows).to_dict())

    def _features_cached(self, fs: FeatureSet, user_id: int, item_ids) -> pd.DataFrame:
        """1-row user read + cached item reads; genre_affinity via the same ODFV transform."""
        user_refs = [r for r in fs.refs if r.startswith(("user_stats:", "user_stream:"))]
        user_refs.append("user_genre_affinity:user_genre_share")
        u = self.store.get_online_features(features=user_refs, entity_rows=[{"user_id": int(user_id)}]).to_dict()
        found, missing = self.cache.get_many(int(i) for i in item_ids)
        if missing:
            got = self.store.get_online_features(features=ITEM_REFS, entity_rows=[{"item_id": i} for i in missing]).to_dict()
            fresh = {int(got["item_id"][j]): {k: v[j] for k, v in got.items() if k != "item_id"} for j in range(len(missing))}
            self.cache.put_many(fresh)
            found.update(fresh)
        df = pd.DataFrame([found[int(i)] for i in item_ids])
        df.insert(0, "item_id", [int(i) for i in item_ids])
        df.insert(0, "user_id", int(user_id))
        for k, v in u.items():
            if k != "user_id":
                df[k] = [v[0]] * len(df)
        if AFFINITY_REF in fs.refs:
            df["genre_affinity"] = user_item_affinity.feature_transformation.udf(df)["genre_affinity"].to_numpy()
        return df

    def online_features(self, fs: FeatureSet, user_id: int, item_ids, use_cache: bool) -> pd.DataFrame:
        if use_cache and self.cache is not None:
            return self._features_cached(fs, user_id, item_ids)
        return self._features_direct(fs, user_id, item_ids)

    # ---- request ------------------------------------------------------------------
    def recommend(self, user_id: int, n: int, variant: str | None = None, as_of: datetime | None = None,
                  use_cache: bool = True, k: int | None = None) -> Result:
        m = self.models[variant or self.default_variant]
        as_of = as_of or datetime.fromisoformat(m.manifest["serving_as_of"])
        if as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=timezone.utc)
        rid = uuid.uuid4().hex
        timings: dict[str, float] = {}
        t0 = time.perf_counter()
        k = k or m.manifest["candidates_k"]  # candidates fetched from the online store (benchmark knob)
        # Streaming variants also drop items the user consumed since the batch cutoff
        # (known from stream_last_items), so over-fetch by LAST_N before filtering.
        res = candidates.top_k(m.als, user_id, k + (LAST_N if m.fs.uses_stream else 0))
        timings["candidates"] = (time.perf_counter() - t0) * 1000
        if res is None:
            items = [Recommendation(int(i), 0.0, None) for i in m.manifest["popular_items"][:n]]
            timings["total"] = (time.perf_counter() - t0) * 1000
            self._observe(timings)
            return Result(rid, m.name, m.run_id, "popular_fallback", items, timings, as_of)

        item_ids, als_scores = res
        t1 = time.perf_counter()
        feats = self.online_features(m.fs, user_id, item_ids, use_cache)
        timings["online_features"] = (time.perf_counter() - t1) * 1000

        t2 = time.perf_counter()
        feats["als_score"] = als_scores
        if m.fs.uses_stream:
            recent = feats["stream_last_items"].iloc[0]
            if recent is not None and len(recent):
                feats = feats[~feats["item_id"].isin(set(recent))]
            feats = feats.head(k).reset_index(drop=True)
            feats = add_stream_derived(feats, m.als, pd.Timestamp(as_of))
        scores = scoring.score(m.booster, feats, m.fs.model_features)
        ids = feats["item_id"].to_numpy()
        als = feats["als_score"].to_numpy()
        order = sorted(range(len(ids)), key=lambda j: (-scores[j], int(ids[j])))[:n]
        timings["ranking"] = (time.perf_counter() - t2) * 1000
        timings["total"] = (time.perf_counter() - t0) * 1000
        self._observe(timings)
        items = [Recommendation(int(ids[j]), float(scores[j]), float(als[j])) for j in order]
        return Result(rid, m.name, m.run_id, "two_stage", items, timings, as_of)

    def _observe(self, timings: dict[str, float]) -> None:
        for stage, ms in timings.items():
            self.hist.observe(stage, ms)


def as_impression(r: Result, user_id: int, experiment_id: str | None = None) -> dict:
    return {
        "request_id": r.request_id,
        "user_id": user_id,
        "items": [i.item_id for i in r.items],
        "scores": [i.score for i in r.items],
        "ranks": list(range(1, len(r.items) + 1)),
        "model_version": r.model_version,
        "variant": r.variant,
        "experiment_id": experiment_id,
        "strategy": r.strategy,
        "timestamp": r.as_of,
        "served_at": datetime.now(timezone.utc),
    }


_ = np  # numpy used by callers via feats; keep import explicit for type checkers
