"""Integration tests against the built pipeline (skipped on a clean checkout)."""

from tests.conftest import requires_pipeline


@requires_pipeline
def test_online_offline_parity():
    from recsys.features.parity import check

    from recsys.features.parity import _replayed

    rows, cols, bad = check(n=100)
    # 18 = batch + streaming at the cutoff; after a replay only the 11 batch
    # features are comparable at the cutoff (streaming parity is checked separately).
    assert rows > 50 and cols == (11 if _replayed() else 18) and bad == []


@requires_pipeline
def test_api_returns_ranked_items():
    from fastapi.testclient import TestClient

    from recsys.config import ARTIFACTS

    if not (ARTIFACTS / "latest_run_id").exists():
        import pytest

        pytest.skip("no trained model")
    from recsys.serving.app import app

    with TestClient(app) as client:
        r = client.get("/recommendations/6040").json()
        assert r["strategy"] == "two_stage" and len(r["items"]) == 10
        scores = [i["score"] for i in r["items"]]
        assert scores == sorted(scores, reverse=True)
        assert client.get("/recommendations/1").json()["strategy"] == "popular_fallback"
        assert client.get("/latency").json()["total"]["count"] >= 2


@requires_pipeline
def test_cached_and_direct_feature_paths_match():
    import numpy as np
    import pandas as pd

    from recsys.feature_repo.store import get_store
    from recsys.latency.histogram import LatencyHistogram
    from recsys.serving.cache import TTLCache
    from recsys.serving.recommender import Recommender, discover_variants
    from recsys.training import candidates

    rec = Recommender(discover_variants(), get_store(), LatencyHistogram(), TTLCache(300), "batch")
    for m in rec.models.values():
        ids, _ = candidates.top_k(m.als, 6040, 50)
        a = rec.online_features(m.fs, 6040, ids, False)
        b = rec.online_features(m.fs, 6040, ids, True)
        for c in [c for c in m.fs.model_features if c in a.columns]:
            assert np.allclose(pd.to_numeric(a[c]), pd.to_numeric(b[c]), equal_nan=True), (m.name, c)
