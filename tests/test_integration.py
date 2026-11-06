"""Integration tests against the built pipeline (skipped on a clean checkout)."""

from tests.conftest import requires_pipeline


@requires_pipeline
def test_online_offline_parity():
    from recsys.features.parity import check

    rows, cols, bad = check(n=100)
    assert rows > 50 and cols == 11 and bad == []


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
