import numpy as np
import pandas as pd

from recsys.eval.report import summarize
from recsys.features.derived import add_stream_derived
from recsys.serving.cache import TTLCache
from recsys.streaming.simulate import request_points
from recsys.training.candidates import ALSModel


def test_ttl_cache_hits_misses_and_expiry():
    now = [0.0]
    c = TTLCache(10, clock=lambda: now[0])
    c.put_many({1: {"a": 1}})
    found, missing = c.get_many([1, 2])
    assert found == {1: {"a": 1}} and missing == [2]
    now[0] = 11
    assert c.get_many([1])[1] == [1]
    assert c.stats()["hits"] == 1 and c.stats()["misses"] == 2


def _als():
    import scipy.sparse as sp

    f = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]])
    return ALSModel(np.array([1]), np.array([10, 20, 30]), np.array([[1.0, 0.0]]), f, sp.csr_matrix((1, 3)))


def test_derived_stream_features():
    df = pd.DataFrame({"user_id": [1, 1, 1], "item_id": [10, 20, 30],
                       "stream_last_event_ts": [1000, 1000, 1000], "stream_last_items": [[10]] * 3})
    out = add_stream_derived(df, _als(), pd.Timestamp(1000 + 60, unit="s", tz="UTC"))
    assert (out.stream_secs_since_last == 60).all() and (out.stream_in_session == 1).all()
    assert np.allclose(out.recent_item_sim, [1.0, 0.0, 1 / np.sqrt(2)])
    missing = add_stream_derived(df.assign(stream_last_event_ts=None, stream_last_items=None), _als(),
                                 pd.Timestamp(5000, unit="s", tz="UTC"))
    assert missing.recent_item_sim.isna().all() and missing.stream_in_session.isna().all()


def test_request_points_session_start_and_refresh():
    t = pd.Timestamp("2001-01-01", tz="UTC")
    ev = pd.DataFrame({"user_id": [1] * 5, "timestamp": [t, t + pd.Timedelta(minutes=1), t + pd.Timedelta(minutes=2),
                                                         t + pd.Timedelta(hours=2), t + pd.Timedelta(hours=2, minutes=1)]})
    ev["seq"] = range(5)
    rp = request_points(ev, h=0, end=t + pd.Timedelta(days=1), refresh_every=2)
    assert rp.is_request.tolist() == [True, False, True, True, False]


def test_report_attribution_and_coverage():
    imp = pd.DataFrame({"variant": ["a", "a", "b"], "user_id": [1, 2, 1], "items": [[1, 2], [3, 4], [5, 6]],
                        "relevant": [{2}, set(), {9}], "model_version": ["m"] * 3})
    s = summarize(imp, catalog_size=10, ks=(1, 2))
    assert s.loc["a", "ctr"] == 0.5 and s.loc["a", "item_ctr"] == 0.25 and s.loc["a", "hit_rate@1"] == 0.0
    assert s.loc["b", "ctr"] == 0.0 and s.loc["a", "coverage"] == 0.4
