import numpy as np
import pandas as pd

from recsys.training import candidates


def _events():
    rng = np.random.default_rng(0)
    return pd.DataFrame(
        {"user_id": rng.integers(0, 50, 2000), "item_id": rng.integers(0, 100, 2000), "weight": rng.integers(1, 6, 2000).astype(float)}
    )


def test_top_k_excludes_seen_and_is_deterministic(tmp_path):
    ev = _events()
    m = candidates.fit(ev, seed=7)
    items, scores = candidates.top_k(m, 3, 20)
    assert not set(items) & set(ev[ev.user_id == 3].item_id)
    assert np.all(np.diff(scores) <= 0)
    m2 = candidates.fit(ev, seed=7)
    assert np.array_equal(items, candidates.top_k(m2, 3, 20)[0])
    m.save(tmp_path / "als.npz")
    assert np.array_equal(items, candidates.top_k(candidates.ALSModel.load(tmp_path / "als.npz"), 3, 20)[0])


def test_unknown_user_is_none():
    assert candidates.top_k(candidates.fit(_events(), seed=0), 10_000, 5) is None
