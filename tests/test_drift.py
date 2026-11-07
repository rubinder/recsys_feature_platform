import numpy as np

from recsys.monitoring.drift import level, psi


def test_psi_stable_vs_shifted():
    rng = np.random.default_rng(0)
    ref = rng.normal(0, 1, 5000)
    assert psi(ref, rng.normal(0, 1, 5000)) < 0.02
    shifted = psi(ref, rng.normal(1.0, 1, 5000))
    assert shifted > 0.25 and level(shifted) == "significant"


def test_psi_handles_degenerate_inputs():
    assert np.isnan(psi([1, 2], [1]))
    assert psi(np.ones(100), np.ones(50)) == 0.0
    assert np.isinf(psi(np.ones(100), np.full(50, 2.0)))
