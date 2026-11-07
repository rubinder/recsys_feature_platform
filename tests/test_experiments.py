import numpy as np
import pytest

from recsys.experiments.assignment import Experiment, Variant
from recsys.experiments.lift import permutation_p, srm, two_proportion_z


def _exp(salt="s", w=0.5):
    return Experiment("e1", salt, (Variant("control", "champion", w), Variant("treatment", "challenger", 1 - w)))


def test_assignment_deterministic_balanced_and_salted():
    e = _exp()
    a = [e.assign(u).name for u in range(1, 20001)]
    assert a == [e.assign(u).name for u in range(1, 20001)]
    share = a.count("treatment") / len(a)
    assert 0.48 < share < 0.52
    flipped = sum(x != y for x, y in zip(a, [_exp(salt="other").assign(u).name for u in range(1, 20001)]))
    assert 0.4 < flipped / len(a) < 0.6  # a new salt reshuffles users


def test_weights_respected_and_validated():
    e = _exp(w=0.9)
    assert 0.88 < sum(e.assign(u).name == "control" for u in range(10000)) / 10000 < 0.92
    with pytest.raises(ValueError):
        Experiment("bad", "s", (Variant("a", "x", 0.6), Variant("b", "y", 0.6)))


def test_srm_flags_imbalance():
    assert not srm({"control": 500, "treatment": 510}, {"control": 0.5, "treatment": 0.5})["mismatch"]
    assert srm({"control": 500, "treatment": 700}, {"control": 0.5, "treatment": 0.5})["mismatch"]


def test_significance_tests():
    rng = np.random.default_rng(0)
    a, b = rng.normal(0, 1, 300), rng.normal(0.4, 1, 300)
    assert permutation_p(a, b, n=500) < 0.01
    assert permutation_p(a, rng.normal(0, 1, 300), n=500) > 0.05
    z, p = two_proportion_z(100, 1000, 140, 1000)
    assert z > 0 and p < 0.01
