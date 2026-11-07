import numpy as np

from benchmarks.compare import independent_diff, paired_diff, statistic_diff


def test_paired_detects_consistent_improvement_and_is_deterministic():
    rng = np.random.default_rng(0)
    a = rng.normal(0.30, 0.1, 500)
    b = a + 0.02 + rng.normal(0, 0.01, 500)
    d1, d2 = paired_diff("m", a, b), paired_diff("m", a, b)
    assert d1 == d2 and d1.significant and d1.ci_low > 0 and abs(d1.diff - 0.02) < 0.005


def test_null_difference_ci_covers_zero():
    rng = np.random.default_rng(1)
    a, b = rng.normal(0, 1, 400), rng.normal(0, 1, 400)
    assert not independent_diff("m", a, b).significant


def test_pairing_tightens_the_interval():
    rng = np.random.default_rng(2)
    base = rng.normal(0, 1, 300)
    a, b = base, base + 0.05 + rng.normal(0, 0.02, 300)
    paired, indep = paired_diff("m", a, b), independent_diff("m", a, b)
    assert (paired.ci_high - paired.ci_low) < (indep.ci_high - indep.ci_low) / 5


def test_statistic_diff_on_coverage():
    items_a = [[1, 2]] * 50
    items_b = [[1, 2], [3, 4]] * 25

    def coverage(idx, side):
        rows = items_a if side == "a" else items_b
        return len({i for j in idx for i in rows[j]}) / 10

    d = statistic_diff("coverage", coverage, 50, n_boot=200)
    assert d.a == 0.2 and d.b == 0.4 and d.significant


def test_biased_statistic_ci_contains_point_estimate():
    """Distinct-count statistics shrink under resampling; the corrected CI must still bracket the estimate."""
    rng = np.random.default_rng(3)
    items_a = [list(rng.integers(0, 300, 10)) for _ in range(200)]
    items_b = [list(rng.integers(0, 600, 10)) for _ in range(200)]

    def coverage(idx, side):
        rows = items_a if side == "a" else items_b
        return len({i for j in idx for i in rows[j]}) / 1000

    d = statistic_diff("coverage", coverage, 200, n_boot=300)
    assert d.ci_low <= d.diff <= d.ci_high
