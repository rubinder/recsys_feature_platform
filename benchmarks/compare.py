"""Reusable comparison of two result sets: deltas with seeded bootstrap confidence intervals.

Every benchmark uses it, and Phase 5's A/B lift analysis builds on it.

    per-unit metrics (one value per request / query / user):
        paired_diff(a, b)       a and b measured on the same units (counterfactual replay)
        independent_diff(a, b)  different units (A/B buckets)
    aggregate statistics (coverage, p99 latency, ...):
        statistic_diff(fn, n_a, n_b, paired)  fn(indices, side) -> float on resampled units

All functions are deterministic given `seed`.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass

import numpy as np

DEFAULT_BOOT = 2000
DEFAULT_SEED = 42


@dataclass(frozen=True)
class Delta:
    metric: str
    a: float
    b: float
    diff: float  # b - a
    rel: float  # (b - a) / a, NaN if a == 0
    ci_low: float
    ci_high: float
    n_a: int
    n_b: int
    alpha: float = 0.05

    @property
    def significant(self) -> bool:
        """CI excludes zero (the bootstrap analogue of p < alpha, two-sided)."""
        return self.ci_low > 0 or self.ci_high < 0

    def as_dict(self) -> dict:
        return {**asdict(self), "significant": self.significant}

    def fmt(self, digits: int = 4) -> str:
        rel = f" ({self.rel:+.1%})" if np.isfinite(self.rel) else ""
        star = " *" if self.significant else ""
        return (f"{self.metric}: {self.a:.{digits}f} -> {self.b:.{digits}f}, "
                f"diff {self.diff:+.{digits}f}{rel} [{self.ci_low:+.{digits}f}, {self.ci_high:+.{digits}f}]{star}")


def _rel(a: float, b: float) -> float:
    return (b - a) / a if a else float("nan")


def _ci(samples: np.ndarray, alpha: float) -> tuple[float, float]:
    lo, hi = np.quantile(samples, [alpha / 2, 1 - alpha / 2])
    return float(lo), float(hi)


def paired_diff(metric: str, a, b, n_boot: int = DEFAULT_BOOT, seed: int = DEFAULT_SEED, alpha: float = 0.05) -> Delta:
    """Mean(b) - mean(a) over the same units; resamples units, keeping pairs together."""
    a, b = np.asarray(a, float), np.asarray(b, float)
    if a.shape != b.shape:
        raise ValueError("paired comparison needs one value per unit on both sides")
    d = b - a
    idx = np.random.default_rng(seed).integers(0, len(d), size=(n_boot, len(d)))
    lo, hi = _ci(d[idx].mean(axis=1), alpha)
    return Delta(metric, float(a.mean()), float(b.mean()), float(d.mean()), _rel(a.mean(), b.mean()), lo, hi,
                 len(a), len(b), alpha)


def independent_diff(metric: str, a, b, n_boot: int = DEFAULT_BOOT, seed: int = DEFAULT_SEED, alpha: float = 0.05) -> Delta:
    """Mean(b) - mean(a) for independent samples (e.g. A/B buckets); resamples each side."""
    a, b = np.asarray(a, float), np.asarray(b, float)
    rng = np.random.default_rng(seed)
    ma = a[rng.integers(0, len(a), size=(n_boot, len(a)))].mean(axis=1)
    mb = b[rng.integers(0, len(b), size=(n_boot, len(b)))].mean(axis=1)
    lo, hi = _ci(mb - ma, alpha)
    return Delta(metric, float(a.mean()), float(b.mean()), float(b.mean() - a.mean()), _rel(a.mean(), b.mean()), lo, hi,
                 len(a), len(b), alpha)


def statistic_diff(metric: str, fn: Callable[[np.ndarray, str], float], n_a: int, n_b: int | None = None,
                   paired: bool = True, n_boot: int = 500, seed: int = DEFAULT_SEED, alpha: float = 0.05) -> Delta:
    """Aggregate statistics: fn(indices, side) with side in {"a", "b"}; resamples unit indices.

    Bias-corrected percentile interval. Statistics like coverage (distinct items)
    are biased under resampling with replacement: duplicated units add no new
    items, so bootstrap replicates are systematically lower than the full-sample
    value and a plain percentile CI can exclude the point estimate. The bootstrap
    distribution is shifted by (estimate - mean of replicates) before taking
    quantiles.
    """
    n_b = n_a if n_b is None else n_b
    rng = np.random.default_rng(seed)
    full_a, full_b = fn(np.arange(n_a), "a"), fn(np.arange(n_b), "b")
    diffs = np.empty(n_boot)
    for i in range(n_boot):
        ia = rng.integers(0, n_a, n_a)
        ib = ia if paired else rng.integers(0, n_b, n_b)
        diffs[i] = fn(ib, "b") - fn(ia, "a")
    est = full_b - full_a
    lo, hi = _ci(diffs + (est - diffs.mean()), alpha)
    return Delta(metric, float(full_a), float(full_b), float(est), _rel(full_a, full_b), lo, hi, n_a, n_b, alpha)
