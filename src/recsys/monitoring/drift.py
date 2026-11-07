"""Population Stability Index (PSI) for feature and prediction drift.

PSI = sum((cur% - ref%) * ln(cur% / ref%)) over bins fixed on the reference
distribution (quantile edges). Rule of thumb: < 0.1 stable, 0.1-0.25 moderate,
> 0.25 significant shift.
"""

from __future__ import annotations

import numpy as np

EPS = 1e-4
MODERATE, SIGNIFICANT = 0.10, 0.25


def psi(reference, current, bins: int = 10) -> float:
    ref = np.asarray(reference, float)
    cur = np.asarray(current, float)
    ref, cur = ref[np.isfinite(ref)], cur[np.isfinite(cur)]
    if len(ref) < bins or len(cur) == 0:
        return float("nan")
    edges = np.unique(np.quantile(ref, np.linspace(0, 1, bins + 1)))
    if len(edges) < 2:  # constant reference: any change is a shift
        return float(0.0 if np.allclose(cur, ref[0]) else np.inf)
    edges[0], edges[-1] = -np.inf, np.inf
    r = np.histogram(ref, edges)[0] / len(ref)
    c = np.histogram(cur, edges)[0] / len(cur)
    r, c = np.clip(r, EPS, None), np.clip(c, EPS, None)
    return float(np.sum((c - r) * np.log(c / r)))


def level(value: float) -> str:
    if not np.isfinite(value):
        return "n/a"
    return "significant" if value > SIGNIFICANT else "moderate" if value > MODERATE else "stable"
