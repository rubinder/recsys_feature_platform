"""Champion/challenger promotion gate.

A challenger is promoted only if
  1. every Phase 3 data-quality check (ERROR severity) passes on the data it was
     trained on, and
  2. it is non-inferior to the champion on held-out ranking quality: the paired
     bootstrap 95% CI of (challenger - champion) NDCG@10 over the same queries has
     a lower bound above -MARGIN (Phase 4 comparison module).

Held-out set: mid-session validation queries (the serving distribution, ADR 0010),
scored by each model with its own feature set.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

from benchmarks.common import per_query
from benchmarks.compare import Delta, paired_diff
from recsys import scoring
from recsys.config import load_split
from recsys.features.features import STREAMING
from recsys.quality import checks
from recsys.serving.recommender import ModelBundle
from recsys.training.train import datasets

MARGIN = 0.005


@dataclass
class GateResult:
    passed: bool
    dq_passed: bool
    dq_failures: list[str]
    delta: dict
    margin: float
    queries: int
    reason: str

    def as_dict(self) -> dict:
        return asdict(self)


def heldout_rows() -> pd.DataFrame:
    _, va, _ = datasets(STREAMING, "session")
    return va


def score_queries(run_id: str, rows: pd.DataFrame) -> pd.DataFrame:
    b = ModelBundle("gate", run_id)
    return per_query(rows.assign(score=scoring.score(b.booster, rows, b.fs.model_features)), "score")


def evaluate(champion_run: str, challenger_run: str, rows: pd.DataFrame | None = None) -> GateResult:
    outcomes = [o for fn in checks.ALL.values() for o in fn()]
    failures = [o.name for o in outcomes if not o.passed and o.severity == "ERROR"]
    rows = heldout_rows() if rows is None else rows
    a, b = score_queries(champion_run, rows), score_queries(challenger_run, rows)
    d: Delta = paired_diff("heldout_ndcg@10", a["ndcg@10"], b["ndcg@10"], seed=load_split().seed)
    non_inferior = d.ci_low > -MARGIN
    passed = not failures and non_inferior
    if failures:
        reason = f"data quality failed: {', '.join(failures)}"
    elif not non_inferior:
        reason = f"inferior to champion: NDCG@10 diff {d.diff:+.4f}, CI low {d.ci_low:+.4f} <= -{MARGIN}"
    else:
        reason = f"non-inferior: NDCG@10 diff {d.diff:+.4f} [{d.ci_low:+.4f}, {d.ci_high:+.4f}], data quality ok"
    return GateResult(passed, not failures, failures, d.as_dict(), MARGIN, len(a), reason)
