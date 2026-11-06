"""Ranking metrics with binary relevance. Extended in Phase 2 (CTR, coverage)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import numpy as np


def hit_rate_at_k(ranked: Sequence[int], relevant: set[int], k: int) -> float:
    return float(any(i in relevant for i in ranked[:k]))


def recall_at_k(ranked: Sequence[int], relevant: set[int], k: int) -> float:
    return len(set(ranked[:k]) & relevant) / len(relevant) if relevant else 0.0


def ndcg_at_k(ranked: Sequence[int], relevant: set[int], k: int) -> float:
    gains = [1.0 / np.log2(r + 2) for r, i in enumerate(ranked[:k]) if i in relevant]
    ideal = sum(1.0 / np.log2(r + 2) for r in range(min(len(relevant), k)))
    return float(sum(gains) / ideal) if ideal else 0.0


def evaluate(ranked: Mapping[int, Sequence[int]], truth: Mapping[int, set[int]], ks: Sequence[int]) -> dict[str, float]:
    """Mean metrics over users that have both a ranking and non-empty ground truth."""
    users = [u for u in ranked if truth.get(u)]
    out: dict[str, float] = {"users": float(len(users))}
    for k in ks:
        out[f"hit_rate_at_{k}"] = float(np.mean([hit_rate_at_k(ranked[u], truth[u], k) for u in users]))
        out[f"ndcg_at_{k}"] = float(np.mean([ndcg_at_k(ranked[u], truth[u], k) for u in users]))
        out[f"recall_at_{k}"] = float(np.mean([recall_at_k(ranked[u], truth[u], k) for u in users]))
    return out
