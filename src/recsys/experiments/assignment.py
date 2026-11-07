"""Deterministic, salted hash bucketing of users into experiment variants."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

from recsys.config import ROOT

CONFIG = ROOT / "config" / "experiments.yaml"
BUCKETS = 10_000


@dataclass(frozen=True)
class Variant:
    name: str
    model: str
    weight: float


@dataclass(frozen=True)
class Experiment:
    id: str
    salt: str
    variants: tuple[Variant, ...]
    primary_metric: str = "ctr"
    guardrails: tuple[tuple[str, float], ...] = ()

    def __post_init__(self) -> None:
        total = sum(v.weight for v in self.variants)
        if abs(total - 1.0) > 1e-9:
            raise ValueError(f"experiment {self.id}: weights sum to {total}, not 1")

    def bucket(self, user_id: int) -> int:
        h = hashlib.sha256(f"{self.id}:{self.salt}:{user_id}".encode()).digest()
        return int.from_bytes(h[:8], "big") % BUCKETS

    def assign(self, user_id: int) -> Variant:
        b, edge = self.bucket(user_id), 0.0
        for v in self.variants:
            edge += v.weight * BUCKETS
            if b < edge:
                return v
        return self.variants[-1]

    @property
    def expected_shares(self) -> dict[str, float]:
        return {v.name: v.weight for v in self.variants}


def _parse(eid: str, spec: dict) -> Experiment:
    return Experiment(
        id=eid, salt=str(spec["salt"]),
        variants=tuple(Variant(n, v["model"], float(v["weight"])) for n, v in spec["variants"].items()),
        primary_metric=spec.get("primary_metric", "ctr"),
        guardrails=tuple((spec.get("guardrails") or {}).items()),
    )


@lru_cache(maxsize=4)
def load(path: Path = CONFIG) -> tuple[Experiment | None, dict[str, Experiment]]:
    cfg = yaml.safe_load(path.read_text())
    exps = {eid: _parse(eid, spec) for eid, spec in cfg.get("experiments", {}).items()}
    active = exps.get(cfg.get("active")) if cfg.get("active") else None
    return active, exps


def active_experiment() -> Experiment | None:
    return load()[0]
