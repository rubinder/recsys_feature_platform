"""Typed configuration (split cutoffs, label window, K, N, seeds) and repo paths."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel, field_validator

ROOT = Path(os.environ.get("RECSYS_ROOT", Path(__file__).resolve().parents[2]))
DATA = ROOT / "data"
RAW = DATA / "raw"
OFFLINE = DATA / "offline"
ARTIFACTS = DATA / "artifacts"
FEATURE_REPO = ROOT / "src" / "recsys" / "feature_repo"
MLRUNS = ROOT / "mlruns"
SPLIT_CONFIG = ROOT / "config" / "split.yaml"


class SplitConfig(BaseModel):
    """The fixed, time-based evaluation protocol shared by every phase."""

    train_end: datetime
    validation_end: datetime
    ranker_window_days: int
    label_window_days: int
    snapshot_cadence_days: int
    candidates_k: int
    top_n: int
    eval_k: list[int]
    seed: int

    @field_validator("train_end", "validation_end", mode="before")
    @classmethod
    def _utc(cls, v: object) -> datetime:
        dt = v if isinstance(v, datetime) else datetime.fromisoformat(str(v))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


@lru_cache(maxsize=1)
def load_split(path: Path = SPLIT_CONFIG) -> SplitConfig:
    return SplitConfig.model_validate(yaml.safe_load(path.read_text()))
