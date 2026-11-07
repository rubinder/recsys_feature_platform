"""Data contracts for events crossing a system boundary.

Kept free of heavy dependencies (pydantic only) because the Flink job imports
this module too. Malformed records are rejected, never coerced silently.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SCHEMA_VERSION = 1
IMPRESSION_SCHEMA_VERSION = 2  # v2 (Phase 5): optional latency_ms; v1 rows remain valid


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=False)

    @field_validator("timestamp", check_fields=False)
    @classmethod
    def _aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return v


class InteractionEvent(_Contract):
    """One user-item interaction on the `interactions` topic and in the interaction log."""

    schema_version: Literal[1] = SCHEMA_VERSION
    seq: int = Field(ge=0, description="global replay order; also the stream progress marker")
    user_id: int = Field(gt=0)
    item_id: int = Field(gt=0)
    event_type: str = Field(min_length=1, max_length=32)
    weight: float
    timestamp: datetime

    @field_validator("weight")
    @classmethod
    def _finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("weight must be finite")
        return v


class Impression(_Contract):
    """What the serving layer showed, to whom, from which model.

    Schema evolution is additive only: v2 added the optional `latency_ms`, so
    readers handle both versions and v1 rows stay valid.
    """

    schema_version: Literal[1, 2] = IMPRESSION_SCHEMA_VERSION
    request_id: str = Field(min_length=8)
    user_id: int = Field(gt=0)
    items: list[int] = Field(min_length=1)
    scores: list[float]
    ranks: list[int]
    model_version: str = Field(min_length=1)
    variant: str = Field(default="control", min_length=1)
    experiment_id: str | None = None
    strategy: str
    timestamp: datetime  # event-time "now" the recommendation was made for
    served_at: datetime  # wall clock
    latency_ms: float | None = Field(default=None, ge=0)  # v2: server-side total latency

    @model_validator(mode="after")
    def _aligned(self) -> Impression:
        if not (len(self.items) == len(self.scores) == len(self.ranks)):
            raise ValueError("items, scores and ranks must have equal length")
        if len(set(self.items)) != len(self.items):
            raise ValueError("items must be unique")
        if self.ranks != list(range(1, len(self.items) + 1)):
            raise ValueError("ranks must be 1..n")
        return self
