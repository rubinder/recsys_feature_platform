"""The one global event order (seq) shared by producer, simulator, Flink and backfill."""

from __future__ import annotations

import pandas as pd

from recsys.config import RAW, load_split
from recsys.contracts import InteractionEvent


def ordered_events() -> pd.DataFrame:
    """All events sorted by (timestamp, user_id, item_id) with seq = position."""
    ev = pd.read_parquet(RAW / "events.parquet")
    ev["timestamp"] = pd.to_datetime(ev.timestamp, utc=True)
    ev = ev.sort_values(["timestamp", "user_id", "item_id"], kind="stable", ignore_index=True)
    ev["seq"] = range(len(ev))
    return ev


def history_end_seq(ev: pd.DataFrame) -> int:
    """Number of history events: seq < this is before validation_end."""
    return int((ev.timestamp < load_split().validation_end).sum())


def to_contract(row) -> InteractionEvent:
    return InteractionEvent(seq=int(row.seq), user_id=int(row.user_id), item_id=int(row.item_id),
                            event_type=str(row.event_type), weight=float(row.weight),
                            timestamp=row.timestamp.to_pydatetime())
