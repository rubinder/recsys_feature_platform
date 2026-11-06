"""Bounded backfill: run the streaming logic over history to build the offline table.

Writes data/offline/user_stream/part-backfill.parquet. The directory is the
batch source behind Feast's `user_stream_push` push source; the Flink job adds
append-only part files for replayed events. Rebuilding removes every other part,
which resets the table to validation_end.
"""

from __future__ import annotations

import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from recsys.config import OFFLINE, RAW, load_split
from recsys.streaming.logic import UserStreamState
from recsys.streaming.schema import ARROW_SCHEMA

log = logging.getLogger(__name__)
STREAM_DIR = OFFLINE / "user_stream"



def ordered_events(events: pd.DataFrame) -> pd.DataFrame:
    """The one global event order used by the producer, Flink and the backfill."""
    return events.sort_values(["timestamp", "user_id", "item_id"], kind="stable", ignore_index=True)


def compute(events: pd.DataFrame) -> tuple[list[dict], dict[int, UserStreamState]]:
    states: dict[int, UserStreamState] = {}
    rows = []
    # Unit-independent epoch seconds (pandas may store us or ns resolution).
    ts = ((events.timestamp - pd.Timestamp(0, tz="UTC")) / pd.Timedelta(seconds=1)).to_numpy()
    for u, i, t in zip(events.user_id.to_numpy(), events.item_id.to_numpy(), ts):
        st = states.setdefault(int(u), UserStreamState())
        rows.append(st.update(int(u), int(i), float(t)))
    return rows, states


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    split = load_split()
    ev = pd.read_parquet(RAW / "events.parquet")
    ev["timestamp"] = pd.to_datetime(ev.timestamp, utc=True)
    hist = ordered_events(ev[ev.timestamp < split.validation_end])
    rows, states = compute(hist)
    table = pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)
    STREAM_DIR.mkdir(parents=True, exist_ok=True)
    for old in STREAM_DIR.glob("*"):
        old.unlink()
    pq.write_table(table, STREAM_DIR / "part-backfill.parquet")
    log.info("backfilled %d streaming feature rows for %d users", table.num_rows, len(states))


if __name__ == "__main__":
    main()
