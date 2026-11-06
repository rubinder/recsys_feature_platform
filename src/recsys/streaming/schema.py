"""Arrow schema of the streaming feature table (backfill parts and live parts alike)."""

import pyarrow as pa

from recsys.streaming.logic import STREAM_COLUMNS

ARROW_SCHEMA = pa.schema(
    [
        ("user_id", pa.int64()),
        ("event_timestamp", pa.timestamp("us", tz="UTC")),
        ("stream_session_events", pa.int64()),
        ("stream_session_duration_s", pa.float64()),
        ("stream_last_items", pa.list_(pa.int64())),
        ("stream_count_1h", pa.int64()),
        ("stream_count_24h", pa.int64()),
        ("stream_gap_s", pa.float64()),
        ("stream_last_event_ts", pa.int64()),
    ]
)
assert ARROW_SCHEMA.names == STREAM_COLUMNS
