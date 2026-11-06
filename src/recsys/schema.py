"""Generic schema: events, items, users. Dataset-agnostic.

Any dataset is normalized into these three tables. MovieLens maps a rating to
event_type="rate", weight=rating; a music dataset would map a listen to
event_type="play", weight=play count or listen fraction.
"""

from __future__ import annotations

import pyarrow as pa

EVENTS = pa.schema(
    [
        ("user_id", pa.int64()),
        ("item_id", pa.int64()),
        ("event_type", pa.string()),
        ("weight", pa.float64()),
        ("timestamp", pa.timestamp("us", tz="UTC")),
    ]
)

ITEMS = pa.schema(
    [
        ("item_id", pa.int64()),
        ("title", pa.string()),
        ("genres", pa.list_(pa.string())),
    ]
)

USERS = pa.schema(
    [
        ("user_id", pa.int64()),
        ("attributes", pa.map_(pa.string(), pa.string())),
    ]
)


def validate(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Cast to the contract schema; raises if columns are missing or uncastable."""
    missing = set(schema.names) - set(table.column_names)
    if missing:
        raise ValueError(f"missing columns: {sorted(missing)}")
    return table.select(schema.names).cast(schema)
