"""Data quality checks, usable standalone (`make quality`) and as Dagster asset checks.

Each check returns a CheckOutcome: pass/fail, severity, and metadata (counts,
rates, a sample of failing rows). Column-level rules are Pandera schemas
(validated lazily so every violation is reported at once); cross-table rules
(referential integrity, grid alignment, per-user monotonicity) are pandas.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import timedelta

import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors

from recsys.config import OFFLINE, RAW, load_split
from recsys.streaming.logic import LAST_N


@dataclass
class CheckOutcome:
    name: str
    passed: bool
    severity: str = "ERROR"  # ERROR blocks downstream assets; WARN does not
    metadata: dict = field(default_factory=dict)

    def summary(self) -> str:
        status = "PASS" if self.passed else f"FAIL ({self.severity})"
        detail = ", ".join(f"{k}={v}" for k, v in self.metadata.items() if not isinstance(v, (list, dict)))
        return f"[{status:<12}] {self.name}: {detail}"


def _pandera(name: str, schema: pa.DataFrameSchema, df: pd.DataFrame, extra: dict | None = None) -> CheckOutcome:
    try:
        schema.validate(df, lazy=True)
        return CheckOutcome(name, True, metadata={"rows": len(df), **(extra or {})})
    except SchemaErrors as err:
        fc = err.failure_cases
        return CheckOutcome(name, False, metadata={
            "rows": len(df), "violations": len(fc), **(extra or {}),
            "sample": fc.head(10).astype(str).to_dict("records"),
        })


def _load(name: str) -> pd.DataFrame:
    path = OFFLINE / name if name.endswith("/") else OFFLINE / f"{name}.parquet"
    df = pd.read_parquet(path)
    if "event_timestamp" in df:
        df["event_timestamp"] = pd.to_datetime(df.event_timestamp, utc=True)
    return df


# ---- raw ---------------------------------------------------------------------------------
EVENTS_SCHEMA = pa.DataFrameSchema(
    {
        "user_id": pa.Column(int, pa.Check.gt(0), nullable=False),
        "item_id": pa.Column(int, pa.Check.gt(0), nullable=False),
        "event_type": pa.Column(str, pa.Check.str_length(1, 32), nullable=False),
        "weight": pa.Column(float, [pa.Check.ge(0), pa.Check(lambda s: np.isfinite(s), element_wise=False)], nullable=False),
        "timestamp": pa.Column("datetime64[us, UTC]", [
            pa.Check.ge(pd.Timestamp("1990-01-01", tz="UTC")),
            pa.Check(lambda s: s <= pd.Timestamp.now(tz="UTC"), name="not_in_future"),
        ], nullable=False, coerce=True),
    },
    strict=True,
)


def raw_events() -> list[CheckOutcome]:
    ev = pd.read_parquet(RAW / "events.parquet")
    items = pd.read_parquet(RAW / "items.parquet", columns=["item_id"])
    users = pd.read_parquet(RAW / "users.parquet", columns=["user_id"])
    out = [_pandera("raw_events.schema", EVENTS_SCHEMA, ev)]
    unknown_items = ~ev.item_id.isin(items.item_id)
    unknown_users = ~ev.user_id.isin(users.user_id)
    out.append(CheckOutcome("raw_events.referential_integrity", not (unknown_items.any() or unknown_users.any()), metadata={
        "events_with_unknown_item": int(unknown_items.sum()), "events_with_unknown_user": int(unknown_users.sum())}))
    dup_rate = float(ev.duplicated(["user_id", "item_id", "timestamp"]).mean())
    out.append(CheckOutcome("raw_events.duplicate_rate", dup_rate <= 0.001, metadata={"duplicate_rate": round(dup_rate, 6)}))
    return out


# ---- batch features ----------------------------------------------------------------------
def _nonneg(*cols: str) -> dict:
    return {c: pa.Column(float, pa.Check.ge(0), nullable=False, coerce=True) for c in cols}


USER_STATS_SCHEMA = pa.DataFrameSchema(
    {
        "user_id": pa.Column(int, pa.Check.gt(0)),
        "event_timestamp": pa.Column("datetime64[ns, UTC]", coerce=True),
        "user_interaction_count": pa.Column(int, pa.Check.ge(1)),
        "user_interaction_count_30d": pa.Column(int, pa.Check.ge(0)),
        **_nonneg("user_avg_weight", "user_days_since_last", "user_days_since_first"),
    },
    checks=[
        pa.Check(lambda d: d.user_interaction_count_30d <= d.user_interaction_count, name="count_30d<=count"),
        pa.Check(lambda d: d.user_days_since_last <= d.user_days_since_first + 1e-9, name="last<=first"),
    ],
    unique=["user_id", "event_timestamp"],
)
ITEM_POP_SCHEMA = pa.DataFrameSchema(
    {
        "item_id": pa.Column(int, pa.Check.gt(0)),
        "event_timestamp": pa.Column("datetime64[ns, UTC]", coerce=True),
        "item_interaction_count": pa.Column(int, pa.Check.ge(1)),
        "item_interaction_count_30d": pa.Column(int, pa.Check.ge(0)),
    },
    checks=[pa.Check(lambda d: d.item_interaction_count_30d <= d.item_interaction_count, name="count_30d<=count")],
    unique=["item_id", "event_timestamp"],
)
ITEM_STATS_SCHEMA = pa.DataFrameSchema(
    {
        "item_id": pa.Column(int, pa.Check.gt(0)),
        "event_timestamp": pa.Column("datetime64[ns, UTC]", coerce=True),
        **_nonneg("item_avg_weight", "item_days_since_last", "item_days_since_first"),
        "item_genre_vector": pa.Column(object, nullable=False),
    },
    unique=["item_id", "event_timestamp"],
)


def batch_features() -> list[CheckOutcome]:
    split = load_split()
    users = pd.read_parquet(RAW / "users.parquet", columns=["user_id"])
    items = pd.read_parquet(RAW / "items.parquet", columns=["item_id"])
    ev_w = pd.read_parquet(RAW / "events.parquet", columns=["weight"]).weight
    genres = json.loads((OFFLINE / "genres.json").read_text())
    us, ga, ip, ist = _load("user_stats"), _load("user_genre_affinity"), _load("item_popularity"), _load("item_stats")

    out = [
        _pandera("user_stats.schema", USER_STATS_SCHEMA, us),
        _pandera("item_popularity.schema", ITEM_POP_SCHEMA, ip),
        _pandera("item_stats.schema", ITEM_STATS_SCHEMA, ist),
    ]
    in_range = us.user_avg_weight.between(ev_w.min(), ev_w.max()).all() and ist.item_avg_weight.between(ev_w.min(), ev_w.max()).all()
    out.append(CheckOutcome("features.avg_weight_within_event_range", bool(in_range),
                            metadata={"event_weight_min": float(ev_w.min()), "event_weight_max": float(ev_w.max())}))
    shares = np.array(ga.user_genre_share.tolist(), dtype=float)
    onehot_len = ist.item_genre_vector.map(len)
    ok_vec = shares.shape[1] == len(genres) and (onehot_len == len(genres)).all() and np.allclose(shares.sum(axis=1), 1, atol=1e-3)
    out.append(CheckOutcome("features.genre_vectors", bool(ok_vec), metadata={
        "genres": len(genres), "share_sum_max_error": float(np.abs(shares.sum(axis=1) - 1).max())}))
    rates = {name: float(df.isna().mean().max()) for name, df in
             (("user_stats", us), ("user_genre_affinity", ga), ("item_popularity", ip), ("item_stats", ist))}
    out.append(CheckOutcome("features.null_rate", max(rates.values()) == 0.0,
                            metadata={f"max_null_rate_{k}": v for k, v in rates.items()}))

    unknown_u = int((~us.user_id.isin(users.user_id)).sum() + (~ga.user_id.isin(users.user_id)).sum())
    unknown_i = int((~ip.item_id.isin(items.item_id)).sum() + (~ist.item_id.isin(items.item_id)).sum())
    out.append(CheckOutcome("features.referential_integrity", unknown_u == 0 and unknown_i == 0,
                            metadata={"feature_rows_unknown_user": unknown_u, "feature_rows_unknown_item": unknown_i}))

    grid_days = ((pd.Timestamp(split.validation_end) - us.event_timestamp) / pd.Timedelta(days=1))
    off_grid = int(((grid_days % split.snapshot_cadence_days) != 0).sum())
    out.append(CheckOutcome("features.snapshot_grid", off_grid == 0 and us.event_timestamp.max() == pd.Timestamp(split.validation_end),
                            metadata={"rows_off_grid": off_grid, "latest_snapshot": str(us.event_timestamp.max())}))
    return out


# ---- streaming backfill / offline parts ---------------------------------------------------
STREAM_SCHEMA = pa.DataFrameSchema(
    {
        "user_id": pa.Column(int, pa.Check.gt(0)),
        "event_timestamp": pa.Column("datetime64[ns, UTC]", coerce=True),
        "stream_session_events": pa.Column(int, pa.Check.ge(1)),
        "stream_session_duration_s": pa.Column(float, pa.Check.ge(0)),
        "stream_last_items": pa.Column(object, pa.Check(lambda s: s.map(len).between(1, LAST_N), name=f"1<=len<={LAST_N}")),
        "stream_count_1h": pa.Column(int, pa.Check.ge(1)),
        "stream_count_24h": pa.Column(int, pa.Check.ge(1)),
        "stream_gap_s": pa.Column(float, pa.Check.ge(0), nullable=True),
        "stream_last_event_ts": pa.Column(int, pa.Check.gt(0)),
    },
    checks=[
        pa.Check(lambda d: d.stream_count_1h <= d.stream_count_24h, name="count_1h<=count_24h"),
        pa.Check(lambda d: (d.stream_last_event_ts - d.event_timestamp.astype("int64") // 10**9).abs() <= 1,
                 name="last_event_ts_matches_event_timestamp"),
    ],
)


def streaming_features() -> list[CheckOutcome]:
    st = pd.read_parquet(OFFLINE / "user_stream")
    st["event_timestamp"] = pd.to_datetime(st.event_timestamp, utc=True)
    ev = pd.read_parquet(RAW / "events.parquet", columns=["timestamp"])
    lo, hi = pd.to_datetime(ev.timestamp, utc=True).min(), pd.to_datetime(ev.timestamp, utc=True).max() + timedelta(seconds=1)
    out = [_pandera("user_stream.schema", STREAM_SCHEMA, st)]
    outside = int((~st.event_timestamp.between(lo, hi)).sum())
    out.append(CheckOutcome("user_stream.timestamps_within_data_range", outside == 0, metadata={
        "rows_outside": outside, "min": str(st.event_timestamp.min()), "max": str(st.event_timestamp.max()),
        "data_min": str(lo), "data_max": str(hi)}))
    st = st.sort_values(["user_id", "event_timestamp"], kind="stable")
    dup = int(st.duplicated(["user_id", "event_timestamp"]).sum())
    out.append(CheckOutcome("user_stream.unique_timestamp_per_user", dup == 0, metadata={"duplicate_user_timestamps": dup}))
    return out


ALL = {"raw_events": raw_events, "batch_features": batch_features, "streaming_features": streaming_features}
