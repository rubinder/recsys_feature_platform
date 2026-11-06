from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from recsys.streaming.backfill import compute
from recsys.streaming.logic import LAST_N, SESSION_GAP_S, UserStreamState

T0 = datetime(2001, 1, 1, tzinfo=timezone.utc).timestamp()


def test_sessions_rolling_counts_and_gap():
    s = UserStreamState()
    r1 = s.update(1, 10, T0)
    assert r1["stream_session_events"] == 1 and r1["stream_gap_s"] is None
    r2 = s.update(1, 11, T0 + 60)
    assert r2["stream_session_events"] == 2 and r2["stream_gap_s"] == 60 and r2["stream_session_duration_s"] == 60
    r3 = s.update(1, 12, T0 + 2 * 3600)  # gap > SESSION_GAP_S: new session; earlier events > 1h ago
    assert r3["stream_session_events"] == 1 and r3["stream_count_1h"] == 1 and r3["stream_count_24h"] == 3
    r4 = s.update(1, 13, T0 + 86400 + 61)  # first two events fall out of the 24h window
    assert r4["stream_count_24h"] == 2


def test_last_items_most_recent_first_deduped_and_capped():
    s = UserStreamState()
    for i in range(LAST_N + 3):
        s.update(1, i, T0 + i)
    r = s.update(1, 5, T0 + 100)
    assert r["stream_last_items"][0] == 5 and len(r["stream_last_items"]) == LAST_N
    assert len(set(r["stream_last_items"])) == LAST_N


def test_same_second_events_get_strictly_increasing_timestamps():
    """Feast's Redis store skips writes with an equal timestamp; offsets keep every update."""
    s = UserStreamState()
    ts = [s.update(1, i, T0)["event_timestamp"] for i in range(3)]
    assert ts[0] < ts[1] < ts[2] and ts[2] - ts[0] == timedelta(microseconds=2)
    assert s.update(1, 9, T0 + 1)["event_timestamp"] == datetime.fromtimestamp(T0 + 1, tz=timezone.utc)


def test_out_of_order_raises():
    s = UserStreamState()
    s.update(1, 1, T0 + 10)
    with pytest.raises(ValueError):
        s.update(1, 2, T0)


def test_state_roundtrip():
    s = UserStreamState()
    s.update(1, 1, T0)
    s2 = UserStreamState.from_dict(s.to_dict())
    assert s2.update(1, 2, T0 + 5) == s.update(1, 2, T0 + 5)


@pytest.mark.parametrize("unit", ["us", "ns"])
def test_backfill_timestamps_match_event_time_regardless_of_pandas_unit(unit):
    """Regression: dividing int64 microseconds by 1e9 once put every row in 1970."""
    when = pd.Timestamp("2000-11-20 12:34:56", tz="UTC")
    ev = pd.DataFrame({"user_id": [7], "item_id": [3], "timestamp": pd.Series([when]).astype(f"datetime64[{unit}, UTC]")})
    rows, _ = compute(ev)
    assert rows[0]["event_timestamp"] == when.to_pydatetime()
    assert rows[0]["stream_last_event_ts"] == int(when.timestamp())
