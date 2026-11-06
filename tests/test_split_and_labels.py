from datetime import datetime, timedelta, timezone

import pandas as pd

from recsys.config import load_split
from recsys.features.batch import snapshot_dates
from recsys.training import labels

UTC = timezone.utc


def test_split_cutoffs_on_snapshot_grid():
    s = load_split()
    assert s.train_end < s.validation_end
    assert (s.validation_end - s.train_end).days % s.snapshot_cadence_days == 0
    assert s.ranker_window_days % s.snapshot_cadence_days == 0


def test_snapshot_grid_ends_at_cutoff_and_is_weekly():
    end = datetime(2000, 12, 29, tzinfo=UTC)
    dates = snapshot_dates(datetime(2000, 11, 1, tzinfo=UTC), end, 7)
    assert dates[-1] == end
    assert all(b - a == timedelta(days=7) for a, b in zip(dates, dates[1:]))
    assert dates[0] > datetime(2000, 11, 1, tzinfo=UTC)


def test_anchors_inside_window():
    end = datetime(2000, 12, 1, tzinfo=UTC)
    a = labels.anchors(end, 28, 7)
    assert a == [end - timedelta(days=28 - 7 * i) for i in range(4)]


def _events(rows):
    return pd.DataFrame(rows, columns=["user_id", "item_id", "weight", "timestamp"]).assign(
        timestamp=lambda d: pd.to_datetime(d.timestamp, utc=True)
    )


def test_labels_use_only_future_unseen_items():
    t = datetime(2000, 1, 10, tzinfo=UTC)
    rows = [(u, i, 5.0, "2000-01-01") for u in range(1, 6) for i in range(1, 6) if (u + i) % 2]
    rows += [(1, 2, 5.0, "2000-01-12"), (1, 1, 5.0, "2000-01-12")]  # item 1 was not seen by user 1 before t
    ev = _events(rows)
    out, stats, _ = labels.build_rows(ev, t, t + timedelta(days=7), k=10, seed=0)
    pos = set(out[out.label == 1].item_id)
    seen_before = set(ev[(ev.user_id == 1) & (ev.timestamp < t)].item_id)
    assert pos and not (pos & seen_before)
    assert (out.event_timestamp == pd.Timestamp(t)).all()
