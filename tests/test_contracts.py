from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from recsys.contracts import Impression, InteractionEvent

GOOD = {"seq": 1, "user_id": 2, "item_id": 3, "event_type": "rate", "weight": 4.0,
        "timestamp": "2001-01-01T00:00:00+00:00"}


def test_valid_event_roundtrips_json():
    e = InteractionEvent.model_validate(GOOD)
    assert InteractionEvent.model_validate_json(e.model_dump_json()) == e


@pytest.mark.parametrize("bad", [
    {"user_id": -1}, {"item_id": 0}, {"timestamp": "2001-01-01T00:00:00"}, {"weight": float("nan")},
    {"event_type": ""}, {"extra": 1}, {"seq": -5},
])
def test_malformed_events_rejected(bad):
    with pytest.raises(ValidationError):
        InteractionEvent.model_validate({**GOOD, **bad})


def _imp(**kw):
    base = dict(request_id="r" * 12, user_id=1, items=[1, 2], scores=[0.5, 0.1], ranks=[1, 2], model_version="m",
                strategy="two_stage", timestamp=datetime(2001, 1, 1, tzinfo=timezone.utc),
                served_at=datetime.now(timezone.utc))
    return Impression.model_validate({**base, **kw})


def test_impression_defaults_variant_and_validates_alignment():
    assert _imp().variant == "control" and _imp().experiment_id is None
    for bad in ({"scores": [0.5]}, {"items": [1, 1]}, {"ranks": [2, 1]}, {"items": []}):
        with pytest.raises(ValidationError):
            _imp(**bad)
