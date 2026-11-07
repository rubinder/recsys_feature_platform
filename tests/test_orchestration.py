"""Dagster wiring: definitions load, and a failing blocking check stops downstream assets."""

import dagster as dg
import pytest

from recsys.config import RAW
from recsys.orchestration import assets as A
from recsys.orchestration import checks as C
from recsys.quality.checks import CheckOutcome


def test_definitions_load_with_jobs_schedules_sensors():
    from recsys.orchestration.definitions import defs

    repo = defs.get_repository_def()
    assert {"batch_pipeline", "freshness_checks", "measurement", "retrain_and_promote"} <= set(repo.job_names)
    assert {s.name for s in repo.schedule_defs} == {"weekly_batch", "hourly_freshness"}
    assert {s.name for s in repo.sensor_defs} == {"new_raw_data", "logs_changed", "fresh_features_retrain"}
    keys = {k.to_user_string() for k in repo.asset_graph.get_all_asset_keys()}
    assert {"raw_events", "batch_features", "online_store", "ranker_streaming_session", "rec_quality_report"} <= keys


def test_every_asset_has_a_retry_policy():
    for a in [A.raw_events, A.batch_features, A.streaming_backfill, A.feast_registry, A.online_store, *A.RANKERS]:
        assert a.op.retry_policy is not None, a.key


@pytest.mark.skipif(not (RAW / "ml-1m.zip").exists(), reason="needs the cached dataset (make data)")
def test_failing_blocking_check_skips_downstream(monkeypatch):
    def bad():
        return [CheckOutcome(n, n != "raw_events.schema", metadata={"injected": True})
                for n in C.DQ[A.raw_events][1]]

    monkeypatch.setitem(C.DQ, A.raw_events, (bad, C.DQ[A.raw_events][1]))
    raw_check = C._dq_checks(A.raw_events, bad, C.DQ[A.raw_events][1])
    ran = []

    @dg.asset(deps=[A.raw_events])
    def downstream():
        ran.append(True)

    result = dg.materialize([A.raw_events, downstream, raw_check], raise_on_error=False)
    evals = {e.check_name: e.passed for e in result.get_asset_check_evaluations()}
    assert evals["raw_events__schema"] is False
    assert not ran, "downstream ran despite a failing blocking check"
    assert not result.success
