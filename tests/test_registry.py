"""Registry flow against a throwaway MLflow store: register, promote, rollback, resolve."""

import mlflow
import pytest


@pytest.fixture()
def reg(tmp_path, monkeypatch):
    monkeypatch.setenv("MLFLOW_TRACKING_URI", f"sqlite:///{tmp_path / 'mlflow.db'}")
    from recsys import registry, tracking

    monkeypatch.setattr(tracking, "experiment", lambda: ("test", (tmp_path / "artifacts").as_uri()))
    return registry


def _run() -> str:
    from recsys import tracking

    tracking.setup()
    with mlflow.start_run() as r:
        mlflow.log_param("x", 1)
    return r.info.run_id


def test_promote_rollback_and_resolve(reg):
    r1, r2 = _run(), _run()
    v1, v2 = reg.register(r1, {"variant": "a"}), reg.register(r2, {"variant": "b"})
    assert reg.register(r1) == v1  # idempotent per run
    assert reg.promote(v1, "initial") is None
    reg.set_alias("challenger", v2)
    assert reg.resolve(reg.PREFIX + "challenger") == (r2, v2)
    assert reg.promote(v2, "better") == v1
    assert reg.resolve(reg.PREFIX + "champion") == (r2, v2)
    assert reg.rollback("regression") == (v2, v1)
    assert reg.alias_version("champion") == v1
    stages = {v["version"]: v["stage"] for v in reg.versions()}
    assert stages[v2] == "rolled-back" and stages[v1] == "champion"
    assert reg.resolve(r1) == (r1, None)  # plain run ids pass through


def test_rollback_without_history_fails(reg):
    v = reg.register(_run())
    reg.promote(v, "only")
    with pytest.raises(RuntimeError):
        reg.rollback()
