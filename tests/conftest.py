import socket

import pytest

from recsys.config import OFFLINE


def _redis_up() -> bool:
    try:
        socket.create_connection(("localhost", 6379), timeout=0.5).close()
        return True
    except OSError:
        return False


requires_pipeline = pytest.mark.skipif(
    not (_redis_up() and (OFFLINE / "user_stats.parquet").exists()),
    reason="needs Redis and built features (run `make materialize` first)",
)


@pytest.fixture(scope="session")
def spark():
    from recsys.features.batch import spark_session

    s = spark_session()
    yield s
    s.stop()
