"""Reset the streaming path to the serving cutoff so a replay is reproducible.

Cancels the Flink job, recreates the Kafka topics, flushes Redis (online store +
progress marker), clears the event logs, rebuilds the offline streaming table from
the backfill (dropping rows written by earlier replays) and re-materializes Redis
as of validation_end.
"""

from __future__ import annotations

import logging
import shutil

import httpx

from recsys.features import materialize
from recsys.serving.impressions import LOGS
from recsys.streaming import backfill, flink_ctl, kafka_io, progress

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:  # drain the API's in-memory impression buffer first, so it cannot leak into the next run's logs
        httpx.post("http://localhost:8000/logs/flush", timeout=10)
    except httpx.HTTPError:
        pass
    flink_ctl.cancel_all()
    kafka_io.recreate_topics()
    # Flush Redis entirely: Feast skips online writes older than what is stored, so
    # re-materializing cannot overwrite values a previous replay (or a probe) wrote
    # with newer event timestamps. Everything is rebuilt by materialize below.
    progress.client().flushdb()
    shutil.rmtree(LOGS, ignore_errors=True)
    backfill.main()
    materialize.main()
    try:
        httpx.post("http://localhost:8000/cache/clear", timeout=5)
    except httpx.HTTPError:
        pass
    log.info("stream reset: topics recreated, offline + online stream features restored to validation_end")


if __name__ == "__main__":
    main()
