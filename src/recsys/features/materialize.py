"""Materialize Feast features into Redis as of the serving "now" (validation_end)."""

from __future__ import annotations

import logging
from datetime import timedelta

from recsys.config import load_split
from recsys.feature_repo.store import get_store

log = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    split = load_split()
    end = split.validation_end
    start = end - timedelta(days=30)
    store = get_store()
    store.materialize(start_date=start, end_date=end + timedelta(seconds=1))
    log.info("materialized features as of %s into Redis", end.isoformat())


if __name__ == "__main__":
    main()
