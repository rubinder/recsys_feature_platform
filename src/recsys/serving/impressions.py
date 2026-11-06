"""Impression log: validated against the data contract, written as date-partitioned Parquet."""

from __future__ import annotations

import logging
import threading
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from recsys.config import DATA
from recsys.contracts import Impression

log = logging.getLogger(__name__)
LOGS = DATA / "logs"


class PartitionedLogWriter:
    """Buffers validated records and flushes them to <root>/date=YYYY-MM-DD/part-*.parquet."""

    def __init__(self, root: Path, model: type, flush_every: int = 500) -> None:
        self.root, self.model, self.flush_every = root, model, flush_every
        self._buf: list[dict] = []
        self._lock = threading.Lock()
        self.accepted = 0
        self.rejected = 0

    def write(self, record: dict) -> bool:
        try:
            row = self.model.model_validate(record).model_dump()
        except ValidationError as exc:
            self.rejected += 1
            log.warning("rejected %s record: %s", self.model.__name__, exc.errors()[0]["msg"])
            return False
        with self._lock:
            self._buf.append(row)
            self.accepted += 1
            full = len(self._buf) >= self.flush_every
        if full:
            self.flush()
        return True

    def flush(self) -> int:
        with self._lock:
            rows, self._buf = self._buf, []
        if not rows:
            return 0
        by_date: dict[str, list[dict]] = {}
        for r in rows:
            by_date.setdefault(r["timestamp"].date().isoformat(), []).append(r)
        for day, part in by_date.items():
            d = self.root / f"date={day}"
            d.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(part), d / f"part-{uuid.uuid4().hex}.parquet")
        return len(rows)


def impression_writer() -> PartitionedLogWriter:
    return PartitionedLogWriter(LOGS / "impressions", Impression)
