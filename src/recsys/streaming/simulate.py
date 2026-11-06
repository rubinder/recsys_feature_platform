"""Deterministic traffic simulation over the replayed test period (lockstep with Flink).

For each test-period event, in global order:
  1. if it opens a session (gap > 30 min) or is every Nth event of a session,
     it is a *request point*: wait until Flink has processed every earlier event
     (progress marker), then ask the API for recommendations for that user, once
     per variant, with as_of = the event's time. The API logs each impression.
  2. publish the event to Kafka (Flink updates features) and log it as an
     interaction (validated contract).

Because each request waits for the stream to catch up, results do not depend on
timing. Clicks are attributed afterwards by `recsys.eval.report` (simulated).
"""

from __future__ import annotations

import argparse
import logging
import time

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from recsys.config import load_split
from recsys.contracts import InteractionEvent
from recsys.serving.impressions import LOGS, PartitionedLogWriter
from recsys.streaming import kafka_io, progress
from recsys.streaming.events import history_end_seq, ordered_events, to_contract
from recsys.streaming.logic import SESSION_GAP_S
from recsys.streaming.producer import publish_history

log = logging.getLogger(__name__)


def request_points(ev: pd.DataFrame, h: int, end: pd.Timestamp, refresh_every: int) -> pd.DataFrame:
    """Replay-window events flagged as request points (session start or every Nth in session)."""
    prev = ev.groupby("user_id").timestamp.shift(1)
    gap = (ev.timestamp - prev) / pd.Timedelta(seconds=1)
    new_session = gap.isna() | (gap > SESSION_GAP_S)
    session_id = new_session.groupby(ev.user_id).cumsum()
    pos = ev.groupby([ev.user_id, session_id]).cumcount()  # 0-based position in session
    window = (ev.seq >= h) & (ev.timestamp < end)
    out = ev[window].copy()
    out["is_request"] = (pos[window] % refresh_every == 0).to_numpy()
    out["session_pos"] = pos[window].to_numpy()
    return out


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=float, default=28)
    ap.add_argument("--variants", default="batch_session,streaming_session")
    ap.add_argument("--refresh-every", type=int, default=10, help="request at session start and every N events")
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--skip-history", action="store_true")
    args = ap.parse_args()
    variants = [v for v in args.variants.split(",") if v]

    split = load_split()
    ev = ordered_events()
    h = history_end_seq(ev)
    window = request_points(ev, h, pd.Timestamp(split.validation_end) + pd.Timedelta(days=args.days), args.refresh_every)
    r = progress.client()
    p = kafka_io.producer()
    api = httpx.Client(base_url=args.api, timeout=30)
    health = api.get("/health").json()
    missing = set(variants) - set(health["variants"])
    if missing:
        raise SystemExit(f"API does not serve variants {missing}; serving {health['variants']}")

    if not args.skip_history:
        publish_history(p)
    t0 = time.monotonic()
    waited = progress.wait_for(r, h - 1, timeout_s=1800, poll_s=0.2)
    log.info("flink state bootstrapped through seq %d in %.1fs", h - 1, waited)

    interactions = PartitionedLogWriter(LOGS / "interactions", InteractionEvent, flush_every=5000)
    requests_log: list[dict] = []
    n_req = 0
    wait_total = 0.0
    t1 = time.monotonic()
    for row in window.itertuples(index=False):
        if row.is_request:
            p.flush()
            wait_total += progress.wait_for(r, int(row.seq) - 1)
            for v in variants:
                resp = api.get(f"/recommendations/{int(row.user_id)}",
                               params={"variant": v, "as_of": row.timestamp.isoformat(), "n": split.top_n})
                resp.raise_for_status()
                requests_log.append({"request_id": resp.json()["request_id"], "trigger_seq": int(row.seq),
                                     "variant": v, "session_pos": int(row.session_pos)})
                n_req += 1
        e = to_contract(row)
        kafka_io.send(p, kafka_io.TOPIC, e.model_dump_json().encode())
        interactions.write(e.model_dump())
    p.flush()
    progress.wait_for(r, int(window.seq.max()))
    interactions.flush()
    flushed = api.post("/logs/flush").json()
    (LOGS / "requests").mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(requests_log), LOGS / "requests" / "requests.parquet")
    log.info("simulated %d events, %d request points x %d variants = %d requests in %.1fs "
             "(waiting on the stream %.1fs); impressions logged %s",
             len(window), int(window.is_request.sum()), len(variants), n_req, time.monotonic() - t1, wait_total, flushed)
    log.info("total wall time %.1fs", time.monotonic() - t0)


if __name__ == "__main__":
    main()
