"""Replay interactions into Kafka as a time-ordered stream.

    python -m recsys.streaming.producer --history            # bulk: all events before validation_end
    python -m recsys.streaming.producer --days 7 --speedup 86400 --malformed 5

--history bootstraps Flink state (no pushes happen for these). The replay then
publishes test-period events with their original timestamps, paced so that
`speedup` seconds of event time pass per wall second (86400 = one day per second).
--malformed injects deterministic contract violations to exercise the DLQ.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import time

from recsys.config import load_split
from recsys.streaming import kafka_io
from recsys.streaming.events import history_end_seq, ordered_events, to_contract

log = logging.getLogger(__name__)

MALFORMED = [
    lambda e: "{not json",
    lambda e: json.dumps({**e, "user_id": -1}),
    lambda e: json.dumps({k: v for k, v in e.items() if k != "item_id"}),
    lambda e: json.dumps({**e, "timestamp": e["timestamp"].replace("Z", "").replace("+00:00", "")}),  # naive timestamp
    lambda e: json.dumps({**e, "weight": "NaN"}),
    lambda e: json.dumps({**e, "extra_field": 1}),
]


def publish_history(p=None) -> int:
    p = p or kafka_io.producer()
    ev = ordered_events()
    h = history_end_seq(ev)
    for row in ev.iloc[:h].itertuples(index=False):
        kafka_io.send(p, kafka_io.TOPIC, to_contract(row).model_dump_json().encode())
    p.flush()
    log.info("published %d history events (seq 0..%d)", h, h - 1)
    return h


def replay(days: float, speedup: float, malformed: int, seed: int) -> None:
    p = kafka_io.producer()
    ev = ordered_events()
    h = history_end_seq(ev)
    start = load_split().validation_end
    window = ev.iloc[h:]
    window = window[window.timestamp < start + __import__("pandas").Timedelta(days=days)]
    rng = random.Random(seed)
    bad_at = set(rng.sample(range(len(window)), min(malformed, len(window))))
    t_wall0 = time.monotonic()
    for i, row in enumerate(window.itertuples(index=False)):
        due = (row.timestamp - start).total_seconds() / speedup
        delay = due - (time.monotonic() - t_wall0)
        if delay > 0:
            p.flush()
            time.sleep(delay)
        msg = to_contract(row).model_dump_json()
        kafka_io.send(p, kafka_io.TOPIC, msg.encode())
        if i in bad_at:
            kafka_io.send(p, kafka_io.TOPIC, MALFORMED[i % len(MALFORMED)](json.loads(msg)).encode())
    p.flush()
    log.info("replayed %d events over %.1f days of event time (+%d malformed)", len(window), days, len(bad_at))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", action="store_true")
    ap.add_argument("--days", type=float, default=7)
    ap.add_argument("--speedup", type=float, default=86400)
    ap.add_argument("--malformed", type=int, default=0)
    args = ap.parse_args()
    if args.history:
        publish_history()
    else:
        replay(args.days, args.speedup, args.malformed, load_split().seed)


if __name__ == "__main__":
    main()
