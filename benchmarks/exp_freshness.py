"""Experiment 1: feature freshness, batch-only vs batch + streaming features.

Knob: whether the ranker sees streaming features. Both rankers are trained on
the same mid-session anchors; both are served at every request point of the
same deterministic 28-day replay (counterfactual, paired). Outcomes are
SIMULATED (Phase 2 attribution rule). Also measures feature staleness at
request time. Needs the streaming stack (`make stream-up`) and the API.
"""

from __future__ import annotations

import subprocess
import sys

import numpy as np
import pandas as pd

from benchmarks.common import coverage_fn, mlflow_run, save
from benchmarks.compare import paired_diff, statistic_diff
from recsys.config import RAW, load_split
from recsys.eval import report
from recsys.eval.metrics import hit_rate_at_k, ndcg_at_k
from recsys.streaming.events import history_end_seq, ordered_events

A, B = "batch_session", "streaming_session"
DAYS = 28


def replay() -> None:
    py = sys.executable
    for args in (["-m", "recsys.streaming.reset"], ["-m", "recsys.streaming.flink_ctl", "submit"],
                 ["-m", "recsys.streaming.simulate", "--days", str(DAYS), "--variants", f"{A},{B}"]):
        subprocess.run([py, *args], check=True)


def staleness(imp: pd.DataFrame) -> dict:
    """Information a user's features are missing at request time.

    Batch features stop at the last weekly snapshot (the serving cutoff here), so
    every user event between the snapshot and the request is unreflected.
    Streaming features include every earlier event: the simulator waits for the
    stream before each request, so their unreflected count is 0 by construction
    (in free-running mode the lag is the ~100 ms pipeline latency).
    """
    split = load_split()
    ev = ordered_events()
    h = history_end_seq(ev)
    replayed = ev[ev.seq >= h][["user_id", "seq", "timestamp"]]
    by_user = {u: g for u, g in replayed.groupby("user_id")}
    snap = pd.Timestamp(split.validation_end)
    counts, lags, ages = [], [], []
    for row in imp[imp.variant == A].itertuples(index=False):
        g = by_user.get(row.user_id)
        before = g[g.seq < row.trigger_seq] if g is not None else None
        n = 0 if before is None else len(before)
        counts.append(n)
        lags.append(0.0 if n == 0 else (before.timestamp.max() - snap).total_seconds() / 3600)
        ages.append((row.timestamp - snap).total_seconds() / 86400)
    return {"batch_unreflected_events_mean": float(np.mean(counts)),
            "batch_requests_with_unreflected_events": float(np.mean(np.array(counts) > 0)),
            "batch_information_lag_hours_mean": float(np.mean(lags)),
            "batch_feature_age_days_mean": float(np.mean(ages)),
            "streaming_unreflected_events_mean": 0.0, "streaming_information_lag_hours_mean": 0.0}


def run(skip_replay: bool = False) -> dict:
    if not skip_replay:
        replay()
    imp = report.load(report.ATTRIBUTION_HOURS)
    ev = pd.read_parquet(RAW / "events.parquet", columns=["item_id", "timestamp"])
    catalog = int(ev[pd.to_datetime(ev.timestamp, utc=True) < load_split().validation_end].item_id.nunique())

    sides = {}
    for v in (A, B):
        g = imp[imp.variant == v].sort_values(["trigger_seq", "user_id"]).reset_index(drop=True)
        g["click"] = [float(len(set(i) & r) > 0) for i, r in zip(g["items"], g["relevant"])]
        g["hit@5"] = [hit_rate_at_k(list(i), r, 5) for i, r in zip(g["items"], g["relevant"])]
        g["ndcg@10"] = [ndcg_at_k(list(i), r, 10) for i, r in zip(g["items"], g["relevant"])]
        sides[v] = g
    a, b = sides[A], sides[B]
    assert (a.trigger_seq.to_numpy() == b.trigger_seq.to_numpy()).all(), "variants must be paired by request point"
    deltas = [paired_diff(m, a[m], b[m]).as_dict() for m in ("click", "hit@5", "ndcg@10")]
    cov = statistic_diff("coverage", coverage_fn([list(x) for x in a["items"]], [list(x) for x in b["items"]], catalog),
                         len(a), n_boot=300)
    deltas.append(cov.as_dict())
    out = {"request_points": int(len(a)), "users": int(a.user_id.nunique()), "days": DAYS, "catalog": catalog,
           "outcomes": "SIMULATED (counterfactual replay, 24h attribution)", "deltas": deltas,
           "staleness": staleness(imp)}
    for v, g in sides.items():
        mlflow_run("freshness", v, {"variant": v, "days": DAYS},
                   {"ctr": g.click.mean(), "hit_at_5": g["hit@5"].mean(), "ndcg_at_10": g["ndcg@10"].mean()})
    save("freshness", out)
    return out


if __name__ == "__main__":
    import json

    print(json.dumps(run("--skip-replay" in sys.argv), indent=2, default=float))
