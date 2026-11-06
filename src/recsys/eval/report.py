"""`make metrics`: rec-quality report from the impression and interaction logs.

Outcomes are SIMULATED: no user saw these recommendations. A replayed
interaction counts as a click on an impression if the same user interacts with
a shown item at or after the request's triggering event (by seq) and within
`--window-hours` of event time. Users' real future behaviour is fixed, so this
is counterfactual replay evaluation: it measures how well each variant
anticipates what the user did next.
"""

from __future__ import annotations

import argparse
import json

import duckdb
import numpy as np
import pandas as pd

from recsys.config import ARTIFACTS, RAW, load_split
from recsys.eval.metrics import hit_rate_at_k, ndcg_at_k
from recsys.serving.impressions import LOGS

ATTRIBUTION_HOURS = 24


def load(window_hours: float) -> pd.DataFrame:
    """One row per impression with `relevant` = items the user went on to interact with."""
    c = duckdb.connect()
    c.sql("SET timezone='UTC'")
    imp = c.sql(f"SELECT * FROM read_parquet('{LOGS}/impressions/*/*.parquet', hive_partitioning=false)").df()
    req = c.sql(f"SELECT * FROM read_parquet('{LOGS}/requests/requests.parquet')").df()
    inter = c.sql(f"SELECT seq, user_id, item_id, timestamp FROM read_parquet('{LOGS}/interactions/*/*.parquet', hive_partitioning=false)").df()
    imp = imp.merge(req[["request_id", "trigger_seq", "session_pos"]], on="request_id", how="inner")
    inter = inter.sort_values("seq")
    by_user = {u: g for u, g in inter.groupby("user_id")}
    horizon = pd.Timedelta(hours=window_hours)
    relevant = []
    for row in imp.itertuples(index=False):
        g = by_user.get(row.user_id)
        if g is None:
            relevant.append(set())
            continue
        m = (g.seq.to_numpy() >= row.trigger_seq) & (g.timestamp <= row.timestamp + horizon).to_numpy()
        relevant.append(set(g.item_id.to_numpy()[m].tolist()))
    imp["relevant"] = relevant
    return imp


def summarize(imp: pd.DataFrame, catalog_size: int, ks=(5, 10)) -> pd.DataFrame:
    rows = []
    for variant, g in imp.groupby("variant", sort=True):
        clicks = [len(set(items) & rel) for items, rel in zip(g["items"], g["relevant"])]
        shown = sum(len(i) for i in g["items"])
        r = {"variant": variant, "impressions": len(g), "users": g.user_id.nunique(),
             "ctr": float(np.mean([c > 0 for c in clicks])), "item_ctr": sum(clicks) / shown if shown else 0.0}
        for k in ks:
            r[f"hit_rate@{k}"] = float(np.mean([hit_rate_at_k(list(i), rel, k) for i, rel in zip(g["items"], g["relevant"])]))
            r[f"ndcg@{k}"] = float(np.mean([ndcg_at_k(list(i), rel, k) for i, rel in zip(g["items"], g["relevant"])]))
        r["coverage"] = len({i for items in g["items"] for i in items}) / catalog_size
        r["model_version"] = ",".join(sorted(g.model_version.unique()))
        rows.append(r)
    return pd.DataFrame(rows).set_index("variant")


def main() -> None:
    if not any((LOGS / "impressions").glob("*/*.parquet")):
        raise SystemExit("no impression logs yet: run `make simulate` (or `make demo-streaming`) first")
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-hours", type=float, default=ATTRIBUTION_HOURS)
    ap.add_argument("--baseline", default="batch_session")
    args = ap.parse_args()
    imp = load(args.window_hours)
    ev = pd.read_parquet(RAW / "events.parquet", columns=["item_id", "timestamp"])
    catalog = int(ev[pd.to_datetime(ev.timestamp, utc=True) < load_split().validation_end].item_id.nunique())
    s = summarize(imp, catalog)
    metrics = [c for c in s.columns if c not in ("impressions", "users", "model_version")]

    print("Recommendation quality from impression + interaction logs")
    print(f"SIMULATED outcomes: a click = the user interacted with a shown item within {args.window_hours:g}h")
    print(f"(event time) at or after the request. Counterfactual replay, not live feedback.\n")
    print(f"impressions: {len(imp)}  users: {imp.user_id.nunique()}  "
          f"period: {imp.timestamp.min():%Y-%m-%d} .. {imp.timestamp.max():%Y-%m-%d}  catalog: {catalog} items\n")
    print(s[["impressions"] + metrics].to_string(float_format=lambda x: f"{x:.4f}"))
    out = {"window_hours": args.window_hours, "catalog": catalog, "variants": s.reset_index().to_dict("records")}
    if args.baseline in s.index and len(s) > 1:
        print(f"\nDelta vs {args.baseline} (point estimates; confidence intervals come in Phase 4):")
        base = s.loc[args.baseline, metrics]
        for v in s.index.drop(args.baseline):
            d = s.loc[v, metrics] - base
            rel = d / base.replace(0, np.nan)
            print(f"  {v}: " + ", ".join(f"{m} {d[m]:+.4f} ({rel[m]:+.1%})" for m in metrics))
            out.setdefault("deltas", {})[v] = {m: float(d[m]) for m in metrics}
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    (ARTIFACTS / "metrics_report.json").write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
