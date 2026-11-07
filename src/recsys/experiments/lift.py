"""A/B lift analysis from the impression + interaction logs (`make experiment`).

Builds on the Phase 4 comparison module (benchmarks.compare) and the Phase 2
metrics and attribution (recsys.eval). Users are the randomization unit, so
metrics are aggregated per user first and compared between buckets as
independent samples:

- per-metric deltas with bootstrap 95% CIs (independent_diff)
- significance: two-proportion z-test on impression CTR (reported with the
  caveat that impressions cluster within users) and a user-level permutation
  test for every metric (the primary test)
- sample-ratio mismatch: chi-square of users per bucket vs configured weights
- guardrails: coverage (relative change floor) and p99 latency (SLO ceiling)

Outcomes are SIMULATED (counterfactual replay, Phase 2 attribution rule).
"""

from __future__ import annotations

import argparse
import json

import numpy as np
import pandas as pd
from scipy import stats

from benchmarks.compare import independent_diff, statistic_diff
from recsys.config import ARTIFACTS, RAW, load_split
from recsys.eval import report
from recsys.eval.metrics import hit_rate_at_k, ndcg_at_k
from recsys.experiments.assignment import Experiment, load

METRICS = ("ctr", "hit@5", "ndcg@10")
N_PERM = 2000


def per_impression(imp: pd.DataFrame) -> pd.DataFrame:
    imp = imp.copy()
    imp["ctr"] = [float(len(set(i) & r) > 0) for i, r in zip(imp["items"], imp["relevant"])]
    imp["hit@5"] = [hit_rate_at_k(list(i), r, 5) for i, r in zip(imp["items"], imp["relevant"])]
    imp["ndcg@10"] = [ndcg_at_k(list(i), r, 10) for i, r in zip(imp["items"], imp["relevant"])]
    return imp


def permutation_p(a: np.ndarray, b: np.ndarray, n: int = N_PERM, seed: int = 42) -> float:
    """Two-sided p-value for mean(b) - mean(a) by permuting bucket labels over users."""
    rng = np.random.default_rng(seed)
    pooled, na = np.concatenate([a, b]), len(a)
    obs = b.mean() - a.mean()
    perm = np.empty(n)
    for i in range(n):
        x = rng.permutation(pooled)
        perm[i] = x[na:].mean() - x[:na].mean()
    return float((np.sum(np.abs(perm) >= abs(obs)) + 1) / (n + 1))


def two_proportion_z(clicks_a: int, n_a: int, clicks_b: int, n_b: int) -> tuple[float, float]:
    p = (clicks_a + clicks_b) / (n_a + n_b)
    se = np.sqrt(p * (1 - p) * (1 / n_a + 1 / n_b))
    z = (clicks_b / n_b - clicks_a / n_a) / se if se else 0.0
    return float(z), float(2 * stats.norm.sf(abs(z)))


def srm(users: dict[str, int], expected: dict[str, float]) -> dict:
    names = sorted(expected)
    obs = np.array([users.get(n, 0) for n in names], float)
    exp = np.array([expected[n] for n in names]) * obs.sum()
    chi2, p = stats.chisquare(obs, exp)
    return {"observed": dict(zip(names, obs.astype(int).tolist())), "expected_share": expected,
            "chi2": float(chi2), "p": float(p), "mismatch": bool(p < 0.001)}


def analyze(exp: Experiment, control: str = "control", treatment: str = "treatment") -> dict:
    imp = per_impression(report.load(report.ATTRIBUTION_HOURS))
    imp = imp[imp.experiment_id == exp.id]
    if imp.empty:
        raise SystemExit(f"no impressions for experiment {exp.id}: run `make experiment`")
    users = imp.groupby("variant").user_id.nunique().to_dict()
    leaked = imp.groupby("user_id").variant.nunique().gt(1).sum()  # a user must stay in one bucket
    ua = imp[imp.variant == control].groupby("user_id")[list(METRICS)].mean()
    ub = imp[imp.variant == treatment].groupby("user_id")[list(METRICS)].mean()
    ia, ib = imp[imp.variant == control], imp[imp.variant == treatment]

    results = {}
    for m in METRICS:
        d = independent_diff(m, ua[m].to_numpy(), ub[m].to_numpy())
        results[m] = {**d.as_dict(), "permutation_p": permutation_p(ua[m].to_numpy(), ub[m].to_numpy())}
    z, pz = two_proportion_z(int(ia.ctr.sum()), len(ia), int(ib.ctr.sum()), len(ib))
    results["ctr"]["impression_z"], results["ctr"]["impression_z_p"] = z, pz

    ev = pd.read_parquet(RAW / "events.parquet", columns=["item_id", "timestamp"])
    catalog = int(ev[pd.to_datetime(ev.timestamp, utc=True) < load_split().validation_end].item_id.nunique())
    items_a, items_b = [list(x) for x in ia["items"]], [list(x) for x in ib["items"]]
    cov = statistic_diff("coverage", lambda idx, side: len({i for j in idx for i in (items_a if side == "a" else items_b)[j]}) / catalog,
                         len(items_a), len(items_b), paired=False, n_boot=300)
    lat_a, lat_b = ia.latency_ms.to_numpy(float), ib.latency_ms.to_numpy(float)
    p99 = statistic_diff("p99_latency_ms", lambda idx, side: float(np.percentile((lat_a if side == "a" else lat_b)[idx], 99)),
                         len(lat_a), len(lat_b), paired=False, n_boot=300)
    g = dict(exp.guardrails)
    guardrails = {
        "coverage": {**cov.as_dict(), "floor_rel": g.get("coverage_min_rel_change", -0.10),
                     "ok": bool(cov.rel >= g.get("coverage_min_rel_change", -0.10))},
        "p99_latency_ms": {**p99.as_dict(), "slo_ms": g.get("p99_latency_ms_max", 50),
                           "ok": bool(p99.b <= g.get("p99_latency_ms_max", 50))},
    }
    primary = results[exp.primary_metric]
    s = srm(users, exp.expected_shares)
    decision = ("ship treatment" if primary["permutation_p"] < 0.05 and primary["diff"] > 0
                and all(x["ok"] for x in guardrails.values()) and not s["mismatch"] and leaked == 0
                else "do not ship" if primary["permutation_p"] < 0.05 and primary["diff"] < 0
                else "inconclusive")
    return {"experiment": exp.id, "outcomes": "SIMULATED (counterfactual replay, 24h attribution)",
            "impressions": {control: len(ia), treatment: len(ib)}, "users": users,
            "users_in_both_buckets": int(leaked), "srm": s, "metrics": results, "guardrails": guardrails,
            "primary_metric": exp.primary_metric, "decision": decision,
            "models": {v: sorted(imp[imp.variant == v].model_version.unique().tolist()) for v in (control, treatment)}}


def render(r: dict) -> str:
    c, t = "control", "treatment"
    out = [f"A/B lift report: {r['experiment']}",
           f"Outcomes are {r['outcomes']}.", "",
           f"users: {r['users'].get(c, 0)} control / {r['users'].get(t, 0)} treatment   "
           f"impressions: {r['impressions'][c]} / {r['impressions'][t]}   users in both buckets: {r['users_in_both_buckets']}",
           f"sample-ratio check: chi2 {r['srm']['chi2']:.2f}, p {r['srm']['p']:.3f} -> "
           f"{'MISMATCH' if r['srm']['mismatch'] else 'ok'}", "",
           f"{'metric (per user)':<18}{'control':>9}{'treatment':>11}{'diff':>9}  {'95% CI':<20}{'rel':>8}{'perm p':>9}"]
    for m, d in r["metrics"].items():
        out.append(f"{m:<18}{d['a']:>9.4f}{d['b']:>11.4f}{d['diff']:>+9.4f}  [{d['ci_low']:+.4f}, {d['ci_high']:+.4f}]"
                   f"{d['rel']:>+8.1%}{d['permutation_p']:>9.4f}")
    ctr = r["metrics"]["ctr"]
    out += [f"(impression-level CTR z-test: z {ctr['impression_z']:.2f}, p {ctr['impression_z_p']:.4f}; "
            "optimistic because impressions cluster within users)", "", "guardrails:"]
    gc, gl = r["guardrails"]["coverage"], r["guardrails"]["p99_latency_ms"]
    out.append(f"  coverage        {gc['a']:.4f} -> {gc['b']:.4f} ({gc['rel']:+.1%}; floor {gc['floor_rel']:+.0%})  "
               f"{'ok' if gc['ok'] else 'BREACH'}")
    out.append(f"  p99 latency     {gl['a']:.1f} -> {gl['b']:.1f} ms (SLO {gl['slo_ms']} ms)  {'ok' if gl['ok'] else 'BREACH'}")
    out += ["", f"decision on primary metric ({r['primary_metric']}): {r['decision'].upper()}",
            f"models: control {r['models'][c]}, treatment {r['models'][t]}"]
    return "\n".join(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--experiment", default=None)
    a = ap.parse_args()
    active, exps = load()
    exp = exps[a.experiment] if a.experiment else active
    r = analyze(exp)
    text = render(r)
    print(text)
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    (ARTIFACTS / "lift_report.json").write_text(json.dumps(r, indent=2, default=float))
    (ARTIFACTS / "lift_report.txt").write_text(text + "\n")


if __name__ == "__main__":
    main()
