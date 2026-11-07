"""Registry operations: bootstrap, retrain + gate + promote, promote, rollback, status.

    python -m recsys.registry.cli bootstrap        champion = batch_session run, challenger = streaming_session run
    python -m recsys.registry.cli retrain --feature-set streaming --anchors session [--demo-bad-challenger]
    python -m recsys.registry.cli promote          gate the current challenger against the champion
    python -m recsys.registry.cli rollback
    python -m recsys.registry.cli status
"""

from __future__ import annotations

import argparse
import json
import sys

import httpx
import mlflow

from recsys import registry, tracking
from recsys.features.features import FEATURE_SETS
from recsys.registry import gate
from recsys.serving.recommender import discover_variants

API = "http://localhost:8000"


def _notify_serving() -> None:
    try:
        r = httpx.post(f"{API}/admin/reload", timeout=120).json()
        print("serving reloaded:", r)
    except httpx.HTTPError:
        print("serving not reachable; it picks up the change on its next 30 s poll or restart")


def _log_decision(champion: str, challenger: str, result: gate.GateResult, action: str) -> None:
    tracking.setup()
    if mlflow.get_experiment_by_name("promotions") is None:
        mlflow.create_experiment("promotions")
    mlflow.set_experiment("promotions")
    with mlflow.start_run(run_name=f"v{challenger}-vs-v{champion}-{action}"):
        mlflow.log_params({"champion_version": champion, "challenger_version": challenger, "action": action,
                           "margin": result.margin, "dq_passed": result.dq_passed})
        mlflow.log_metrics({"ndcg_diff": result.delta["diff"], "ci_low": result.delta["ci_low"],
                            "ci_high": result.delta["ci_high"], "queries": result.queries})
        mlflow.set_tag("reason", result.reason)


def gate_and_decide(challenger: str) -> bool:
    champion = registry.alias_version("champion")
    if champion is None:
        registry.promote(challenger, "first champion (no incumbent)")
        print(f"v{challenger} is the first champion")
        return True
    res = gate.evaluate(registry.run_of(champion), registry.run_of(challenger))
    c = registry.client()
    c.set_model_version_tag(registry.MODEL, challenger, "gate", "passed" if res.passed else "rejected")
    c.set_model_version_tag(registry.MODEL, challenger, "gate_reason", res.reason[:250])
    print(f"gate v{challenger} vs champion v{champion} on {res.queries} held-out queries: {res.reason}")
    if res.passed:
        registry.promote(challenger, res.reason)
        print(f"PROMOTED v{challenger} to champion (previous v{champion} kept for rollback)")
        _log_decision(champion, challenger, res, "promoted")
    else:
        c.set_model_version_tag(registry.MODEL, challenger, "stage", "rejected")
        # A rejected model must not keep receiving experiment traffic through the alias.
        if registry.alias_version("challenger") == challenger:
            c.delete_registered_model_alias(registry.MODEL, "challenger")
        print(f"REJECTED v{challenger}; champion stays v{champion}; challenger alias removed")
        _log_decision(champion, challenger, res, "rejected")
    return res.passed


def cmd_bootstrap(_: argparse.Namespace) -> None:
    runs = discover_variants()
    for need in ("batch_session", "streaming_session"):
        if need not in runs:
            sys.exit(f"no {need} run yet: run `make train-streaming` first")
    v1 = registry.register(runs["batch_session"], {"variant": "batch_session"})
    v2 = registry.register(runs["streaming_session"], {"variant": "streaming_session"})
    if registry.alias_version("champion") is None:
        registry.promote(v1, "initial champion (batch features)")
    registry.set_alias("challenger", v2)
    print(f"champion = v{registry.alias_version('champion')}, challenger = v{v2}")
    _notify_serving()


def cmd_retrain(a: argparse.Namespace) -> None:
    from recsys.training.train import train, variant_name

    fs = FEATURE_SETS[a.feature_set]
    run_id, m = train(fs, a.anchors, record=False, demo_shuffle_labels=a.demo_bad_challenger)
    variant = variant_name(fs.name, a.anchors) + ("_shuffled" if a.demo_bad_challenger else "")
    v = registry.register(run_id, {"variant": variant, "val_ndcg_at_10": round(m["val_ndcg_at_10"], 4)})
    registry.set_alias("challenger", v)
    print(f"registered challenger v{v} ({variant}, run {run_id[:8]})")
    if gate_and_decide(v):
        _notify_serving()


def cmd_promote(_: argparse.Namespace) -> None:
    v = registry.alias_version("challenger")
    if v is None:
        sys.exit("no challenger alias set")
    if gate_and_decide(v):
        _notify_serving()


def cmd_rollback(a: argparse.Namespace) -> None:
    frm, to = registry.rollback(a.reason)
    print(f"ROLLED BACK champion v{frm} -> v{to}")
    _notify_serving()


def cmd_status(_: argparse.Namespace) -> None:
    print(json.dumps(registry.versions(), indent=2))


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("bootstrap").set_defaults(fn=cmd_bootstrap)
    r = sub.add_parser("retrain")
    r.add_argument("--feature-set", default="streaming", choices=sorted(FEATURE_SETS))
    r.add_argument("--anchors", default="session", choices=["weekly", "session"])
    r.add_argument("--demo-bad-challenger", action="store_true", help="shuffle labels to show a rejection")
    r.set_defaults(fn=cmd_retrain)
    sub.add_parser("promote").set_defaults(fn=cmd_promote)
    rb = sub.add_parser("rollback")
    rb.add_argument("--reason", default="manual rollback")
    rb.set_defaults(fn=cmd_rollback)
    sub.add_parser("status").set_defaults(fn=cmd_status)
    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
