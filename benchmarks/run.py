"""`make benchmarks`: run every experiment (or --only some), then regenerate the README section."""

from __future__ import annotations

import argparse
import time

from benchmarks import exp_ablation, exp_freshness, exp_latency, exp_pit, report

EXPERIMENTS = {"pit": exp_pit.run, "ablation": exp_ablation.run, "latency": exp_latency.run, "freshness": exp_freshness.run}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=",".join(EXPERIMENTS))
    args = ap.parse_args()
    for name in args.only.split(","):
        t = time.monotonic()
        print(f"=== benchmark: {name}")
        EXPERIMENTS[name]()
        print(f"=== {name} done in {time.monotonic() - t:.0f}s")
    report.main()


if __name__ == "__main__":
    main()
