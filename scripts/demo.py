"""make demo: call the API for a sample user and print ranked recommendations."""

from __future__ import annotations

import sys
import time

import httpx
import pandas as pd

from recsys.config import RAW

BASE = "http://localhost:8000"
USER = int(sys.argv[1]) if len(sys.argv) > 1 else 6040


def main() -> None:
    for _ in range(60):
        try:
            if httpx.get(f"{BASE}/health", timeout=2).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(1)
    titles = pd.read_parquet(RAW / "items.parquet").set_index("item_id").title
    events = pd.read_parquet(RAW / "events.parquet")
    r = httpx.get(f"{BASE}/recommendations/{USER}", timeout=10).json()

    print(f"\nUser {USER}: features as of {r['features_as_of']}, model run {r['model_version']}")
    hist = events[events.user_id == USER].sort_values("weight", ascending=False).head(5)
    print("Top-rated history:", "; ".join(titles[i] for i in hist.item_id))
    print(f"\nTop {len(r['items'])} recommendations ({r['strategy']}):")
    for rank, it in enumerate(r["items"], 1):
        print(f"  {rank:>2}. {titles.get(it['item_id'], it['item_id']):<55} ranker {it['score']:+.3f}  als {it['als_score']:.3f}")
    print("\nLatency (ms):", ", ".join(f"{k} {v:.2f}" for k, v in r["latency_ms"].items()))


if __name__ == "__main__":
    main()
