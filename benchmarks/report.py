"""Regenerate the README RESULTS block and the benchmark charts from benchmarks/results/*.json.

Only the text between <!-- BENCHMARKS:START --> and <!-- BENCHMARKS:END --> is
replaced; the rest of the README is never touched.
"""

from __future__ import annotations

import re

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from benchmarks.common import IMG, ROOT, load  # noqa: E402

README = ROOT / "README.md"
START, END = "<!-- BENCHMARKS:START -->", "<!-- BENCHMARKS:END -->"
# Validated categorical slots 1-2 of the dataviz reference palette (light surface).
BLUE, ORANGE = "#2a78d6", "#eb6834"
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def _style(ax, title: str) -> None:
    ax.set_title(title, loc="left", fontsize=11, color=INK, pad=10)
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=9)
    ax.grid(axis="both", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def _ci(d: dict, digits: int = 4) -> str:
    star = " ✱" if d["significant"] else ""
    return f"{d['diff']:+.{digits}f} [{d['ci_low']:+.{digits}f}, {d['ci_high']:+.{digits}f}]{star}"


def _pct(d: dict) -> str:
    return f"{d['rel']:+.1%}" if d.get("rel") == d.get("rel") else "n/a"


def chart_latency(r: dict) -> str:
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.6), facecolor=SURFACE)
    for ax, metric, label in ((axes[0], "p50", "Median latency (ms), concurrency 1"),
                              (axes[1], "rps", "Throughput (req/s), concurrency 8")):
        conc = 1 if metric == "p50" else 8
        for cache, color, name in ((False, ORANGE, "no cache"), (True, BLUE, "item-feature cache")):
            pts = sorted((c["k"], c[metric]) for c in r["configs"] if c["cache"] == cache and c["concurrency"] == conc)
            xs, ys = zip(*pts)
            ax.plot(xs, ys, color=color, linewidth=2, marker="o", markersize=5, label=name)
            ax.annotate(name, (xs[-1], ys[-1]), xytext=(6, 0), textcoords="offset points", va="center",
                        fontsize=9, color=INK2)
        _style(ax, label)
        ax.set_xscale("log", base=2)
        ax.set_xticks([50, 100, 200, 400], ["50", "100", "200", "400"])
        ax.set_xlabel("candidates fetched per request (K)", color=INK2, fontsize=9)
        ax.set_ylim(bottom=0)
        ax.set_xlim(40, 900)
    axes[0].legend(frameon=False, fontsize=9, loc="upper left")
    fig.tight_layout()
    path = IMG / "bench-latency.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path.name


def chart_ablation(r: dict) -> str:
    rows = [(n, c) for n, c in r["configs"].items() if n != "all features"]
    names = [n for n, _ in rows][::-1]
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.6), sharey=True, facecolor=SURFACE)
    for ax, key, title in ((axes[0], "delta_ndcg", "Change in NDCG@10 vs all features"),
                           (axes[1], "delta_coverage", "Change in catalog coverage@10")):
        for y, n in enumerate(names):
            d = r["configs"][n][key]
            ax.plot([d["ci_low"], d["ci_high"]], [y, y], color=BLUE, linewidth=2, solid_capstyle="round")
            ax.plot(d["diff"], y, "o", color=BLUE, markersize=7, markeredgecolor=SURFACE, markeredgewidth=2)
        ax.axvline(0, color=INK2, linewidth=1)
        _style(ax, title)
        ax.grid(axis="y", visible=False)
    axes[0].set_yticks(range(len(names)), names)
    fig.text(0.01, 0.01, "Dots: point estimate. Lines: 95% paired bootstrap CI over "
             f"{r['queries']} out-of-fold queries.", fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    path = IMG / "bench-ablation.png"
    fig.savefig(path, dpi=160)
    plt.close(fig)
    return path.name


def section_freshness(r: dict) -> str:
    d = {x["metric"]: x for x in r["deltas"]}
    st = r["staleness"]
    lines = [
        "### 1. Feature freshness: batch-only vs batch + streaming",
        "",
        f"Same mid-session-trained ranker architecture, only the feature set differs; both served at all "
        f"{r['request_points']:,} request points of the same {r['days']}-day replay ({r['users']} users). "
        f"Outcomes are **simulated** (counterfactual replay, 24 h attribution). Paired bootstrap 95% CIs.",
        "",
        "| Metric | batch only | + streaming | Change [95% CI] | Relative |",
        "|---|---:|---:|---|---:|",
    ]
    for key, name in (("click", "CTR"), ("hit@5", "Hit-rate@5"), ("ndcg@10", "NDCG@10"), ("coverage", "Catalog coverage")):
        x = d[key]
        lines.append(f"| {name} | {x['a']:.4f} | {x['b']:.4f} | {_ci(x)} | {_pct(x)} |")
    lines += [
        "",
        "| Feature staleness at request time | batch only | + streaming |",
        "|---|---:|---:|",
        f"| User events not yet reflected in features (mean) | {st['batch_unreflected_events_mean']:.1f} | 0 |",
        f"| Requests with unreflected events | {st['batch_requests_with_unreflected_events']:.0%} | 0% |",
        f"| Newest unreflected event, hours after the snapshot (mean) | {st['batch_information_lag_hours_mean']:.1f} | 0 (~0.1 s pipeline) |",
        f"| Age of the batch snapshot (mean days) | {st['batch_feature_age_days_mean']:.1f} | same, but session state is live |",
        "",
        f"**Takeaway:** streaming features carry the {st['batch_unreflected_events_mean']:.0f} events a user has "
        f"typically generated since the last batch snapshot (the current session), which lifts CTR by "
        f"{_pct(d['click'])} and widens coverage by {_pct(d['coverage'])}, because recommendations follow what "
        "the user is doing now instead of their long-run profile.",
    ]
    return "\n".join(lines)


def section_pit(r: dict) -> str:
    p, n, lk, t = r["configs"]["pit"], r["configs"]["latest"], r["leakage"], r["test_delta"]
    val_gain = (n["val_ndcg@10"] - p["val_ndcg@10"]) / p["val_ndcg@10"]
    return "\n".join([
        "### 2. Point-in-time join vs naive latest-value join",
        "",
        "Identical anchors, labels, candidates and model; only the training feature join differs. Both models "
        f"are then served with correct cutoff-time features on the test period ({r['users']:,} users).",
        "",
        "| | Validation NDCG@10 (what offline eval reports) | Test NDCG@10 (how it serves) |",
        "|---|---:|---:|",
        f"| Point-in-time (Feast `get_historical_features`) | {p['val_ndcg@10']:.4f} | **{p['test_ndcg@10']:.4f}** |",
        f"| Naive latest value | **{n['val_ndcg@10']:.4f}** ({val_gain:+.0%}) | {n['test_ndcg@10']:.4f} ({_pct(t)}) |",
        "",
        f"Test change, naive vs PIT: {_ci(t)}. Leakage diagnostics: "
        f"{lk['train_rows_with_future_values']:.0%} of naive training rows carry values from after their anchor; "
        f"for positive rows the item's interaction count grew by {lk['item_count_gain_positive_rows']:.0f} vs "
        f"{lk['item_count_gain_negative_rows']:.0f} for negatives, i.e. the label leaks into the feature.",
        "",
        f"**Takeaway:** the naive join lets features see the label window, so offline validation looks "
        f"{val_gain:.0%} better while the served model is {abs(t['rel']):.0%} worse"
        f"{'' if t['significant'] else ' (95% CI just touches zero)'}: leakage inflates offline metrics and "
        "trains on a feature distribution serving never sees.",
    ])


def section_latency(r: dict, chart: str) -> str:
    def get(cache, k, conc):
        return next(c for c in r["configs"] if c["cache"] == cache and c["k"] == k and c["concurrency"] == conc)

    lines = ["### 3. Serving latency: item-feature cache and fetch batch size", "",
             f"`{r['variant']}` variant, API in Docker, {r['requests']} seeded requests per configuration, median of "
             f"{r['repeats']} runs. Latency is a physical measurement and varies run to run on a laptop.", "",
             "| K (candidates fetched) | p50, no cache | p50, cache | p99, no cache | p99, cache | req/s c=8, no cache | req/s c=8, cache |",
             "|---:|---:|---:|---:|---:|---:|---:|"]
    for k in (50, 100, 200, 400):
        a1, b1, a8, b8 = get(False, k, 1), get(True, k, 1), get(False, k, 8), get(True, k, 8)
        lines.append(f"| {k} | {a1['p50']:.1f} ms | {b1['p50']:.1f} ms | {a1['p99']:.1f} ms | {b1['p99']:.1f} ms | "
                     f"{a8['rps']:.0f} | {b8['rps']:.0f} |")
    a, b = get(False, 400, 1), get(True, 400, 1)
    lines += ["", f"![Latency vs K](docs/img/{chart})", "",
              f"**Takeaway:** without the cache every candidate is an online-store row to fetch and deserialize, so "
              f"p50 grows linearly with K ({get(False, 50, 1)['p50']:.0f} → {a['p50']:.0f} ms); with item features "
              f"cached only the 1-row user read remains, so latency stays nearly flat ({get(True, 50, 1)['p50']:.0f} → "
              f"{b['p50']:.0f} ms) and K becomes a quality knob instead of a latency knob."]
    return "\n".join(lines)


def section_ablation(r: dict, chart: str) -> str:
    lines = ["### 4. Feature ablation (remove one group)", "", f"{r['design']}; {r['queries']} queries scored "
             "out-of-fold. Paired bootstrap 95% CIs; ✱ = CI excludes zero.", "",
             "| Configuration | NDCG@10 | Δ NDCG@10 [95% CI] | Coverage@10 | Δ coverage |", "|---|---:|---|---:|---|"]
    for n, c in r["configs"].items():
        dn = "—" if n == "all features" else _ci(c["delta_ndcg"])
        dc = "—" if n == "all features" else _ci(c["delta_coverage"], 3)
        lines.append(f"| {n} | {c['ndcg@10']:.4f} | {dn} | {c['coverage@10']:.3f} | {dc} |")
    cfg = r["configs"]
    lines += ["", f"![Ablation](docs/img/{chart})", "",
              f"**Takeaway:** popularity is the strongest accuracy signal ({cfg['- popularity']['delta_ndcg']['diff']:+.3f} "
              f"NDCG when removed) and also what concentrates recommendations (coverage {cfg['- popularity']['delta_coverage']['diff']:+.3f} "
              f"without it); session features are the only group that improves accuracy and coverage together "
              f"({cfg['- session']['delta_ndcg']['diff']:+.3f} NDCG, {cfg['- session']['delta_coverage']['diff']:+.3f} coverage when removed), "
              f"and the ranker as a whole adds {abs(cfg['ALS only (no ranker)']['delta_ndcg']['diff']):.3f} NDCG@10 over ALS order."]
    return "\n".join(lines)


def build() -> str:
    parts = [START, "", "## Benchmarks", "",
             "Generated by `make benchmarks` from `benchmarks/results/*.json` (one knob per experiment, seeded, "
             "fixed Phase 1 split; every run is also logged to MLflow under `benchmarks-*`). ✱ = 95% CI excludes zero.", ""]
    if (r := load("freshness")):
        parts += [section_freshness(r), ""]
    if (r := load("pit")):
        parts += [section_pit(r), ""]
    if (r := load("latency")):
        parts += [section_latency(r, chart_latency(r)), ""]
    if (r := load("ablation")):
        parts += [section_ablation(r, chart_ablation(r)), ""]
    parts.append(END)
    return "\n".join(parts)


def main() -> None:
    text = README.read_text()
    block = build()
    if START in text and END in text:
        text = re.sub(re.escape(START) + r".*?" + re.escape(END), lambda _: block, text, flags=re.S)
    else:
        marker = "## Run it"
        text = text.replace(marker, block + "\n\n" + marker, 1)
    README.write_text(text)
    print("README benchmarks section regenerated")


if __name__ == "__main__":
    main()
