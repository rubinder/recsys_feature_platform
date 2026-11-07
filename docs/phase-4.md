# Phase 4: Benchmarks

**Goal:** demonstrate the trade-offs behind the design with reproducible
experiments that each vary one knob and report real, measured before/after
numbers with confidence intervals.

## What was built

| Piece | Implementation |
|---|---|
| Harness | `benchmarks/`: one module per experiment, `run.py` driver, `make benchmarks` ([ADR 0015](decisions/0015-benchmark-methodology.md)) |
| Comparison module | `benchmarks/compare.py`: `paired_diff`, `independent_diff`, `statistic_diff` (bias-corrected) returning typed `Delta`s with seeded bootstrap 95% CIs; tested; Phase 5 builds on it |
| Results | `benchmarks/results/<experiment>.json` (config + metrics + CIs), MLflow runs under `benchmarks-*` |
| Report | `benchmarks/report.py` regenerates the README section between `<!-- BENCHMARKS:START -->` / `END` markers and two charts in `docs/img/` |
| Reuse | Phase 1 split, metrics and latency load generator; Phase 2 simulator, attribution and cache; Phase 3 quality-checked features. No new infrastructure (one small addition: a `k` request parameter on the API for the batch-size knob) |

## Results

The full tables, charts and one-line takeaways are in the
[README Benchmarks section](../README.md#benchmarks). Summary:

| Experiment | Knob | Headline (95% CI) |
|---|---|---|
| 1. Feature freshness | batch-only vs batch + streaming features | CTR +0.050 [+0.035, +0.065] (+12.6%), NDCG@10 +0.012 [+0.007, +0.016], coverage ×2.7; batch features miss 72 user events at the average request |
| 2. Point-in-time correctness | PIT join vs naive latest-value join | naive join: validation NDCG@10 **+20%** (looks better), served test NDCG@10 **−3.0%** [−0.018, +0.0005]; 100% of naive training rows carry future values |
| 3. Serving latency | item-feature cache × candidates fetched (K) | no cache: p50 grows with K (10 → 35 ms across two runs: 11 → 42 ms); cache: nearly flat (6 → 7 ms), throughput ×2–7 at concurrency 8 |
| 4. Feature ablation | remove one feature group | popularity: −0.047 NDCG but +0.159 coverage when removed (popularity bias); session: the only group that helps accuracy **and** coverage; ranker adds +0.079 NDCG over ALS order |

## Mechanisms (why each result happens)

1. **Freshness.** Batch features stop at the weekly snapshot; by the time of a
   request the user has usually produced dozens of interactions the batch
   features cannot see, most of them in the current session. Streaming features
   carry that session, so recommendations follow current intent, which both
   anticipates the next interaction better and spreads recommendations across
   more of the catalog.
2. **Point-in-time.** A latest-value join lets training rows see counts that
   include their own label window. Items that end up being clicked gain more
   interactions after the anchor than items that don't (+254 vs +201), so the
   model learns a feature-label relationship that exists only in hindsight:
   offline validation (also leaky) rewards it, serving punishes it.
3. **Latency.** Uncached, each candidate is a row to fetch from Redis and
   deserialize in Python, so cost is linear in K. Item features change weekly, so
   caching them costs no freshness; what remains per request is one user row.
4. **Ablation.** Popularity is a strong prior for "what will this user rate
   next" but pulls every list toward the same head items. Session features are
   user-specific and current, which is why removing them hurts accuracy and
   halves coverage at once.

## Reproducibility

- Two complete runs of experiments 1, 2 and 4 give identical numbers (seeded
  training, deterministic lockstep simulation, fixed folds).
- Latency varies run to run by a few milliseconds; the conclusions (linear
  without cache, flat with cache) hold in every run.

## Problems found and fixed

- **Ablation too noisy to read.** A two-week holdout (153 queries) made two
  removals look like significant improvements. Grouped 5-fold CV over 727
  queries fixed the power problem; those effects disappeared.
- **Confidence intervals excluding their own estimate** for coverage: the
  bootstrap of a distinct-count statistic is biased downward. Fixed with a
  bias-corrected percentile interval and a regression test.
- **MLflow rejects `@` in metric names**; names are sanitized centrally.

## Limitations

- Freshness CTR is simulated (counterfactual replay); it cannot capture
  feedback effects of the recommendations themselves.
- The PIT test difference is borderline (CI touches zero); the validation
  inflation is the robust effect.
- Latency was measured on a laptop with Docker Desktop; absolute numbers are
  machine-specific, the shape is the finding.
- The optional ALS factors / top-K sweep was not run (model tuning is out of
  focus for this phase).

## Artifacts

- [README Benchmarks section](../README.md#benchmarks) (generated)
- [Latency chart](img/bench-latency.png), [ablation chart](img/bench-ablation.png)
- `benchmarks/results/*.json`, MLflow experiments `benchmarks-freshness`, `-pit`, `-latency`, `-ablation`
