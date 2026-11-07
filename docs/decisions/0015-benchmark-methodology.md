# 0015: Benchmark methodology

- Status: accepted
- Phase: 4

## Decision
- **One knob per experiment.** Every other input (split, anchors, labels,
  candidates, folds, boosting rounds, seed, request sample) is held fixed and
  shared, so a difference can only come from the knob.
- **Paired comparisons wherever the design allows.** Freshness compares both
  variants at the same 3,054 request points; ablation compares configurations
  on the same out-of-fold queries; the PIT experiment compares both models on
  the same test users. Pairing removes per-unit variance and tightens intervals
  several-fold versus independent samples (tested in `tests/test_compare.py`).
- **Seeded bootstrap 95% CIs** from one module (`benchmarks/compare.py`):
  `paired_diff`, `independent_diff` (for Phase 5 A/B buckets) and
  `statistic_diff` for aggregate statistics. Coverage-type statistics are biased
  under resampling with replacement, so `statistic_diff` uses a bias-corrected
  percentile interval (found when a first version produced CIs that excluded
  their own point estimates).
- **Enough units to say something.** The first ablation design held out two
  weeks (153 queries) and produced noisy, partly contradictory effects. It was
  replaced by 5-fold cross-validation grouped by user over all 727 queries with a
  fixed number of boosting rounds.
- **Simulated outcomes are labelled as such** wherever CTR appears.
- **Latency is reported as the median of 3 runs** per configuration with the
  same seeded request sample; it is the only experiment that is not bit-for-bit
  reproducible, because it measures a machine.
- **Every configuration is an MLflow run** under `benchmarks-<experiment>`; the
  README section is generated from `benchmarks/results/*.json` between marker
  comments.

## Consequences
- Statistical claims in the README are backed by intervals; non-significant
  effects are reported as such rather than omitted.
- The full suite takes ~20 minutes, dominated by the 28-day replay.
