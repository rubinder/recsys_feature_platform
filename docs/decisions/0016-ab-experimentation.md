# 0016: A/B experimentation: hash assignment, user-level analysis, SRM, guardrails

- Status: accepted
- Phase: 5

## Decision
- **Assignment:** `sha256("<experiment>:<salt>:<user_id>") mod 10000`, mapped to
  variants by cumulative weight (`config/experiments.yaml`). Deterministic and
  stateless: any API replica assigns the same user identically, no assignment
  store is needed, and a new salt reshuffles users for the next experiment.
  The API assigns only when a request names no variant, and logs the arm in the
  `variant` / `experiment_id` fields reserved in Phase 2's impression contract;
  `model_version` records which registered model served it.
- **Variants are registry aliases** (`control` → `champion`, `treatment` →
  `challenger`), so an experiment tests exactly what the registry would ship.
- **Unit of analysis = unit of randomization (user).** Metrics are averaged per
  user, then compared between buckets as independent samples with the Phase 4
  `independent_diff` (bootstrap CI). The primary test is a user-level permutation
  test; an impression-level two-proportion z-test is reported but flagged as
  optimistic, since impressions within a user are correlated.
- **Validity checks:** sample-ratio mismatch (chi-square of users per bucket vs
  configured weights, alarm at p < 0.001) and "users in both buckets" must be 0.
- **Guardrails:** catalog coverage may not fall more than 10%; treatment p99
  latency must stay within the 50 ms SLO (from the per-request `latency_ms` added
  to the impression contract in schema v2, an additive, backward-compatible
  change).
- **Decision rule:** ship only if the primary metric improves with p < 0.05, all
  guardrails hold, and SRM / bucket leakage checks pass; otherwise
  "inconclusive" or "do not ship".

## Consequences
- Compared with Phase 2's counterfactual replay (every request served by both
  variants), a real A/B design halves the data per variant and adds between-user
  variance, so effects need more traffic to reach significance; the replay
  remains the higher-power offline tool, the A/B test is what would run live.
- Outcomes remain simulated (replayed behaviour), labelled in every report.
