# 0003: Fixed time-based split, weekly anchors, 7-day labels

- Status: accepted
- Phase: 1

## Context
Every phase compares numbers against earlier ones, so the evaluation protocol
must be fixed once. Random splits leak the future into training and overstate
quality. The ranker also needs explicit labels to learn from.

## Decision
- Global cutoffs in `config/split.yaml`: train before **2000-12-01**, validation
  until **2000-12-29**, test after. Chosen from event quantiles (~80/10/10) and
  committed as literal dates.
- Feature snapshots, anchors, and cutoffs all sit on one weekly grid ending at
  validation_end, so every anchor lines up exactly with a feature snapshot.
- Ranker rows: for weekly anchors `t` (4 in the last 28 days of train, 4 in
  validation), candidates = ALS top-200 fit on events before `t`; label = 1 if
  the user interacts with that candidate in `[t, t + 7 days)` and had not seen
  it before `t`. Only queries with at least one positive are kept (LambdaRank
  learns nothing from all-zero queries).
- Test: features and ALS as of validation_end; ground truth = every unseen item
  the user interacts with after it.

## Consequences
- MovieLens users rate most of their history in their first session, so many
  users with future activity are cold-start at each anchor and are excluded.
  Training data is therefore small (hundreds of queries); docs/phase-1.md reports
  the exact counts.
- Test ground truth spans ~2 years, which makes hit-rate look high; NDCG and the
  ALS-only baseline are the meaningful comparisons.
- Phase 2's streaming replay starts at validation_end, so nothing replayed can
  leak into training.
