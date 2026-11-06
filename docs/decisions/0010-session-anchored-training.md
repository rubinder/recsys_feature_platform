# 0010: Session-anchored training rows for freshness-sensitive models

- Status: accepted
- Phase: 2

## Context
Phase 1 anchors training rows weekly at midnight. At those instants almost no
user is mid-session, so session features describe sessions that ended days ago,
and a ranker trained on them never learns what fresh context looks like. In
serving, requests happen mid-session. A streaming-feature ranker trained on
weekly anchors scored *below* the batch ranker (test NDCG@10 0.272 vs 0.283).

## Decision
Add `--anchors session`: for each week, ALS is fit on events before the week
(frozen, like the served model), and each user gets one anchor right after the
5th event of their first session with at least 6 events. Labels are unseen items
in the following 7 days; batch features come from the latest weekly snapshot,
streaming features as of the anchor. Both Phase 2 variants (`batch_session`,
`streaming_session`) are trained this way, so only the feature set differs.

## Consequences
- Training matches the serving distribution of the replay simulation.
- Fewer queries than weekly anchoring (299 training queries), since only users
  with a qualifying session count.
- On the static Dec 29 snapshot test (nobody mid-session) the streaming model
  is worse than batch; that test no longer reflects how it is served, and the
  replay metrics are the relevant comparison.
