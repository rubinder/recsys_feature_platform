# 0001: Two-stage ranking (ALS candidates + LightGBM LambdaRank)

- Status: accepted
- Phase: 1

## Context
The project's deliverable is a feature store with online/offline consistency.
The obvious off-the-shelf recommender, implicit ALS, is pure collaborative
filtering: it consumes an interaction matrix and no features. With ALS alone the
serving path would fetch Feast features and then ignore them, so point-in-time
retrieval, train/serve skew, and the feature experiments in Phase 4 would all be
moot.

## Options considered
1. **ALS only.** Simplest; the feature store becomes decoration.
2. **LightFM with user/item features.** One model that uses features, but
   features enter as static side information, not per-request context, and it
   is not how large-scale systems are built.
3. **Two-stage: ALS retrieves top-K, a learning-to-rank model reranks using
   Feast features plus the ALS score.** Matches production recsys architecture
   (retrieval + ranking); every feature is consumed at both training and serving
   time.

## Decision
Option 3. ALS (`implicit`, 64 factors) generates K=200 candidates; LightGBM
LambdaRank reranks them using 11 Feast features plus `als_score`. Both models are
off-the-shelf. ALS is refit per training anchor on events before that anchor, the
same way serving uses ALS fit on everything before its "now".

## Consequences
- Features now affect ranking quality measurably, so Phase 4 freshness and
  ablation experiments can show real effects.
- Candidate recall caps quality: the ranker cannot recover an item ALS did not
  retrieve. `test_candidate_recall_at_k` is logged to make that ceiling visible.
- Two models to version; both are logged as artifacts of one MLflow run so they
  can never drift apart.
- Serving latency is dominated by the batched online read for 200 candidates
  (see docs/phase-1.md), which motivates the Phase 2 feature cache.
