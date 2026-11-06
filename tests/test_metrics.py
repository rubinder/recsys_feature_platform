import math

from recsys.eval.metrics import evaluate, hit_rate_at_k, ndcg_at_k, recall_at_k


def test_hit_rate():
    assert hit_rate_at_k([1, 2, 3], {3}, 3) == 1.0
    assert hit_rate_at_k([1, 2, 3], {3}, 2) == 0.0


def test_ndcg_perfect_and_partial():
    assert ndcg_at_k([1, 2], {1, 2}, 2) == 1.0
    assert math.isclose(ndcg_at_k([9, 1], {1}, 2), 1 / math.log2(3))
    assert ndcg_at_k([9, 8], {1}, 2) == 0.0


def test_recall():
    assert recall_at_k([1, 2, 3], {1, 4}, 3) == 0.5


def test_evaluate_skips_users_without_truth():
    out = evaluate({1: [1, 2], 2: [3]}, {1: {2}, 2: set()}, [1, 2])
    assert out["users"] == 1.0
    assert out["hit_rate_at_1"] == 0.0 and out["hit_rate_at_2"] == 1.0
