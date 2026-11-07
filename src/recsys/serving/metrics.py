"""Prometheus metrics for the serving API (scraped at /metrics)."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

LATENCY_BUCKETS_S = (0.002, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.03, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0)

REQUESTS = Counter("recsys_requests_total", "Recommendation requests", ["variant", "strategy", "status"])
LATENCY = Histogram("recsys_request_latency_seconds", "Server-side recommendation latency", ["variant"],
                    buckets=LATENCY_BUCKETS_S)
STAGE_LATENCY = Histogram("recsys_stage_latency_seconds", "Latency per serving stage", ["stage"],
                          buckets=LATENCY_BUCKETS_S)
CACHE_HITS = Gauge("recsys_feature_cache_hits", "Item-feature cache hits since start")
CACHE_MISSES = Gauge("recsys_feature_cache_misses", "Item-feature cache misses since start")
MODEL_VERSION = Gauge("recsys_model_info", "Served model per variant (value 1)", ["variant", "run_id", "registry_version"])
ASSIGNMENTS = Counter("recsys_experiment_assignments_total", "Experiment assignments", ["experiment", "variant"])
