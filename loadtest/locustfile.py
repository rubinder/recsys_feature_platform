"""Locust load test for the serving API; fails the run if the p99 latency SLO is breached.

    uv run --group loadtest locust -f loadtest/locustfile.py --headless -u 32 -r 8 -t 60s --host http://localhost:8080
"""

from __future__ import annotations

import os
import random

from locust import HttpUser, between, events, task

SLO_P99_MS = float(os.environ.get("SLO_P99_MS", "50"))
USERS = list(range(1, 6041))


class Recommender(HttpUser):
    wait_time = between(0.0, 0.05)

    @task
    def recommend(self) -> None:
        self.client.get(f"/recommendations/{random.choice(USERS)}?log_impression=false", name="/recommendations/{user}")


@events.quitting.add_listener
def slo_gate(environment, **_) -> None:
    stats = environment.stats.total
    p99 = stats.get_response_time_percentile(0.99)
    print(f"requests {stats.num_requests}, failures {stats.num_failures}, p50 {stats.median_response_time} ms, "
          f"p99 {p99} ms (SLO {SLO_P99_MS} ms), {stats.total_rps:.0f} req/s")
    if p99 > SLO_P99_MS or stats.fail_ratio > 0.01:
        print("SLO BREACHED")
        environment.process_exit_code = 1
