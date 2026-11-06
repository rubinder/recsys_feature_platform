from recsys.latency.histogram import LatencyHistogram


def test_percentiles_and_buckets():
    h = LatencyHistogram()
    for ms in range(1, 101):
        h.observe("total", float(ms))
    s = h.summary()["total"]
    assert s["count"] == 100 and 49 <= s["p50_ms"] <= 51 and s["p99_ms"] >= 99
    assert dict(h.buckets()["total"])[10] == 10
    h.reset()
    assert h.summary() == {}
