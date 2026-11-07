# 0018: Monitoring: Prometheus metrics, PSI drift, alert rules

- Status: accepted
- Phase: 5

## Decision
- **Serving metrics** come from the API itself (`prometheus_client`, `/metrics`):
  request counter by variant/strategy/status, latency histogram by variant
  (buckets around the 50 ms SLO), per-stage latency, cache hits/misses, served
  model versions, experiment assignments.
- **Drift** is computed by a separate exporter process (not inside the request
  path) every 15 s:
  - *feature drift:* PSI of what the online store serves now (users and items in
    the latest impressions) against the distribution the champion was trained on
    (training-window offline features);
  - *prediction drift:* PSI of the top-1 ranker score in the latest impressions
    against the first impressions of the run, per variant.
  PSI with quantile bins on the reference; < 0.1 stable, 0.1–0.25 moderate,
  > 0.25 significant.
- **Freshness** gauges reuse the Phase 3 SLA checks (batch age, stream lag).
- **Alert rules:** p99 latency > 50 ms for 1 m (page), error rate > 1% (page),
  feature PSI > 0.25 (ticket), prediction PSI > 0.25 (ticket), batch features
  older than 8 days (ticket), stream lag > 300 s (page), exporter not running
  (ticket).
- **Grafana** dashboard and datasource are provisioned from files in
  `monitoring/`; anonymous read-only access, all ports on loopback.

## Consequences
- Cumulative features (lifetime interaction counts) drift by construction as
  time passes; their PSI alerts are expected and signal "retrain soon", which is
  what the retraining sensor is for.
- Alertmanager routing (pager/Slack) is not configured; alerts are visible in
  Prometheus and Grafana.
