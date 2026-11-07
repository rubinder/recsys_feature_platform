# 0014: Local services bind to loopback; job-submission APIs are not published

- Status: accepted
- Phase: 3 (hardening of Phase 1-2 code)

## Context
Automated security review flagged that Redis (no auth) was published on all
interfaces, and later that Flink's REST API and Feast's `/push` endpoint were
reachable from the host. Flink's REST API accepts job submissions without auth;
a malicious web page can reach `localhost` services through DNS rebinding.

## Decision
- Every published port binds to `127.0.0.1`.
- The Feast feature server is not published at all (only Flink, inside the
  compose network, pushes features).
- Flink's REST port is not published. Job control (`flink run` / `flink cancel`,
  REST reads) runs inside the jobmanager container; `web.submit.enable` and
  `web.cancel.enable` are off as defence in depth. The dashboard is opt-in via
  `make flink-ui`, a temporary loopback-only socat proxy.
- Kafka advertises `127.0.0.1` to host clients.

## Consequences
- No unauthenticated code-execution or feature-write surface on the host by
  default. The cloud equivalents (MSK with IAM auth + TLS, ElastiCache with TLS +
  AUTH) are authenticated by construction.
