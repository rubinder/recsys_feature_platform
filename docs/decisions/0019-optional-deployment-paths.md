# 0019: Optional deployment paths: kind + Helm, and EKS

- Status: accepted (EKS executed in a temporary session, docs/phase-5-eks-session.md; kind not run)
- Phase: 5

## Decision
- **K8S=1:** a Helm chart (`deploy/helm/recsys`) runs the API (2 replicas,
  readiness/liveness on `/health`), an in-cluster Redis and a post-install Feast
  materialize Job on a kind cluster whose node mounts `data/` and `mlruns/` at
  their absolute paths. A Locust run (`make loadtest K8S=1`) fails if p99 exceeds
  the 50 ms SLO or more than 1% of requests fail.
- **EKS=1:** Terraform in the Phase 3 AWS module adds ECR, an EKS cluster
  (API authentication mode, endpoint restricted to `eks_public_access_cidrs`),
  one t3.medium managed node group in public subnets of the same VPC (no NAT
  gateway, IMDSv2 with hop limit 1), lake/MLflow access for the API's service
  account only (EKS Pod Identity, not the node role), and a security-group rule
  to ElastiCache. Pods run as non-root with a read-only root filesystem. The same chart deploys with `redis.enabled=false` and
  `redis.external` pointing at ElastiCache (TLS + AUTH). `make eks-plan` is
  read-only; `eks-apply` is run by a person; `eks-destroy` turns it off again.
  Estimated ~$120/month with ElastiCache while it exists.
- Both are behind explicit flags so the default path needs no cluster and no
  cloud account.

## Consequences
- The EKS path ran for real: 30 resources up in 12.5 min, Snowflake →
  ElastiCache in 67 s, in-cluster p99 30 ms at one client (SLO met) and 230 ms
  at eight (saturated), torn down and verified clean. It needed an explicit
  EKS access entry, `fsGroup` for root-owned secret files, ARM nodes, and
  self-refreshing AWS credentials (exported `aws login` credentials expire in
  ~15 minutes, shorter than an EKS apply).
