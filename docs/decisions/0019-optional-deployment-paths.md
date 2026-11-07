# 0019: Optional deployment paths: kind + Helm, and EKS

- Status: accepted (not executed; see docs/phase-5.md)
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
  gateway), node access to the lake/MLflow buckets, and a security-group rule to
  ElastiCache. The same chart deploys with `redis.enabled=false` and
  `redis.external` pointing at ElastiCache (TLS + AUTH). `make eks-plan` is
  read-only; `eks-apply` is run by a person; `eks-destroy` turns it off again.
  Estimated ~$120/month with ElastiCache while it exists.
- Both are behind explicit flags so the default path needs no cluster and no
  cloud account.

## Consequences
- `terraform plan` with EKS + ElastiCache enabled resolves to 62 resources
  against the real account. The Helm chart and Locust test have not been run
  (kind and Helm were not installed on the build machine).
