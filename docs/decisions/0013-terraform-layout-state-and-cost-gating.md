# 0013: Terraform layout, remote state with S3-native locking, cost gating

- Status: accepted
- Phase: 3

## Decision
- **Layout:** `bootstrap/` (state bucket, local state), `modules/aws`,
  `modules/snowflake`, and `envs/main` wiring them with a partial S3 backend
  (`backend.hcl` written by bootstrap). Before bootstrap, a gitignored
  `backend_override.tf` forces local state so `terraform plan` works on day one.
- **Locking:** S3-native lock files (`use_lockfile = true`). The prompt asked for
  DynamoDB locking, which Terraform deprecated in 1.11 in favour of S3 lock
  files; native locking needs no table and no extra cost.
- **Cost gating:** MSK (~$70/month) and ElastiCache (~$12/month) behind
  `enable_msk` / `enable_elasticache`, default false. Snowflake's warehouse is XS
  with 60 s auto-suspend under a 10-credit monthly resource monitor; an AWS Budget
  (tag-filtered to this project) alerts at 80% actual and 100% forecast of $25.
- **No apply by automation:** `make terraform-plan` is read-only;
  `terraform-apply` applies only the saved plan and is run by a person.
- **Credentials:** never in files. Snowflake env vars are derived from
  `~/.snowflake/connections.toml` inside the make recipe; the Feast service user
  authenticates with a key pair kept in `.secrets/` (gitignored).
- **Integration without a cycle:** Snowflake's storage integration needs the IAM
  role ARN and the role's trust policy needs Snowflake's IAM user and external
  ID. The role ARN is computed from the account ID and a fixed name, and we set
  the external ID ourselves (random UUID), so one apply creates both sides.

## Consequences
- `terraform plan` against the real accounts: 38 resources by default, 46 with
  MSK and ElastiCache enabled; both validate.
- MSK and ElastiCache are only reachable inside the VPC; no compute exists there
  until Phase 5's optional EKS deployment.
