# The platform's cloud footprint: AWS (lake, MLflow artifacts, IAM, budget,
# optional MSK + ElastiCache) and Snowflake (Feast offline store).
#
# Credentials come from the environment only:
#   AWS:       AWS_PROFILE / SSO / default credential chain
#   Snowflake: SNOWFLAKE_* env vars (`make terraform-plan` derives them from
#              ~/.snowflake/connections.toml without printing them)

terraform {
  required_version = ">= 1.10"
  backend "s3" {} # partial config: backend.hcl, written by `make terraform-bootstrap`
  required_providers {
    aws       = { source = "hashicorp/aws", version = "~> 6.72" }
    random    = { source = "hashicorp/random", version = "~> 3.9" }
    snowflake = { source = "snowflakedb/snowflake", version = "~> 2.23" }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project    = "recsys-feature-platform"
      env        = var.env
      managed_by = "terraform"
    }
  }
}

provider "snowflake" {
  # organization_name, account_name, user, authenticator, token/private key,
  # and role all come from SNOWFLAKE_* environment variables.
}

data "aws_caller_identity" "me" {}

locals {
  snowflake_role_name = "${var.prefix}-snowflake-lake"
  # Known before either side exists, which breaks the integration <-> role cycle:
  # Snowflake needs the role ARN, the role's trust policy needs Snowflake's outputs.
  snowflake_role_arn = "arn:aws:iam::${data.aws_caller_identity.me.account_id}:role/${local.snowflake_role_name}"
}

module "aws" {
  source                  = "../../modules/aws"
  prefix                  = var.prefix
  account_id              = data.aws_caller_identity.me.account_id
  region                  = var.region
  snowflake_role_name     = local.snowflake_role_name
  snowflake_iam_user_arn  = module.snowflake.storage_aws_iam_user_arn
  snowflake_external_id   = module.snowflake.storage_aws_external_id
  budget_limit_usd        = var.budget_limit_usd
  alert_email             = var.alert_email
  enable_msk              = var.enable_msk
  enable_elasticache      = var.enable_elasticache
  enable_eks              = var.enable_eks
  eks_public_access_cidrs = var.eks_public_access_cidrs
  eks_admin_principal_arn = data.aws_caller_identity.me.arn
}

module "snowflake" {
  source               = "../../modules/snowflake"
  prefix               = var.prefix
  lake_bucket          = "${var.prefix}-lake-${data.aws_caller_identity.me.account_id}"
  aws_role_arn         = local.snowflake_role_arn
  feast_rsa_public_key = var.feast_rsa_public_key
  credit_quota         = var.snowflake_credit_quota
}
