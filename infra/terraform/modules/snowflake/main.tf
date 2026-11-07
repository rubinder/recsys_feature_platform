# Snowflake as Feast's cloud offline store.
#   warehouse  XS, 60 s auto-suspend, capped by a monthly resource monitor
#   database / schema for feature tables
#   role + key-pair service user for Feast, least-privilege grants
#   S3 storage integration + Parquet external stage over the lake's offline/ prefix

terraform {
  required_providers {
    snowflake = { source = "snowflakedb/snowflake", version = "~> 2.23" }
    random    = { source = "hashicorp/random", version = "~> 3.9" }
  }
}

locals {
  name = upper(var.prefix)
  db   = local.name
}

resource "snowflake_resource_monitor" "this" {
  name                      = "${local.name}_MONITOR"
  credit_quota              = var.credit_quota
  frequency                 = "MONTHLY"
  start_timestamp           = "IMMEDIATELY"
  notify_triggers           = [80]
  suspend_trigger           = 100
  suspend_immediate_trigger = 110
}

resource "snowflake_warehouse" "this" {
  name                = "${local.name}_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = "true"
  initially_suspended = true
  resource_monitor    = snowflake_resource_monitor.this.name
  comment             = "Feast offline store compute (recsys-feature-platform)"
}

resource "snowflake_database" "this" {
  name    = local.db
  comment = "recsys-feature-platform"
}

resource "snowflake_schema" "features" {
  database = snowflake_database.this.name
  name     = "FEATURES"
}

resource "snowflake_account_role" "feast" {
  name    = "${local.name}_FEAST"
  comment = "Feast offline store: read/write feature tables, read the lake stage"
}

resource "snowflake_service_user" "feast" {
  name              = "${local.name}_FEAST_SVC"
  comment           = "Feast service user (key-pair auth only)"
  rsa_public_key    = var.feast_rsa_public_key
  default_role      = snowflake_account_role.feast.name
  default_warehouse = snowflake_warehouse.this.name
  default_namespace = "${snowflake_database.this.name}.${snowflake_schema.features.name}"
}

resource "snowflake_grant_account_role" "feast_user" {
  role_name = snowflake_account_role.feast.name
  user_name = snowflake_service_user.feast.name
}

resource "snowflake_grant_account_role" "feast_to_sysadmin" {
  role_name        = snowflake_account_role.feast.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_privileges_to_account_role" "warehouse" {
  account_role_name = snowflake_account_role.feast.name
  privileges        = ["USAGE", "OPERATE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.this.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "database" {
  account_role_name = snowflake_account_role.feast.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.this.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema" {
  account_role_name = snowflake_account_role.feast.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE STAGE", "CREATE FILE FORMAT", "CREATE VIEW"]
  on_schema {
    schema_name = "\"${snowflake_database.this.name}\".\"${snowflake_schema.features.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "future_tables" {
  account_role_name = snowflake_account_role.feast.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.this.name}\".\"${snowflake_schema.features.name}\""
    }
  }
}

# We set the external ID ourselves (random UUID), so the IAM role's trust policy
# only needs Snowflake's IAM user ARN from the integration's describe output.
resource "random_uuid" "external_id" {}

resource "snowflake_storage_integration_aws" "lake" {
  name                      = "${local.name}_LAKE"
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = var.aws_role_arn
  storage_aws_external_id   = random_uuid.external_id.result
  storage_allowed_locations = ["s3://${var.lake_bucket}/offline/"]
}

resource "snowflake_grant_privileges_to_account_role" "integration" {
  account_role_name = snowflake_account_role.feast.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "INTEGRATION"
    object_name = snowflake_storage_integration_aws.lake.name
  }
}

resource "snowflake_file_format_parquet" "parquet" {
  name     = "PARQUET"
  database = snowflake_database.this.name
  schema   = snowflake_schema.features.name
}

resource "snowflake_stage_external_s3" "lake" {
  name                = "LAKE_OFFLINE"
  database            = snowflake_database.this.name
  schema              = snowflake_schema.features.name
  url                 = "s3://${var.lake_bucket}/offline/"
  storage_integration = snowflake_storage_integration_aws.lake.name
  file_format {
    format_name = snowflake_file_format_parquet.parquet.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "stage" {
  account_role_name = snowflake_account_role.feast.name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "STAGE"
    object_name = "\"${snowflake_database.this.name}\".\"${snowflake_schema.features.name}\".\"${snowflake_stage_external_s3.lake.name}\""
  }
}
