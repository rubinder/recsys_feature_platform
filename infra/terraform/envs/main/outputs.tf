output "lake_bucket" { value = module.aws.lake_bucket }
output "mlflow_bucket" { value = module.aws.mlflow_bucket }
output "snowflake_database" { value = module.snowflake.database }
output "snowflake_schema" { value = module.snowflake.schema }
output "snowflake_warehouse" { value = module.snowflake.warehouse }
output "snowflake_role" { value = module.snowflake.role }
output "snowflake_user" { value = module.snowflake.user }
output "snowflake_stage" { value = module.snowflake.stage }
output "msk_bootstrap_brokers_iam" { value = module.aws.msk_bootstrap_brokers_iam }
output "redis_endpoint" { value = module.aws.redis_endpoint }
output "eks_cluster_name" { value = module.aws.eks_cluster_name }
output "ecr_repository_url" { value = module.aws.ecr_repository_url }
output "redis_auth_token" {
  value     = module.aws.redis_auth_token
  sensitive = true
}
