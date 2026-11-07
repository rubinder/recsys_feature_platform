output "lake_bucket" { value = aws_s3_bucket.this["lake"].bucket }
output "mlflow_bucket" { value = aws_s3_bucket.this["mlflow"].bucket }
output "snowflake_role_arn" { value = aws_iam_role.snowflake_lake.arn }
output "app_role_arn" { value = aws_iam_role.app.arn }
output "msk_bootstrap_brokers_iam" {
  value = var.enable_msk ? aws_msk_cluster.this[0].bootstrap_brokers_sasl_iam : null
}
output "redis_endpoint" {
  value = var.enable_elasticache ? aws_elasticache_replication_group.this[0].primary_endpoint_address : null
}
output "redis_auth_token" {
  value     = var.enable_elasticache ? random_password.redis_auth[0].result : null
  sensitive = true
}
output "eks_cluster_name" {
  value = var.enable_eks ? aws_eks_cluster.this[0].name : null
}
output "ecr_repository_url" {
  value = var.enable_eks ? aws_ecr_repository.api[0].repository_url : null
}
