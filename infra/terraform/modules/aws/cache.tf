# ElastiCache Redis (cloud equivalent of the local Redis online store).
# Off by default: ~$12/month. TLS in transit, encrypted at rest, AUTH token.
resource "random_password" "redis_auth" {
  count   = var.enable_elasticache ? 1 : 0
  length  = 48
  special = false
}

resource "aws_elasticache_subnet_group" "this" {
  count      = var.enable_elasticache ? 1 : 0
  name       = "${var.prefix}-redis"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_elasticache_replication_group" "this" {
  count                      = var.enable_elasticache ? 1 : 0
  replication_group_id       = "${var.prefix}-online"
  description                = "Feast online store"
  engine                     = "redis"
  engine_version             = "7.1"
  node_type                  = "cache.t4g.micro"
  num_cache_clusters         = 1
  port                       = 6379
  subnet_group_name          = aws_elasticache_subnet_group.this[0].name
  security_group_ids         = [aws_security_group.data[0].id]
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  auth_token                 = random_password.redis_auth[0].result
  automatic_failover_enabled = false
  apply_immediately          = true
}
