# Private network for MSK / ElastiCache, created only when either is enabled.
# No NAT gateway and no internet gateway: these services are only reachable from
# compute inside the VPC (none is created in Phase 3; see docs/phase-3.md).

locals {
  needs_vpc = var.enable_msk || var.enable_elasticache
}

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "this" {
  count                = local.needs_vpc ? 1 : 0
  cidr_block           = "10.40.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "${var.prefix}-vpc" }
}

resource "aws_subnet" "private" {
  count             = local.needs_vpc ? 2 : 0
  vpc_id            = aws_vpc.this[0].id
  cidr_block        = cidrsubnet("10.40.0.0/16", 8, count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]
  tags              = { Name = "${var.prefix}-private-${count.index}" }
}

resource "aws_security_group" "data" {
  count       = local.needs_vpc ? 1 : 0
  name        = "${var.prefix}-data"
  description = "Kafka and Redis, reachable from inside the VPC only"
  vpc_id      = aws_vpc.this[0].id

  ingress {
    description = "Kafka (IAM auth, TLS)"
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.this[0].cidr_block]
  }
  ingress {
    description = "Redis (TLS)"
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.this[0].cidr_block]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_vpc.this[0].cidr_block]
  }
}
