# MSK (cloud equivalent of the local Kafka). Off by default: ~$70/month.
resource "aws_msk_cluster" "this" {
  count                  = var.enable_msk ? 1 : 0
  cluster_name           = "${var.prefix}-kafka"
  kafka_version          = "3.8.x"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = aws_subnet.private[*].id
    security_groups = [aws_security_group.data[0].id]
    storage_info {
      ebs_storage_info { volume_size = 10 }
    }
  }

  client_authentication {
    sasl { iam = true }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }
}
