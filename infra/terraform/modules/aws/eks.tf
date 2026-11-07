# Optional (enable_eks): EKS cluster + managed node group + ECR, reusing the VPC
# that MSK / ElastiCache live in, so the serving API can reach ElastiCache.
# ~$110/month while it exists (control plane $73 + one t3.medium + public IPv4).
# Nodes run in public subnets (no NAT gateway cost) with a restricted API endpoint.

resource "aws_internet_gateway" "this" {
  count  = var.enable_eks ? 1 : 0
  vpc_id = aws_vpc.this[0].id
}

resource "aws_subnet" "public" {
  count                   = var.enable_eks ? 2 : 0
  vpc_id                  = aws_vpc.this[0].id
  cidr_block              = cidrsubnet("10.40.0.0/16", 8, 10 + count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  tags = {
    Name                                      = "${var.prefix}-public-${count.index}"
    "kubernetes.io/role/elb"                  = "1"
    "kubernetes.io/cluster/${var.prefix}-eks" = "shared"
  }
}

resource "aws_route_table" "public" {
  count  = var.enable_eks ? 1 : 0
  vpc_id = aws_vpc.this[0].id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this[0].id
  }
}

resource "aws_route_table_association" "public" {
  count          = var.enable_eks ? 2 : 0
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public[0].id
}

resource "aws_ecr_repository" "api" {
  count                = var.enable_eks ? 1 : 0
  name                 = "${var.prefix}-api"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true
  image_scanning_configuration { scan_on_push = true }
}

data "aws_iam_policy_document" "eks_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eks_cluster" {
  count              = var.enable_eks ? 1 : 0
  name               = "${var.prefix}-eks-cluster"
  assume_role_policy = data.aws_iam_policy_document.eks_trust.json
}

resource "aws_iam_role_policy_attachment" "eks_cluster" {
  count      = var.enable_eks ? 1 : 0
  role       = aws_iam_role.eks_cluster[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_eks_cluster" "this" {
  count    = var.enable_eks ? 1 : 0
  name     = "${var.prefix}-eks"
  role_arn = aws_iam_role.eks_cluster[0].arn
  vpc_config {
    subnet_ids              = concat(aws_subnet.public[*].id, aws_subnet.private[*].id)
    endpoint_public_access  = length(var.eks_public_access_cidrs) > 0
    endpoint_private_access = true
    public_access_cidrs     = length(var.eks_public_access_cidrs) > 0 ? var.eks_public_access_cidrs : null
  }
  access_config { authentication_mode = "API" }
  depends_on = [aws_iam_role_policy_attachment.eks_cluster]
}

data "aws_iam_policy_document" "node_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eks_node" {
  count              = var.enable_eks ? 1 : 0
  name               = "${var.prefix}-eks-node"
  assume_role_policy = data.aws_iam_policy_document.node_trust.json
}

resource "aws_iam_role_policy_attachment" "eks_node" {
  for_each = var.enable_eks ? toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
  ]) : toset([])
  role       = aws_iam_role.eks_node[0].name
  policy_arn = each.value
}

# Lake + MLflow access is granted to the API's pods only (service account
# recsys/recsys) via EKS Pod Identity, never to the node role, so other pods on
# the node cannot use it. IMDSv2 with hop limit 1 keeps pods off the node role.
resource "aws_eks_addon" "pod_identity" {
  count        = var.enable_eks ? 1 : 0
  cluster_name = aws_eks_cluster.this[0].name
  addon_name   = "eks-pod-identity-agent"
}

data "aws_iam_policy_document" "pod_identity_trust" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "api_pods" {
  count              = var.enable_eks ? 1 : 0
  name               = "${var.prefix}-api-pods"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_trust.json
}

resource "aws_iam_role_policy_attachment" "api_pods" {
  count      = var.enable_eks ? 1 : 0
  role       = aws_iam_role.api_pods[0].name
  policy_arn = aws_iam_policy.app.arn
}

resource "aws_eks_pod_identity_association" "api" {
  count           = var.enable_eks ? 1 : 0
  cluster_name    = aws_eks_cluster.this[0].name
  namespace       = "default"
  service_account = "recsys"
  role_arn        = aws_iam_role.api_pods[0].arn
  depends_on      = [aws_eks_addon.pod_identity]
}

resource "aws_launch_template" "nodes" {
  count = var.enable_eks ? 1 : 0
  name  = "${var.prefix}-eks-nodes"
  metadata_options {
    http_tokens                 = "required" # IMDSv2 only
    http_put_response_hop_limit = 1          # pods cannot reach the node's instance credentials
    http_endpoint               = "enabled"
  }
}

resource "aws_eks_node_group" "default" {
  count           = var.enable_eks ? 1 : 0
  cluster_name    = aws_eks_cluster.this[0].name
  node_group_name = "default"
  node_role_arn   = aws_iam_role.eks_node[0].arn
  subnet_ids      = aws_subnet.public[*].id
  instance_types  = ["t3.medium"]
  launch_template {
    id      = aws_launch_template.nodes[0].id
    version = aws_launch_template.nodes[0].latest_version
  }
  scaling_config {
    desired_size = 1
    min_size     = 1
    max_size     = 2
  }
  depends_on = [aws_iam_role_policy_attachment.eks_node]
}

resource "aws_security_group_rule" "eks_to_data" {
  count                    = var.enable_eks ? 1 : 0
  type                     = "ingress"
  description              = "EKS pods/nodes to Redis"
  from_port                = 6379
  to_port                  = 6379
  protocol                 = "tcp"
  security_group_id        = aws_security_group.data[0].id
  source_security_group_id = aws_eks_cluster.this[0].vpc_config[0].cluster_security_group_id
}
