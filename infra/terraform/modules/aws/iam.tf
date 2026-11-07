# Least-privilege IAM:
#  - snowflake_lake: assumed by Snowflake's storage integration (external-id bound),
#    read-only on the lake's offline/ prefix.
#  - app: policy for platform jobs (batch, training, serving) on lake + MLflow,
#    attached to a role assumable by ECS tasks for a future in-VPC deployment.

data "aws_iam_policy_document" "snowflake_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.snowflake_iam_user_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.snowflake_external_id]
    }
  }
}

resource "aws_iam_role" "snowflake_lake" {
  name               = var.snowflake_role_name
  assume_role_policy = data.aws_iam_policy_document.snowflake_trust.json
}

data "aws_iam_policy_document" "snowflake_lake" {
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.this["lake"].arn}/offline/*"]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.this["lake"].arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["offline/*"]
    }
  }
}

resource "aws_iam_role_policy" "snowflake_lake" {
  name   = "lake-offline-read"
  role   = aws_iam_role.snowflake_lake.id
  policy = data.aws_iam_policy_document.snowflake_lake.json
}

data "aws_iam_policy_document" "app" {
  statement {
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.this["lake"].arn}/*",
      "${aws_s3_bucket.this["mlflow"].arn}/*",
    ]
  }
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.this["lake"].arn, aws_s3_bucket.this["mlflow"].arn]
  }
}

resource "aws_iam_policy" "app" {
  name        = "${var.prefix}-app"
  description = "Recsys platform jobs: read/write the lake and MLflow artifacts."
  policy      = data.aws_iam_policy_document.app.json
}

data "aws_iam_policy_document" "ecs_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "app" {
  name               = "${var.prefix}-app"
  assume_role_policy = data.aws_iam_policy_document.ecs_trust.json
}

resource "aws_iam_role_policy_attachment" "app" {
  role       = aws_iam_role.app.name
  policy_arn = aws_iam_policy.app.arn
}
