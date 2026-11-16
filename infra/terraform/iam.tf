data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "instance" {
  name               = "${var.project}-instance"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

locals {
  metrics_namespace = "BookRecorder"
}

data "aws_iam_policy_document" "instance" {
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
  }

  # Recorder: PUT + GET/HEAD on raw/. Pipeline: read raw/, write curated/ and reports/
  # (curate, validate and nightly run reports). One role serves both containers, so every
  # prefix gets both actions. No DeleteObject anywhere.
  statement {
    sid     = "ReadWriteObjects"
    actions = ["s3:PutObject", "s3:GetObject"]
    resources = [
      "${aws_s3_bucket.data.arn}/raw/*",
      "${aws_s3_bucket.data.arn}/curated/*",
      "${aws_s3_bucket.data.arn}/reports/*",
    ]
  }

  # SecureStrings use the AWS-managed aws/ssm key, whose key policy already lets
  # account principals decrypt through SSM, so no kms:Decrypt statement is needed.
  statement {
    sid = "ReadOwnParameters"
    actions = [
      "ssm:GetParameter",
      "ssm:GetParameters",
      "ssm:GetParametersByPath",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter${var.ssm_prefix}",
      "arn:${data.aws_partition.current.partition}:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter${var.ssm_prefix}/*",
    ]
  }

  # Needed for the health and disk alarms: the host publishes two custom metrics.
  # PutMetricData has no resource ARN; the condition pins it to one namespace.
  statement {
    sid       = "PublishHealthMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = [local.metrics_namespace]
    }
  }
}

resource "aws_iam_role_policy" "instance" {
  name   = "${var.project}-instance"
  role   = aws_iam_role.instance.id
  policy = data.aws_iam_policy_document.instance.json
}

resource "aws_iam_instance_profile" "instance" {
  name = "${var.project}-instance"
  role = aws_iam_role.instance.name
}
