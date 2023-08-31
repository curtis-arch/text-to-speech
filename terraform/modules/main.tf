locals {
  resource_prefix = "${var.namespace}-${var.project}-${terraform.workspace}"
}

resource "aws_s3_bucket" "default" {
  bucket = "${local.resource_prefix}-input"

  tags = var.tags
}

resource "aws_s3_bucket_public_access_block" "default" {
  bucket = aws_s3_bucket.default.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_policy" "default" {
  bucket = aws_s3_bucket.default.id
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
	  "Principal": "*",
      "Action": [ "s3:PutObject" ],
      "Resource": [
        "arn:aws:s3:::${aws_s3_bucket.default.bucket}/inputz/*"
      ]
    }
  ]
}
EOF
}

resource "aws_sqs_queue" "status" {
  name                       = "${local.resource_prefix}-status"
  visibility_timeout_seconds = 15
  message_retention_seconds  = 3600
  redrive_policy             = jsonencode({
    deadLetterTargetArn      = aws_sqs_queue.dlq.arn
    maxReceiveCount          = 20
  })

  tags                       = var.tags
}

resource "aws_sqs_queue" "download" {
  name                       = "${local.resource_prefix}-download"
  message_retention_seconds  = 600
  redrive_policy             = jsonencode({
    deadLetterTargetArn      = aws_sqs_queue.dlq.arn
    maxReceiveCount          = 5
  })

  tags                       = var.tags
}

resource "aws_sqs_queue" "dlq" {
  name = "${local.resource_prefix}-dlq"

  tags = var.tags
}
