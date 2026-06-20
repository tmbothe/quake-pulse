###############################################################################
# IAM – Earthquake pipeline
#
# Three Lambda execution roles (least-privilege):
#   earthquake_lambda      – Worker    (SQS → fetch USGS → S3 + DynamoDB)
#   earthquake_dispatcher  – Dispatcher (SQS send + DynamoDB write)
#   earthquake_detector    – Completion detector (DynamoDB read/stream + SFN signal)
#
# Shared managed policies re-used across roles:
#   cloudwatch_logs  – covers /aws/lambda/<name_prefix>*
#   kms              – GenerateDataKey + Decrypt on S3 bucket key
###############################################################################

locals {
  name_prefix = "${var.project}-${var.environment}-earthquake"
}

data "aws_caller_identity" "current" {}

# ─────────────────────────────────────────────────────────────────────────────
# Common Lambda trust policy
# ─────────────────────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    sid     = "AllowLambdaAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# Execution roles
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_iam_role" "earthquake_lambda" {
  name               = "${local.name_prefix}-worker-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role" "earthquake_dispatcher" {
  name               = "${local.name_prefix}-dispatcher-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

resource "aws_iam_role" "earthquake_detector" {
  name               = "${local.name_prefix}-detector-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

# ─────────────────────────────────────────────────────────────────────────────
# Shared: CloudWatch Logs  (covers all three Lambda log groups via wildcard)
# ─────────────────────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "cloudwatch_logs" {
  statement {
    sid    = "AllowCloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${local.name_prefix}*",
    ]
  }
}

resource "aws_iam_policy" "cloudwatch_logs" {
  name        = "${local.name_prefix}-cloudwatch-logs"
  description = "CloudWatch Logs write access for all earthquake Lambdas"
  policy      = data.aws_iam_policy_document.cloudwatch_logs.json
  tags        = var.tags
}

resource "aws_iam_role_policy_attachment" "worker_cloudwatch" {
  role       = aws_iam_role.earthquake_lambda.name
  policy_arn = aws_iam_policy.cloudwatch_logs.arn
}

resource "aws_iam_role_policy_attachment" "dispatcher_cloudwatch" {
  role       = aws_iam_role.earthquake_dispatcher.name
  policy_arn = aws_iam_policy.cloudwatch_logs.arn
}

resource "aws_iam_role_policy_attachment" "detector_cloudwatch" {
  role       = aws_iam_role.earthquake_detector.name
  policy_arn = aws_iam_policy.cloudwatch_logs.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# Shared: KMS (S3 SSE-KMS – worker writes, dispatcher doesn't touch S3)
# ─────────────────────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "kms" {
  statement {
    sid    = "AllowKMSForS3"
    effect = "Allow"
    actions = [
      "kms:GenerateDataKey",
      "kms:Decrypt",
    ]
    resources = [var.s3_kms_key_arn]
  }
}

resource "aws_iam_policy" "kms" {
  name        = "${local.name_prefix}-kms"
  description = "KMS access for S3 SSE-KMS encryption"
  policy      = data.aws_iam_policy_document.kms.json
  tags        = var.tags
}

resource "aws_iam_role_policy_attachment" "worker_kms" {
  role       = aws_iam_role.earthquake_lambda.name
  policy_arn = aws_iam_policy.kms.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# Worker: S3 write
# ─────────────────────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "s3_write" {
  statement {
    sid    = "AllowS3PutEarthquake"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:PutObjectAcl",
    ]
    resources = ["arn:aws:s3:::${var.s3_bucket}/${var.s3_prefix}/*"]
  }

  statement {
    sid       = "AllowS3ListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.s3_bucket}"]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.s3_prefix}/*"]
    }
  }
}

resource "aws_iam_policy" "s3_write" {
  name        = "${local.name_prefix}-s3-write"
  description = "Allow worker Lambda to write earthquake JSON to S3"
  policy      = data.aws_iam_policy_document.s3_write.json
  tags        = var.tags
}

resource "aws_iam_role_policy_attachment" "s3_write" {
  role       = aws_iam_role.earthquake_lambda.name
  policy_arn = aws_iam_policy.s3_write.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# Worker: SQS receive
# ─────────────────────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "sqs_receive" {
  statement {
    sid    = "AllowSQSReceive"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:ChangeMessageVisibility",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
    ]
    resources = [aws_sqs_queue.pages.arn]
  }
}

resource "aws_iam_policy" "sqs_receive" {
  name        = "${local.name_prefix}-sqs-receive"
  description = "Allow worker Lambda to consume from the pages SQS queue"
  policy      = data.aws_iam_policy_document.sqs_receive.json
  tags        = var.tags
}

resource "aws_iam_role_policy_attachment" "worker_sqs_receive" {
  role       = aws_iam_role.earthquake_lambda.name
  policy_arn = aws_iam_policy.sqs_receive.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# Worker + Dispatcher: DynamoDB read/write (checkpoint get/put/update)
# ─────────────────────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "dynamodb_readwrite" {
  statement {
    sid    = "AllowDynamoDBReadWrite"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:Query",
    ]
    resources = [aws_dynamodb_table.checkpoints.arn]
  }
}

resource "aws_iam_policy" "dynamodb_readwrite" {
  name        = "${local.name_prefix}-dynamodb-readwrite"
  description = "DynamoDB GetItem/PutItem/UpdateItem on checkpoints table"
  policy      = data.aws_iam_policy_document.dynamodb_readwrite.json
  tags        = var.tags
}

resource "aws_iam_role_policy_attachment" "worker_dynamodb" {
  role       = aws_iam_role.earthquake_lambda.name
  policy_arn = aws_iam_policy.dynamodb_readwrite.arn
}

resource "aws_iam_role_policy_attachment" "dispatcher_dynamodb" {
  role       = aws_iam_role.earthquake_dispatcher.name
  policy_arn = aws_iam_policy.dynamodb_readwrite.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# Dispatcher: SQS send
# ─────────────────────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "sqs_send" {
  statement {
    sid    = "AllowSQSSend"
    effect = "Allow"
    actions = [
      "sqs:SendMessage",
      "sqs:SendMessageBatch",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
    ]
    resources = [aws_sqs_queue.pages.arn]
  }
}

resource "aws_iam_policy" "sqs_send" {
  name        = "${local.name_prefix}-sqs-send"
  description = "Allow dispatcher Lambda to enqueue page messages"
  policy      = data.aws_iam_policy_document.sqs_send.json
  tags        = var.tags
}

resource "aws_iam_role_policy_attachment" "dispatcher_sqs_send" {
  role       = aws_iam_role.earthquake_dispatcher.name
  policy_arn = aws_iam_policy.sqs_send.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# Detector: DynamoDB read + stream
# ─────────────────────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "dynamodb_read_stream" {
  statement {
    sid    = "AllowDynamoDBRead"
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:Query",
    ]
    resources = [aws_dynamodb_table.checkpoints.arn]
  }

  statement {
    sid    = "AllowDynamoDBStream"
    effect = "Allow"
    actions = [
      "dynamodb:GetRecords",
      "dynamodb:GetShardIterator",
      "dynamodb:DescribeStream",
      "dynamodb:ListStreams",
    ]
    resources = [aws_dynamodb_table.checkpoints.stream_arn]
  }
}

resource "aws_iam_policy" "dynamodb_read_stream" {
  name        = "${local.name_prefix}-dynamodb-read-stream"
  description = "Allow detector Lambda to read checkpoints and consume the DynamoDB stream"
  policy      = data.aws_iam_policy_document.dynamodb_read_stream.json
  tags        = var.tags
}

resource "aws_iam_role_policy_attachment" "detector_dynamodb" {
  role       = aws_iam_role.earthquake_detector.name
  policy_arn = aws_iam_policy.dynamodb_read_stream.arn
}

# ─────────────────────────────────────────────────────────────────────────────
# Detector: Step Functions task signal
# ─────────────────────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "sfn_signal" {
  statement {
    sid    = "AllowSFNSignal"
    effect = "Allow"
    actions = [
      "states:SendTaskSuccess",
      "states:SendTaskFailure",
    ]
    resources = ["*"] # task tokens don't map to a specific resource ARN
  }
}

resource "aws_iam_policy" "sfn_signal" {
  name        = "${local.name_prefix}-sfn-signal"
  description = "Allow detector Lambda to signal Step Functions task completion"
  policy      = data.aws_iam_policy_document.sfn_signal.json
  tags        = var.tags
}

resource "aws_iam_role_policy_attachment" "detector_sfn_signal" {
  role       = aws_iam_role.earthquake_detector.name
  policy_arn = aws_iam_policy.sfn_signal.arn
}
