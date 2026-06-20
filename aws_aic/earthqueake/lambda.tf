###############################################################################
# Lambda – Earthquake pipeline (worker + dispatcher + completion detector)
###############################################################################

locals {
  src_root        = "${path.module}/../../feeds/earthqueake"
  worker_src_dir  = "${local.src_root}/earthqueake_lambda_function"
  layer_zip_path  = "${local.worker_src_dir}/dist/layer.zip"
}

# ─────────────────────────────────────────────────────────────────────────────
# Shared Lambda Layer – pip-installed dependencies (requests)
# ─────────────────────────────────────────────────────────────────────────────
resource "null_resource" "pip_install" {
  triggers = {
    requirements = filemd5("${local.worker_src_dir}/requirements.txt")
  }

  provisioner "local-exec" {
    interpreter = ["PowerShell", "-Command"]
    command     = <<-EOT
      New-Item -ItemType Directory -Force -Path "${local.worker_src_dir}/dist/python" | Out-Null;
      pip install -r "${local.worker_src_dir}/requirements.txt" -t "${local.worker_src_dir}/dist/python" --quiet
    EOT
  }
}

data "archive_file" "layer" {
  depends_on  = [null_resource.pip_install]
  type        = "zip"
  source_dir  = "${local.worker_src_dir}/dist"
  output_path = local.layer_zip_path
}

resource "aws_lambda_layer_version" "requests" {
  filename            = data.archive_file.layer.output_path
  source_code_hash    = data.archive_file.layer.output_base64sha256
  layer_name          = "${local.name_prefix}-deps"
  compatible_runtimes = ["python3.12"]
  description         = "requests library shared by worker and dispatcher"
}

# ─────────────────────────────────────────────────────────────────────────────
# Source archives
# ─────────────────────────────────────────────────────────────────────────────
data "archive_file" "worker" {
  type        = "zip"
  source_file = "${local.worker_src_dir}/lambda_function.py"
  output_path = "${local.worker_src_dir}/dist/worker.zip"
}

data "archive_file" "dispatcher" {
  type        = "zip"
  source_file = "${local.src_root}/dispatcher_lambda/lambda_function.py"
  output_path = "${local.src_root}/dispatcher_lambda/dist/dispatcher.zip"
}

data "archive_file" "completion_detector" {
  type        = "zip"
  source_file = "${local.src_root}/completion_detector_lambda/lambda_function.py"
  output_path = "${local.src_root}/completion_detector_lambda/dist/detector.zip"
}

# ─────────────────────────────────────────────────────────────────────────────
# CloudWatch Log Groups (pre-created with retention)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_cloudwatch_log_group" "worker" {
  name              = "/aws/lambda/${local.name_prefix}-worker"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "dispatcher" {
  name              = "/aws/lambda/${local.name_prefix}-dispatcher"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_cloudwatch_log_group" "detector" {
  name              = "/aws/lambda/${local.name_prefix}-detector"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

# ─────────────────────────────────────────────────────────────────────────────
# 1. Worker Lambda  (SQS-triggered, reads checkpoint → fetch USGS → S3)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_lambda_function" "earthquake" {
  function_name    = "${local.name_prefix}-worker"
  description      = "Fetches one USGS page, writes to S3, updates DynamoDB checkpoint"
  role             = aws_iam_role.earthquake_lambda.arn
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  filename         = data.archive_file.worker.output_path
  source_code_hash = data.archive_file.worker.output_base64sha256
  timeout          = 120
  memory_size      = 256
  layers           = [aws_lambda_layer_version.requests.arn]

  environment {
    variables = {
      S3_BUCKET      = var.s3_bucket
      S3_PREFIX      = var.s3_prefix
      DYNAMODB_TABLE = aws_dynamodb_table.checkpoints.name
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.worker_kms,
    aws_iam_role_policy_attachment.s3_write,
    aws_iam_role_policy_attachment.worker_cloudwatch,
    aws_iam_role_policy_attachment.worker_dynamodb,
    aws_iam_role_policy_attachment.worker_sqs_receive,
    aws_cloudwatch_log_group.worker,
  ]

  tags = var.tags
}

# SQS → Worker trigger
# batch_size=1          → one page per Lambda invocation (maximum isolation)
# ReportBatchItemFailures → only failed messages are retried, not the whole batch
# scaling_config        → caps simultaneous worker Lambdas; Lambda auto-scales up
#                         to this limit as the queue fills (min 2, max 1000)
resource "aws_lambda_event_source_mapping" "sqs_worker" {
  event_source_arn        = aws_sqs_queue.pages.arn
  function_name           = aws_lambda_function.earthquake.arn
  batch_size              = var.worker_batch_size
  function_response_types = ["ReportBatchItemFailures"]

  scaling_config {
    maximum_concurrency = var.worker_max_concurrency
  }
}

# ─────────────────────────────────────────────────────────────────────────────
# 2. Dispatcher Lambda  (Step Functions invokes, fans out pages to SQS)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_lambda_function" "dispatcher" {
  function_name    = "${local.name_prefix}-dispatcher"
  description      = "Queries USGS total count, stores run metadata, fans out SQS messages"
  role             = aws_iam_role.earthquake_dispatcher.arn
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  filename         = data.archive_file.dispatcher.output_path
  source_code_hash = data.archive_file.dispatcher.output_base64sha256
  timeout          = 300
  memory_size      = 256
  layers           = [aws_lambda_layer_version.requests.arn]

  environment {
    variables = {
      SQS_QUEUE_URL  = aws_sqs_queue.pages.url
      DYNAMODB_TABLE = aws_dynamodb_table.checkpoints.name
      PAGE_SIZE      = tostring(var.page_size)
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.dispatcher_cloudwatch,
    aws_iam_role_policy_attachment.dispatcher_dynamodb,
    aws_iam_role_policy_attachment.dispatcher_sqs_send,
    aws_cloudwatch_log_group.dispatcher,
  ]

  tags = var.tags
}

# ─────────────────────────────────────────────────────────────────────────────
# 3. Completion Detector Lambda  (DynamoDB stream → signals Step Functions)
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_lambda_function" "completion_detector" {
  function_name    = "${local.name_prefix}-detector"
  description      = "Reacts to DynamoDB checkpoint writes, signals Step Functions on run completion"
  role             = aws_iam_role.earthquake_detector.arn
  runtime          = "python3.12"
  handler          = "lambda_function.lambda_handler"
  filename         = data.archive_file.completion_detector.output_path
  source_code_hash = data.archive_file.completion_detector.output_base64sha256
  timeout          = 60
  memory_size      = 128

  environment {
    variables = {
      DYNAMODB_TABLE = aws_dynamodb_table.checkpoints.name
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.detector_cloudwatch,
    aws_iam_role_policy_attachment.detector_dynamodb,
    aws_iam_role_policy_attachment.detector_sfn_signal,
    aws_cloudwatch_log_group.detector,
  ]

  tags = var.tags
}

# DynamoDB stream → Detector trigger
# Filter: only PAGE# records that just reached a terminal state
resource "aws_lambda_event_source_mapping" "dynamodb_stream" {
  event_source_arn  = aws_dynamodb_table.checkpoints.stream_arn
  function_name     = aws_lambda_function.completion_detector.arn
  starting_position = "LATEST"
  batch_size        = 100
  bisect_batch_on_function_error = true

  filter_criteria {
    filter {
      pattern = jsonencode({
        dynamodb = {
          NewImage = {
            sk     = { S = [{ prefix = "PAGE#" }] }
            status = { S = ["SUCCESS", "FAILED"] }
          }
        }
      })
    }
  }
}
