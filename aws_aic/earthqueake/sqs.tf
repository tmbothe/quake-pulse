###############################################################################
# SQS – Page work queue + dead-letter queue
# maxReceiveCount = 3: three delivery attempts before routing to DLQ
###############################################################################

resource "aws_sqs_queue" "pages_dlq" {
  name                      = "${local.name_prefix}-pages-dlq"
  message_retention_seconds = 1209600 # 14 days

  tags = var.tags
}

resource "aws_sqs_queue" "pages" {
  name = "${local.name_prefix}-pages"

  # Must exceed the worker Lambda timeout (120 s) so a running invocation
  # doesn't cause a duplicate delivery before the Lambda finishes.
  visibility_timeout_seconds = 180

  message_retention_seconds = 86400 # 1 day
  receive_wait_time_seconds = 20    # long polling – reduces empty-receive cost

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.pages_dlq.arn
    maxReceiveCount     = 3
  })

  tags = var.tags
}

# Allow the worker Lambda's execution role to read from the queue
resource "aws_sqs_queue_policy" "pages" {
  queue_url = aws_sqs_queue.pages.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowWorkerLambda"
        Effect    = "Allow"
        Principal = { AWS = aws_iam_role.earthquake_lambda.arn }
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:ChangeMessageVisibility",
          "sqs:GetQueueAttributes",
        ]
        Resource = aws_sqs_queue.pages.arn
      }
    ]
  })
}
