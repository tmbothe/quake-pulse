# ── IAM ──────────────────────────────────────────────────────────────────────
output "worker_role_arn" {
  description = "IAM role ARN for the worker Lambda"
  value       = aws_iam_role.earthquake_lambda.arn
}

output "dispatcher_role_arn" {
  description = "IAM role ARN for the dispatcher Lambda"
  value       = aws_iam_role.earthquake_dispatcher.arn
}

output "detector_role_arn" {
  description = "IAM role ARN for the completion detector Lambda"
  value       = aws_iam_role.earthquake_detector.arn
}

# ── Lambda functions ──────────────────────────────────────────────────────────
output "worker_function_arn" {
  description = "ARN of the worker Lambda"
  value       = aws_lambda_function.earthquake.arn
}

output "dispatcher_function_arn" {
  description = "ARN of the dispatcher Lambda"
  value       = aws_lambda_function.dispatcher.arn
}

output "detector_function_arn" {
  description = "ARN of the completion detector Lambda"
  value       = aws_lambda_function.completion_detector.arn
}

# ── Storage ───────────────────────────────────────────────────────────────────
output "dynamodb_table_name" {
  description = "DynamoDB checkpoint table name"
  value       = aws_dynamodb_table.checkpoints.name
}

output "dynamodb_stream_arn" {
  description = "DynamoDB stream ARN (consumed by completion detector)"
  value       = aws_dynamodb_table.checkpoints.stream_arn
}

output "sqs_queue_url" {
  description = "SQS page work queue URL"
  value       = aws_sqs_queue.pages.url
}

output "sqs_dlq_url" {
  description = "SQS dead-letter queue URL"
  value       = aws_sqs_queue.pages_dlq.url
}

# ── Orchestration ─────────────────────────────────────────────────────────────
output "state_machine_arn" {
  description = "Step Functions state machine ARN"
  value       = aws_sfn_state_machine.earthquake.arn
}

output "alerts_topic_arn" {
  description = "SNS topic ARN for pipeline failure alerts"
  value       = aws_sns_topic.alerts.arn
}

output "eventbridge_rule_arn" {
  description = "EventBridge daily schedule rule ARN"
  value       = aws_cloudwatch_event_rule.daily_sfn.arn
}
