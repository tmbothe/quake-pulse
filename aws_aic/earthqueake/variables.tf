variable "project" {
  description = "Project name used as a resource name prefix"
  type        = string
  default     = "openweathermap"
}

variable "environment" {
  description = "Deployment environment (e.g. dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "aws_region" {
  description = "AWS region where resources are deployed"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket" {
  description = "Name of the S3 bucket where earthquake data is stored"
  type        = string
  default     = "openweathermap-thim"
}

variable "s3_prefix" {
  description = "S3 key prefix for earthquake data"
  type        = string
  default     = "earthquake"
}

variable "s3_kms_key_arn" {
  description = "ARN of the KMS key used for SSE-KMS encryption on the S3 bucket"
  type        = string
  default     = "arn:aws:kms:us-east-1:442042510197:key/f1530ef2-bb3a-4389-868c-306793becc6d"
}

variable "page_size" {
  description = "Number of USGS events to fetch per page (max 20000)"
  type        = number
  default     = 1000
}

variable "worker_batch_size" {
  description = "Number of SQS messages processed per worker Lambda invocation (keep at 1 for isolation)"
  type        = number
  default     = 1
}

variable "worker_max_concurrency" {
  description = <<-EOT
    Maximum number of simultaneous worker Lambda invocations driven from the SQS
    queue (scaling_config.maximum_concurrency). Valid range: 2–1000.
    Set lower to protect downstream API rate limits; set higher for max throughput.
  EOT
  type        = number
  default     = 10
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30
}

variable "schedule_expression" {
  description = "EventBridge cron expression for the daily trigger"
  type        = string
  default     = "cron(0 1 * * ? *)" # 01:00 UTC every day
}

variable "tags" {
  description = "Tags applied to all resources"
  type        = map(string)
  default = {
    Project   = "openweathermap"
    ManagedBy = "terraform"
    Feed      = "earthquake"
  }
}
