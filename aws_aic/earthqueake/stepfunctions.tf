###############################################################################
# Step Functions – Earthquake ingestion pipeline
#
# Flow:
#   EventBridge (daily cron) → StartExecution
#   → Dispatch (waitForTaskToken) ──────────── dispatcher Lambda fans out SQS
#   → workers drain queue, update DynamoDB
#   → completion detector fires on stream, calls SendTaskSuccess / SendTaskFailure
#   → RunComplete  |  AlertOnFailure → RunFailed
###############################################################################

# ─────────────────────────────────────────────────────────────────────────────
# SNS – alert topic for pipeline failures
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_sns_topic" "alerts" {
  name = "${local.name_prefix}-alerts"
  tags = var.tags
}

# ─────────────────────────────────────────────────────────────────────────────
# Step Functions IAM role
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_iam_role" "stepfunctions" {
  name = "${local.name_prefix}-sfn-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowSFNAssumeRole"
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "states.amazonaws.com" }
      Condition = {
        StringEquals = {
          "aws:SourceAccount" = data.aws_caller_identity.current.account_id
        }
      }
    }]
  })
  tags = var.tags
}

resource "aws_iam_role_policy" "stepfunctions" {
  name = "${local.name_prefix}-sfn-policy"
  role = aws_iam_role.stepfunctions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "InvokeDispatcher"
        Effect = "Allow"
        Action = ["lambda:InvokeFunction"]
        Resource = [
          aws_lambda_function.dispatcher.arn,
          "${aws_lambda_function.dispatcher.arn}:*",
        ]
      },
      {
        Sid      = "PublishAlerts"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.alerts.arn]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups",
        ]
        Resource = ["*"]
      }
    ]
  })
}

# ─────────────────────────────────────────────────────────────────────────────
# State machine
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/states/${local.name_prefix}-pipeline"
  retention_in_days = var.log_retention_days
  tags              = var.tags
}

resource "aws_sfn_state_machine" "earthquake" {
  name     = "${local.name_prefix}-pipeline"
  role_arn = aws_iam_role.stepfunctions.arn

  definition = jsonencode({
    Comment = "Earthquake paginated ingestion: dispatcher fans out SQS, detector signals completion"
    StartAt = "Dispatch"
    States = {
      Dispatch = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke.waitForTaskToken"
        Parameters = {
          FunctionName = aws_lambda_function.dispatcher.arn
          Payload = {
            # Pass the task token and execution start time (used as default runId).
            # The entire input is forwarded as-is so the dispatcher can read
            # any optional overrides (runId, starttime, endtime, pageSize)
            # without the state machine failing on missing JSONPath references.
            "taskToken.$" = "$$.Task.Token"
            "execTime.$"  = "$$.Execution.StartTime"
            "input.$"     = "$"
          }
        }
        # 24 h window – safe maximum for any realistic page count
        TimeoutSeconds = 86400
        ResultPath     = "$.dispatchResult"
        Catch = [
          {
            ErrorEquals = ["RunFailed"]
            Next        = "AlertOnFailure"
            ResultPath  = "$.error"
          },
          {
            ErrorEquals = ["States.ALL"]
            Next        = "AlertOnFailure"
            ResultPath  = "$.error"
          }
        ]
        Next = "RunComplete"
      }

      AlertOnFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.alerts.arn
          "Message.$" = "States.JsonToString($.error)"
          Subject     = "Earthquake pipeline FAILED"
        }
        Next = "RunFailed"
      }

      RunComplete = {
        Type = "Succeed"
      }

      RunFailed = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "One or more pages failed after all SQS retries"
      }
    }
  })

  logging_configuration {
    level                  = "ERROR"
    include_execution_data = true
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
  }

  tags = var.tags
}

# ─────────────────────────────────────────────────────────────────────────────
# EventBridge – daily cron → Step Functions
# ─────────────────────────────────────────────────────────────────────────────
resource "aws_iam_role" "eventbridge_sfn" {
  name = "${local.name_prefix}-eventbridge-sfn-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "events.amazonaws.com" }
    }]
  })
  tags = var.tags
}

resource "aws_iam_role_policy" "eventbridge_sfn" {
  name = "${local.name_prefix}-eventbridge-sfn-policy"
  role = aws_iam_role.eventbridge_sfn.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = [aws_sfn_state_machine.earthquake.arn]
    }]
  })
}

resource "aws_cloudwatch_event_rule" "daily_sfn" {
  name                = "${local.name_prefix}-daily"
  description         = "Daily trigger for earthquake ingestion pipeline via Step Functions"
  schedule_expression = var.schedule_expression
  tags                = var.tags
}

resource "aws_cloudwatch_event_target" "sfn" {
  rule     = aws_cloudwatch_event_rule.daily_sfn.name
  arn      = aws_sfn_state_machine.earthquake.arn
  role_arn = aws_iam_role.eventbridge_sfn.arn

  # Pass event time as runId; starttime/endtime are empty – dispatcher computes them
  input_transformer {
    input_paths = {
      time = "$.time"
    }
    input_template = "{\"runId\":\"<time>\",\"starttime\":\"\",\"endtime\":\"\",\"pageSize\":${var.page_size}}"
  }
}
