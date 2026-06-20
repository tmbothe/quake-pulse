###############################################################################
# DynamoDB – Checkpoint table
# PK: pk  (String)  e.g. "RUN#<runId>"
# SK: sk  (String)  "META" | "PAGE#<phase>#<page>"
###############################################################################

resource "aws_dynamodb_table" "checkpoints" {
  name         = "${local.name_prefix}-checkpoints"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  # Streams drive the completion detector Lambda
  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  point_in_time_recovery {
    enabled = true
  }

  # AWS-managed encryption (no extra KMS IAM permissions needed)
  server_side_encryption {
    enabled = true
  }

  # Auto-expire old run records after 7 days (workers set ttl attribute)
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = var.tags
}
