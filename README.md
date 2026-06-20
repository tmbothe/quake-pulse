# Quake Pulse — Serverless Earthquake Ingestion Pipeline on AWS

> A production-grade, fully serverless data ingestion pipeline that continuously harvests global earthquake data from the USGS Earthquake Hazards Program API into Amazon S3, orchestrated by AWS Step Functions and deployed entirely through Terraform.

[![Terraform](https://img.shields.io/badge/IaC-Terraform%20%E2%89%A51.5-7B42BC?logo=terraform)](https://www.terraform.io/)
[![Python](https://img.shields.io/badge/Runtime-Python%203.12-3776AB?logo=python)](https://www.python.org/)
[![AWS](https://img.shields.io/badge/Cloud-AWS-FF9900?logo=amazonaws)](https://aws.amazon.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Step Functions Workflow](#step-functions-workflow)
- [Project Structure](#project-structure)
- [Components](#components)
- [Key Design Decisions](#key-design-decisions)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Cost Estimate](#cost-estimate)
- [Author](#author)

---

## Overview

**Quake Pulse** ingests paginated earthquake event data from the [USGS FDSN Web Service](https://earthquake.usgs.gov/fdsnws/event/1/) into Amazon S3 in Hive-style partitioned GeoJSON format, making it immediately queryable by Amazon Athena or AWS Glue.

Key properties:

| Property | Value |
|---|---|
| **Compute** | 100% serverless — Lambda, Step Functions, EventBridge |
| **Storage** | S3 (`year=YYYY/month=MM/day=DD/` partitions, SSE-KMS) |
| **Orchestration** | Step Functions `waitForTaskToken` callback pattern |
| **Concurrency** | Up to 10 parallel worker Lambdas (configurable) |
| **Idempotency** | Per-page DynamoDB checkpoint — safe to re-run anytime |
| **Fault tolerance** | 3-layer retry: Lambda backoff → SQS redrive → DLQ |
| **IaC** | 100% Terraform (AWS, archive, null providers) |
| **Cost** | ~$0.01–$0.02/month at steady state |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          QUAKE PULSE PIPELINE                               │
├──────────────┬──────────────────┬─────────────────────────┬────────────────┤
│   TRIGGER    │    DISPATCH      │  PROCESS (concurrent)   │   COMPLETE     │
│              │                  │                         │                │
│  EventBridge │  Step Functions  │  SQS Work Queue         │  Detector λ    │
│  cron rule   │  Standard        │  ┌──────────────┐       │                │
│  01:00 UTC   │  Workflow        │  │ Worker λ #1  │──► S3 │  DynamoDB      │
│      │       │  waitForToken    │  │ Worker λ #2  │──► S3 │  stream        │
│      │       │      │           │  │ Worker λ … N │──► S3 │  filter        │
│      ▼       │      ▼           │  └──────────────┘       │      │         │
│  StartExec   │  Dispatcher λ    │  max 10 concurrent      │      ▼         │
│              │  /count per day  │                         │  SendTask      │
│              │  fan-out to SQS  │  DynamoDB Checkpoint    │  Success/Fail  │
│              │                  │  RUNNING→SUCCESS/FAILED │                │
│              │                  │                         │  SNS alert     │
│              │                  │  KMS-encrypted S3 write │  on failure    │
└──────────────┴──────────────────┴─────────────────────────┴────────────────┘
```

### AWS Services Used

| Service | Role |
|---|---|
| **EventBridge** | Daily cron trigger at 01:00 UTC |
| **Step Functions** | Workflow orchestration, `waitForTaskToken` callback |
| **Lambda (Dispatcher)** | Splits window by day, queries USGS `/count`, fans out SQS messages |
| **Lambda (Worker)** | Fetches one USGS page, writes GeoJSON to S3, updates DynamoDB checkpoint |
| **Lambda (Detector)** | Reacts to DynamoDB stream, signals Step Functions when all pages complete |
| **SQS** | Decouples dispatcher from workers; enables horizontal auto-scaling |
| **SQS DLQ** | Captures pages that failed all 3 delivery attempts for investigation |
| **DynamoDB** | Run metadata + per-page checkpoint state machine; streams to detector |
| **S3** | Final storage in Hive-partitioned GeoJSON (SSE-KMS encrypted) |
| **KMS** | Customer-managed key for S3 SSE-KMS encryption |
| **SNS** | Failure alerting (email, PagerDuty, Slack webhook) |
| **CloudWatch Logs** | Structured logs for all three Lambdas, 30-day retention |
| **IAM** | Three least-privilege execution roles, one per Lambda |

---

## Data Flow

```
 EventBridge (01:00 UTC)
       │
       │ StartExecution({})
       ▼
 Step Functions ──► Dispatch state (waitForTaskToken)
       │
       │ invoke(taskToken, execTime, input)
       ▼
 Dispatcher Lambda
   1. Atomically increment DynamoDB run counter → runId=000001
   2. Split [starttime, endtime] into per-day ranges
   3. Call USGS /count once per day (lightweight endpoint)
   4. Write RUN#000001|META to DynamoDB (taskToken, totalPages)
   5. Fan out 1 SQS message per (day × page)
       │
       │ SendMessageBatch (10 per API call)
       ▼
 SQS Work Queue  ◄────────── Dead-Letter Queue (after 3 failures)
       │
       │ (up to 10 concurrent consumers)
       ▼
 Worker Lambda × N
   1. Read DynamoDB checkpoint → skip if already SUCCESS (idempotent)
   2. Mark RUNNING
   3. Fetch USGS /query with retry + exponential backoff
   4. Write GeoJSON to S3:
      earthquake/year=2025/month=06/day=15/run=000001/fetch/page=00001.json
   5. Mark SUCCESS (triggers DynamoDB stream)
       │
       │ DynamoDB stream (NEW_AND_OLD_IMAGES, filter: PAGE# + terminal)
       ▼
 Completion Detector Lambda
   1. Read META → totalPages, taskToken
   2. Query all PAGE# checkpoints
   3. Count terminals (SUCCESS + FAILED)
   4. If terminals == totalPages:
      - All SUCCESS → sfn.send_task_success()  → RunComplete
      - Any FAILED  → sfn.send_task_failure()  → AlertOnFailure → SNS → RunFailed
```

---

## Step Functions Workflow

```
                    ┌─────────────────────────┐
                    │         START           │
                    └────────────┬────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │        Dispatch         │
                    │  (waitForTaskToken)     │  ◄── Dispatcher Lambda fans out
                    │   TimeoutSeconds=86400  │       pages to SQS, then waits
                    └────────┬───────┬────────┘       for Detector to signal back
                             │       │
                     success │       │ catch(States.ALL)
                             │       │
              ┌──────────────▼──┐  ┌─▼──────────────────┐
              │   RunComplete   │  │   AlertOnFailure    │
              │    (Succeed)    │  │  SNS.publish(error) │
              └─────────────────┘  └────────┬────────────┘
                                            │
                                   ┌────────▼────────┐
                                   │    RunFailed    │
                                   │     (Fail)      │
                                   └─────────────────┘
```

**Callback pattern:** The Dispatcher embeds `taskToken` in every SQS message. Workers forward it to the Detector via DynamoDB (META record). When the last page completes, the Detector calls `SendTaskSuccess`/`SendTaskFailure` to resume the paused Step Functions execution — no polling required.

---

## Project Structure

```
quake-pulse/
├── feeds/
│   └── earthqueake/
│       ├── dispatcher_lambda/
│       │   └── lambda_function.py        # Fan-out: /count per day → SQS
│       ├── earthqueake_lambda_function/
│       │   ├── lambda_function.py        # Worker: fetch page → S3
│       │   └── requirements.txt          # requests==2.32.3
│       └── completion_detector_lambda/
│           └── lambda_function.py        # Detector: stream → SFN signal
└── aws_aic/
    └── earthqueake/
        ├── versions.tf                   # Provider + Terraform version pins
        ├── variables.tf                  # All input variables with defaults
        ├── main.tf                       # IAM roles & least-privilege policies
        ├── lambda.tf                     # Lambda functions + event source mappings
        ├── sqs.tf                        # Work queue + dead-letter queue
        ├── dynamodb.tf                   # Checkpoint table + streams + PITR
        ├── stepfunctions.tf              # State machine + EventBridge rule + SNS
        └── outputs.tf                    # ARNs and URLs for downstream use
```

---

## Components

### Dispatcher Lambda (`dispatcher_lambda/lambda_function.py`)

- Receives `taskToken` from Step Functions
- **Atomically increments** a DynamoDB counter (`pk=COUNTER/sk=RUN_SEQ`) for a unique sequential `runId` (e.g. `000001`)
- Splits the time window into per-day ranges and calls USGS `/count` per day
- Fans out one SQS message per `(day, page)` pair — each message carries day-specific `starttime`/`endtime` for accurate S3 partitioning
- **Resume mode**: pass `runId` to reuse an existing run ID; automatically skips already-`SUCCESS` pages; optional `fromPage` to skip earlier pages

### Worker Lambda (`earthqueake_lambda_function/lambda_function.py`)

- Triggered by SQS (batch size = 1, `ReportBatchItemFailures`)
- **Idempotent**: reads DynamoDB checkpoint first; returns immediately if `status == SUCCESS`
- Calls USGS `/query` with exponential backoff (3 attempts, 1s/2s/4s delays)
- Writes enriched GeoJSON to S3 with `_meta` block (`runId`, `featureCount`, `ingestTimestamp`)
- S3 key derived from the **data date** (query `starttime`), not the Lambda execution timestamp

### Completion Detector Lambda (`completion_detector_lambda/lambda_function.py`)

- Triggered by DynamoDB stream with server-side filter (`sk BEGINS_WITH PAGE#` AND `status IN [SUCCESS, FAILED]`)
- Reads `totalPages` and `taskToken` from the run's META record
- Paginates through all `PAGE#` checkpoints to count terminals
- Signals Step Functions exactly once when `terminals == totalPages`

### Terraform Modules

| File | Resources |
|---|---|
| `main.tf` | 3 IAM roles, 8 policies (least-privilege per service) |
| `lambda.tf` | 3 Lambda functions, Lambda Layer (requests), 2 ESMs, 3 log groups |
| `sqs.tf` | Work queue (180s visibility), DLQ (14-day retention) |
| `dynamodb.tf` | Checkpoint table (PAY_PER_REQUEST, streams, PITR, TTL) |
| `stepfunctions.tf` | State machine, SNS topic, EventBridge rule + target |
| `outputs.tf` | 13 output values (ARNs, URLs, table name) |

---

## Key Design Decisions

### Idempotency
Every worker reads the DynamoDB checkpoint before doing any work. A `SUCCESS` record short-circuits the invocation. Safe for SQS duplicate delivery, Lambda retries, and full pipeline re-runs.

### Day-Level S3 Partitioning
The dispatcher splits any time window into per-day sub-windows and dispatches day-specific `starttime`/`endtime` in each SQS message. The worker derives the S3 partition from the message's `starttime`, never from `datetime.now()`. A 3-year backfill run in 2026 still writes to `year=2024/...`.

### Paginated Fan-Out with Concurrency Cap
SQS decouples dispatch from processing. `scaling_config.maximum_concurrency = 10` on the SQS event source mapping caps concurrent workers without touching the account Lambda concurrency limit, protecting against USGS API rate limits.

### waitForTaskToken Callback
The Step Functions workflow pauses in the `Dispatch` state indefinitely (up to 24 hours). The Detector Lambda signals completion via `SendTaskSuccess`/`SendTaskFailure` when the last DynamoDB stream event arrives. Zero polling — cost is exactly $0 while waiting.

### Sequential Run IDs
DynamoDB atomic `ADD` counter produces `000001`, `000002`, … — human-readable in CloudWatch Logs and S3 paths, lexicographically sortable, race-condition safe.

### Three-Layer Retry
1. Lambda: 3 USGS fetch attempts with exponential backoff
2. SQS: `maxReceiveCount=3` — message re-queued on Lambda error
3. DLQ: failed messages held 14 days for manual inspection and replay

---

## Getting Started

### Prerequisites

- AWS CLI configured with appropriate permissions
- Terraform ≥ 1.5
- Python 3.12 + pip (for local layer build)

### Deploy

```bash
cd aws_aic/earthqueake

# Initialise providers
terraform init

# Review the plan
terraform plan \
  -var="s3_kms_key_arn=arn:aws:kms:us-east-1:YOUR_ACCOUNT:key/YOUR_KEY_ID"

# Apply
terraform apply \
  -var="s3_kms_key_arn=arn:aws:kms:us-east-1:YOUR_ACCOUNT:key/YOUR_KEY_ID"
```

### Key Variables

| Variable | Default | Description |
|---|---|---|
| `s3_bucket` | `openweathermap-thim` | Destination S3 bucket |
| `s3_kms_key_arn` | *(required)* | CMK ARN for S3 SSE-KMS |
| `page_size` | `1000` | Events per USGS page (max 20,000) |
| `worker_max_concurrency` | `10` | Max concurrent worker Lambdas (2–1000) |
| `log_retention_days` | `30` | CloudWatch log retention |
| `schedule_expression` | `cron(0 1 * * ? *)` | EventBridge trigger schedule |

---

## Usage

### Automated Daily Run

The EventBridge rule fires every day at 01:00 UTC, starting a Step Functions execution that fetches the previous day's earthquake data automatically.

### Manual Backfill

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:ACCOUNT:stateMachine:openweathermap-prod-earthquake-pipeline \
  --input '{
    "starttime": "2024-01-01T00:00:00",
    "endtime":   "2024-12-31T23:59:59"
  }'
```

### Resume a Failed Run

```bash
# Resume automatically (skips all SUCCESS pages)
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:ACCOUNT:stateMachine:openweathermap-prod-earthquake-pipeline \
  --input '{
    "runId":     "000003",
    "starttime": "2024-01-01T00:00:00",
    "endtime":   "2024-12-31T23:59:59"
  }'

# Resume from a specific page
aws stepfunctions start-execution \
  --state-machine-arn ... \
  --input '{
    "runId":     "000003",
    "fromPage":  5,
    "starttime": "2024-06-01T00:00:00",
    "endtime":   "2024-06-30T23:59:59"
  }'
```

### Check Run Status

```bash
# List all checkpoints for a run
aws dynamodb query \
  --table-name openweathermap-prod-earthquake-checkpoints \
  --key-condition-expression "pk = :pk" \
  --expression-attribute-values '{":pk": {"S": "RUN#000001"}}' \
  --query 'Items[*].{sk: sk.S, status: status.S}'
```

### S3 Output Layout

```
s3://openweathermap-thim/
└── earthquake/
    └── year=2024/
        └── month=06/
            └── day=15/
                └── run=000001/
                    └── fetch/
                        ├── page=00001.json
                        ├── page=00002.json
                        └── ...
```

Each file is enriched GeoJSON with a `_meta` block:

```json
{
  "type": "FeatureCollection",
  "features": [ ... ],
  "_meta": {
    "runId": "000001",
    "phase": "fetch",
    "page": 1,
    "offset": 1,
    "featureCount": 847,
    "ingestTimestamp": "2024-06-15T01:03:22.456789+00:00",
    "source": "https://earthquake.usgs.gov/fdsnws/event/1/query"
  }
}
```

---

## Cost Estimate

| Scenario | Estimated Cost |
|---|---|
| One-time 3-year backfill (1,096 days, ~548 pages) | ~$0.32 |
| Monthly steady state (30 daily runs, 1 page/day) | ~$0.01–$0.02 |
| Annual operating cost | ~$0.12–$0.24 |

> Lambda, DynamoDB, and S3 free tiers cover all usage at this scale — real-world cost may be $0.00 for compute.
> KMS CMK incurs a flat $1/month key maintenance fee (not included above).

---

## Author

**Thierno Mbodj**
GitHub: [github.com/tmbothe](https://github.com/tmbothe)
