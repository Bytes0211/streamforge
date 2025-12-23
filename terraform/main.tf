# StreamForge AWS Infrastructure
# Terraform configuration for production deployment

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  backend "s3" {
    bucket         = "streamforge-terraform-state"
    key            = "streamforge/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "streamforge-terraform-locks"
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "StreamForge"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# Variables
variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "streamforge"
}

# DynamoDB Table for Processed Data
resource "aws_dynamodb_table" "processed_data" {
  name           = "${var.project_name}-processed-data-${var.environment}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "id"
  range_key      = "timestamp"
  
  attribute {
    name = "id"
    type = "S"
  }
  
  attribute {
    name = "timestamp"
    type = "N"
  }
  
  attribute {
    name = "userId"
    type = "S"
  }
  
  global_secondary_index {
    name            = "UserIdIndex"
    hash_key        = "userId"
    range_key       = "timestamp"
    projection_type = "ALL"
  }
  
  point_in_time_recovery {
    enabled = true
  }
  
  tags = {
    Name = "${var.project_name}-processed-data"
  }
}

# DynamoDB Table for Aggregated Metrics
resource "aws_dynamodb_table" "aggregated_metrics" {
  name           = "${var.project_name}-aggregated-metrics-${var.environment}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "userId"
  range_key      = "windowStart"
  
  attribute {
    name = "userId"
    type = "S"
  }
  
  attribute {
    name = "windowStart"
    type = "N"
  }
  
  attribute {
    name = "eventType"
    type = "S"
  }
  
  global_secondary_index {
    name            = "EventTypeIndex"
    hash_key        = "eventType"
    range_key       = "windowStart"
    projection_type = "ALL"
  }
  
  point_in_time_recovery {
    enabled = true
  }
  
  tags = {
    Name = "${var.project_name}-aggregated-metrics"
  }
}

# DynamoDB Table for Dead Letter Queue
resource "aws_dynamodb_table" "dead_letter_queue" {
  name           = "${var.project_name}-dlq-${var.environment}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "id"
  range_key      = "failedAt"
  
  attribute {
    name = "id"
    type = "S"
  }
  
  attribute {
    name = "failedAt"
    type = "N"
  }
  
  ttl {
    attribute_name = "expiresAt"
    enabled        = true
  }
  
  tags = {
    Name = "${var.project_name}-dlq"
  }
}

# S3 Bucket for Flink Checkpoints
resource "aws_s3_bucket" "flink_checkpoints" {
  bucket = "${var.project_name}-flink-checkpoints-${var.environment}"
  
  tags = {
    Name = "${var.project_name}-flink-checkpoints"
  }
}

resource "aws_s3_bucket_versioning" "flink_checkpoints" {
  bucket = aws_s3_bucket.flink_checkpoints.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "flink_checkpoints" {
  bucket = aws_s3_bucket.flink_checkpoints.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# IAM Assume Role Policy Document
data "aws_iam_policy_document" "flink_assume_role" {
  statement {
    effect = "Allow"
    
    principals {
      type        = "Service"
      identifiers = [
        "kinesisanalytics.amazonaws.com"
      ]
    }
    
    actions = ["sts:AssumeRole"]
  }
}

# IAM Role for Flink Application
resource "aws_iam_role" "flink_app" {
  name = "${var.project_name}-flink-role-${var.environment}"
  
  assume_role_policy = data.aws_iam_policy_document.flink_assume_role.json
  
  tags = {
    Name = "${var.project_name}-flink-role"
  }
}

# IAM Policy for Flink Application
resource "aws_iam_role_policy" "flink_app" {
  name = "${var.project_name}-flink-policy"
  role = aws_iam_role.flink_app.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.flink_checkpoints.arn,
          "${aws_s3_bucket.flink_checkpoints.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:BatchWriteItem"
        ]
        Resource = [
          aws_dynamodb_table.processed_data.arn,
          aws_dynamodb_table.aggregated_metrics.arn,
          aws_dynamodb_table.dead_letter_queue.arn,
          "${aws_dynamodb_table.processed_data.arn}/index/*",
          "${aws_dynamodb_table.aggregated_metrics.arn}/index/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards"
        ]
        Resource = "arn:aws:kinesis:${var.aws_region}:*:stream/${var.project_name}-*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:*:log-group:/aws/flink/${var.project_name}*"
      }
    ]
  })
}

# Outputs
output "dynamodb_processed_data_table" {
  value       = aws_dynamodb_table.processed_data.name
  description = "DynamoDB table name for processed data"
}

output "dynamodb_aggregated_metrics_table" {
  value       = aws_dynamodb_table.aggregated_metrics.name
  description = "DynamoDB table name for aggregated metrics"
}

output "dynamodb_dlq_table" {
  value       = aws_dynamodb_table.dead_letter_queue.name
  description = "DynamoDB table name for dead letter queue"
}

output "s3_checkpoint_bucket" {
  value       = aws_s3_bucket.flink_checkpoints.bucket
  description = "S3 bucket for Flink checkpoints"
}

output "flink_role_arn" {
  value       = aws_iam_role.flink_app.arn
  description = "IAM role ARN for Flink application"
}
