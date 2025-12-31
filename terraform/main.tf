terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
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

# Local variables
locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
  
  resource_prefix = "${var.project_name}-${var.environment}"
}

# ====================================================================
# DynamoDB Tables
# ====================================================================

# Table 1: Processed Events (equivalent to MongoDB processed_data collection)
resource "aws_dynamodb_table" "processed_data" {
  name           = "${local.resource_prefix}-processed-data"
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
  
  attribute {
    name = "type"
    type = "S"
  }
  
  # GSI for querying by userId
  global_secondary_index {
    name            = "UserIdIndex"
    hash_key        = "userId"
    range_key       = "timestamp"
    projection_type = "ALL"
  }
  
  # GSI for querying by event type
  global_secondary_index {
    name            = "TypeIndex"
    hash_key        = "type"
    range_key       = "timestamp"
    projection_type = "ALL"
  }
  
  ttl {
    attribute_name = "expirationTime"
    enabled        = true
  }
  
  point_in_time_recovery {
    enabled = var.environment == "prod" ? true : false
  }
  
  tags = merge(
    local.common_tags,
    {
      Name = "${local.resource_prefix}-processed-data"
      Description = "Stores processed events from Flink stream"
    }
  )
}

# Table 2: Aggregated Metrics (windowed aggregations)
resource "aws_dynamodb_table" "aggregated_metrics" {
  name           = "${local.resource_prefix}-aggregated-metrics"
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
  
  # GSI for querying by event type
  global_secondary_index {
    name            = "EventTypeIndex"
    hash_key        = "eventType"
    range_key       = "windowStart"
    projection_type = "ALL"
  }
  
  ttl {
    attribute_name = "expirationTime"
    enabled        = true
  }
  
  point_in_time_recovery {
    enabled = var.environment == "prod" ? true : false
  }
  
  tags = merge(
    local.common_tags,
    {
      Name = "${local.resource_prefix}-aggregated-metrics"
      Description = "Stores windowed aggregation results"
    }
  )
}

# Table 3: Dead Letter Queue
resource "aws_dynamodb_table" "dead_letter_queue" {
  name           = "${local.resource_prefix}-dlq"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "errorId"
  range_key      = "timestamp"
  
  attribute {
    name = "errorId"
    type = "S"
  }
  
  attribute {
    name = "timestamp"
    type = "N"
  }
  
  ttl {
    attribute_name = "expirationTime"
    enabled        = true
  }
  
  tags = merge(
    local.common_tags,
    {
      Name = "${local.resource_prefix}-dlq"
      Description = "Dead letter queue for failed events"
    }
  )
}

# ====================================================================
# S3 Bucket for Flink Checkpoints
# ====================================================================

resource "aws_s3_bucket" "flink_checkpoints" {
  bucket = "${local.resource_prefix}-flink-checkpoints"
  
  tags = merge(
    local.common_tags,
    {
      Name = "${local.resource_prefix}-flink-checkpoints"
      Description = "Flink checkpoint storage"
    }
  )
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

resource "aws_s3_bucket_lifecycle_configuration" "flink_checkpoints" {
  bucket = aws_s3_bucket.flink_checkpoints.id
  
  rule {
    id     = "cleanup-old-checkpoints"
    status = "Enabled"
    
    expiration {
      days = 7
    }
    
    noncurrent_version_expiration {
      noncurrent_days = 3
    }
  }
}

resource "aws_s3_bucket_public_access_block" "flink_checkpoints" {
  bucket = aws_s3_bucket.flink_checkpoints.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ====================================================================
# IAM Role and Policies for Flink
# ====================================================================

# IAM Role for Flink Application
resource "aws_iam_role" "flink_role" {
  name = "${local.resource_prefix}-flink-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "kinesisanalytics.amazonaws.com",
            "ec2.amazonaws.com"
          ]
        }
      }
    ]
  })
  
  tags = merge(
    local.common_tags,
    {
      Name = "${local.resource_prefix}-flink-role"
    }
  )
}

# Policy for DynamoDB access
resource "aws_iam_policy" "flink_dynamodb_policy" {
  name        = "${local.resource_prefix}-flink-dynamodb"
  description = "Allow Flink to write to DynamoDB tables"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
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
      }
    ]
  })
}

# Policy for S3 checkpoint access
resource "aws_iam_policy" "flink_s3_policy" {
  name        = "${local.resource_prefix}-flink-s3"
  description = "Allow Flink to access checkpoint bucket"
  
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
      }
    ]
  })
}

# Policy for CloudWatch Logs
resource "aws_iam_policy" "flink_cloudwatch_policy" {
  name        = "${local.resource_prefix}-flink-cloudwatch"
  description = "Allow Flink to write logs to CloudWatch"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:*:log-group:/aws/kinesis-analytics/${local.resource_prefix}*"
      }
    ]
  })
}

# Attach policies to role
resource "aws_iam_role_policy_attachment" "flink_dynamodb" {
  role       = aws_iam_role.flink_role.name
  policy_arn = aws_iam_policy.flink_dynamodb_policy.arn
}

resource "aws_iam_role_policy_attachment" "flink_s3" {
  role       = aws_iam_role.flink_role.name
  policy_arn = aws_iam_policy.flink_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "flink_cloudwatch" {
  role       = aws_iam_role.flink_role.name
  policy_arn = aws_iam_policy.flink_cloudwatch_policy.arn
}

# ====================================================================
# VPC Endpoints (Cost Optimization)
# ====================================================================

# Get default VPC
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# VPC Endpoint for DynamoDB (Gateway endpoint - free)
resource "aws_vpc_endpoint" "dynamodb" {
  vpc_id            = data.aws_vpc.default.id
  service_name      = "com.amazonaws.${var.aws_region}.dynamodb"
  vpc_endpoint_type = "Gateway"
  
  route_table_ids = data.aws_route_tables.default.ids
  
  tags = merge(
    local.common_tags,
    {
      Name = "${local.resource_prefix}-dynamodb-endpoint"
    }
  )
}

data "aws_route_tables" "default" {
  vpc_id = data.aws_vpc.default.id
}

# VPC Endpoint for S3 (Gateway endpoint - free)
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = data.aws_vpc.default.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  
  route_table_ids = data.aws_route_tables.default.ids
  
  tags = merge(
    local.common_tags,
    {
      Name = "${local.resource_prefix}-s3-endpoint"
    }
  )
}

# ====================================================================
# Outputs
# ====================================================================

output "dynamodb_tables" {
  description = "DynamoDB table names and ARNs"
  value = {
    processed_data = {
      name = aws_dynamodb_table.processed_data.name
      arn  = aws_dynamodb_table.processed_data.arn
    }
    aggregated_metrics = {
      name = aws_dynamodb_table.aggregated_metrics.name
      arn  = aws_dynamodb_table.aggregated_metrics.arn
    }
    dead_letter_queue = {
      name = aws_dynamodb_table.dead_letter_queue.name
      arn  = aws_dynamodb_table.dead_letter_queue.arn
    }
  }
}

output "s3_checkpoint_bucket" {
  description = "S3 bucket for Flink checkpoints"
  value = {
    name = aws_s3_bucket.flink_checkpoints.bucket
    arn  = aws_s3_bucket.flink_checkpoints.arn
  }
}

output "flink_role" {
  description = "IAM role for Flink application"
  value = {
    name = aws_iam_role.flink_role.name
    arn  = aws_iam_role.flink_role.arn
  }
}

output "vpc_endpoints" {
  description = "VPC endpoint IDs"
  value = {
    dynamodb = aws_vpc_endpoint.dynamodb.id
    s3       = aws_vpc_endpoint.s3.id
  }
}
