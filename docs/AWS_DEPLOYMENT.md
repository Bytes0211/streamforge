# StreamForge AWS Deployment Guide

**Version:** 1.0  
**Last Updated:** December 23, 2025  
**Status:** Production Ready (Infrastructure as Code)

This guide provides complete step-by-step instructions for deploying StreamForge to AWS, migrating from the local MongoDB setup to AWS DynamoDB, and setting up a React frontend with AWS Amplify.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Architecture](#architecture)
4. [Cost Estimation](#cost-estimation)
5. [Infrastructure Deployment](#infrastructure-deployment)
6. [Data Migration](#data-migration)
7. [Flink Job Deployment](#flink-job-deployment)
8. [Frontend Deployment](#frontend-deployment)
9. [Monitoring & Operations](#monitoring--operations)
10. [Troubleshooting](#troubleshooting)

---

## Overview

### Deployment Architecture

The AWS deployment consists of:

- **Amazon MSK (Managed Kafka)**: Event streaming
- **Amazon Kinesis Data Analytics**: Flink runtime
- **Amazon DynamoDB**: NoSQL database (replaces MongoDB)
- **Amazon S3**: Flink checkpoint storage
- **AWS Amplify**: React frontend hosting
- **CloudWatch**: Logging and monitoring

### Migration Path

```
Local Environment               AWS Cloud
─────────────────               ─────────
Kafka (Docker)        →         Amazon MSK
Flink (Docker)        →         Kinesis Data Analytics
MongoDB (Docker)      →         DynamoDB
N/A                   →         AWS Amplify (React)
```

---

## Prerequisites

### Required Tools

1. **AWS CLI** (v2.x)
   ```bash
   aws --version
   # AWS CLI 2.13+ required
   ```

2. **Terraform** (v1.0+)
   ```bash
   terraform --version
   # Terraform v1.5+ recommended
   ```

3. **Node.js** (v18+) for frontend
   ```bash
   node --version
   npm --version
   ```

4. **Java 11** and **Maven** for Flink job
   ```bash
   java -version
   mvn --version
   ```

5. **Python 3.8+** for migration scripts
   ```bash
   python3 --version
   ```

### AWS Account Setup

1. **AWS Account** with admin access
2. **AWS CLI configured** with credentials
   ```bash
   aws configure
   # Enter: Access Key ID, Secret Access Key, Region (us-east-1), Output format (json)
   ```

3. **Service Quotas** (default limits are sufficient for dev)
   - DynamoDB: 256 tables per region
   - MSK: 1 cluster per account (adjustable)
   - Kinesis Analytics: 50 applications per region

---

## Architecture

### AWS Service Diagram

```
┌─────────────────┐
│   Data Source   │
│  (External API) │
└────────┬────────┘
         │
         v
┌─────────────────┐
│   Amazon MSK    │  Event streaming (Kafka replacement)
│   3 Brokers     │
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Kinesis Data   │  Flink stream processing
│   Analytics     │
│ (Flink 1.18)    │
└────────┬────────┘
         │
         v
┌─────────────────┐
│   DynamoDB      │  3 tables:
│   - Events      │  - processed_data
│   - Metrics     │  - aggregated_metrics
│   - DLQ         │  - dead_letter_queue
└─────────────────┘

         ┌─────────────────┐
         │   S3 Bucket     │  Flink checkpoints
         └─────────────────┘

         ┌─────────────────┐
         │  AWS Amplify    │  React frontend
         └─────────────────┘

         ┌─────────────────┐
         │   CloudWatch    │  Logs & Metrics
         └─────────────────┘
```

### DynamoDB Schema

#### Table 1: processed_data

Stores individual processed events (replaces MongoDB `processed_data` collection).

**Key Schema:**
- **Partition Key**: `id` (String) - Event ID
- **Sort Key**: `timestamp` (Number) - Event timestamp

**Attributes:**
- `id`: String - Unique event identifier
- `type`: String - Event type (click, view, purchase, etc.)
- `userId`: String - User identifier
- `value`: Number - Event value
- `timestamp`: Number - Event timestamp (epoch milliseconds)
- `payload`: String - Additional event data
- `processedAt`: Number - Processing timestamp
- `expirationTime`: Number - TTL attribute (90 days)

**Global Secondary Indexes:**
- **UserIdIndex**: `userId` (PK) + `timestamp` (SK)
- **TypeIndex**: `type` (PK) + `timestamp` (SK)

**Example Item:**
```json
{
  "id": "evt-123456",
  "type": "click",
  "userId": "user-001",
  "value": 10.5,
  "timestamp": 1702828800000,
  "payload": "button-checkout",
  "processedAt": 1702828801234,
  "expirationTime": 1710604800
}
```

#### Table 2: aggregated_metrics

Stores windowed aggregation results (replaces MongoDB `aggregated_metrics` collection).

**Key Schema:**
- **Partition Key**: `userId` (String)
- **Sort Key**: `windowStart` (Number)

**Attributes:**
- `userId`: String
- `eventType`: String
- `windowStart`: Number - Window start timestamp
- `windowEnd`: Number - Window end timestamp
- `count`: Number - Event count in window
- `sum`: Number - Sum of values
- `avg`: Number - Average value
- `min`: Number - Minimum value
- `max`: Number - Maximum value
- `expirationTime`: Number - TTL (90 days)

**Global Secondary Indexes:**
- **EventTypeIndex**: `eventType` (PK) + `windowStart` (SK)

**Example Item:**
```json
{
  "userId": "user-001",
  "eventType": "click",
  "windowStart": 1702828800000,
  "windowEnd": 1702828860000,
  "count": 42,
  "sum": 210.5,
  "avg": 5.01,
  "min": 1.2,
  "max": 15.8,
  "expirationTime": 1710604800
}
```

#### Table 3: dead_letter_queue

Stores failed events for debugging (replaces MongoDB `dead_letter_queue` collection).

**Key Schema:**
- **Partition Key**: `errorId` (String) - Unique error ID
- **Sort Key**: `timestamp` (Number)

**Attributes:**
- `errorId`: String
- `timestamp`: Number
- `rawEvent`: String - Original event JSON
- `errorMessage`: String
- `errorType`: String
- `expirationTime`: Number - TTL (30 days)

---

## Cost Estimation

### Development Environment (~$45/month)

| Service | Configuration | Monthly Cost |
|---------|--------------|--------------|
| DynamoDB | PAY_PER_REQUEST, 1M reads, 500K writes | $1.50 |
| MSK (Kafka) | kafka.t3.small (1 broker) | $30.00 |
| Kinesis Analytics | 1 KPU (Flink) | $10.00 |
| S3 | 10GB storage, 1K requests | $0.50 |
| VPC Endpoints | Gateway (DynamoDB, S3) | $0.00 |
| CloudWatch | Basic logs | $2.00 |
| Amplify | React hosting | $1.00 |
| **Total** | | **~$45/month** |

### Production Environment (~$176-385/month)

| Service | Configuration | Monthly Cost |
|---------|--------------|--------------|
| DynamoDB | PAY_PER_REQUEST, 10M reads, 5M writes | $15.00 |
| MSK (Kafka) | kafka.m5.large (3 brokers) | $180.00 |
| Kinesis Analytics | 4 KPUs (Flink) | $160.00 |
| S3 | 100GB storage, 10K requests | $2.50 |
| VPC Endpoints | Gateway (free) | $0.00 |
| CloudWatch | Enhanced monitoring | $25.00 |
| Amplify | React hosting + custom domain | $2.50 |
| **Total** | | **~$385/month** |

**Cost Optimization Strategies:**
1. Use **PAY_PER_REQUEST** billing for DynamoDB (no provisioned capacity)
2. Use **VPC Gateway Endpoints** for DynamoDB/S3 (no data transfer charges)
3. Configure **S3 lifecycle policies** to delete old checkpoints (7 days)
4. Enable **DynamoDB TTL** for automatic data expiration (90 days)
5. Use **t3.small** MSK brokers for dev/test
6. Set **CloudWatch log retention** to 7 days for dev, 30 days for prod

---

## Infrastructure Deployment

### Step 1: Validate Terraform Configuration

```bash
cd terraform

# Initialize Terraform
terraform init

# Validate configuration
terraform validate

# Preview changes
terraform plan
```

### Step 2: Deploy Infrastructure

```bash
# Deploy with default variables (dev environment)
terraform apply

# Or deploy with custom environment
terraform apply -var="environment=staging"

# Review and confirm
# Enter: yes
```

**Expected output:**
```
Apply complete! Resources: 18 added, 0 changed, 0 destroyed.

Outputs:

dynamodb_tables = {
  "processed_data" = {
    "arn" = "arn:aws:dynamodb:us-east-1:123456789012:table/streamforge-dev-processed-data"
    "name" = "streamforge-dev-processed-data"
  }
  ...
}
```

### Step 3: Verify Deployment

```bash
# List DynamoDB tables
aws dynamodb list-tables

# Describe S3 bucket
aws s3 ls | grep flink-checkpoints

# List IAM roles
aws iam list-roles | grep streamforge
```

### Step 4: Configure Additional Settings

#### Enable DynamoDB Streams (for real-time change capture)

```bash
aws dynamodb update-table \
  --table-name streamforge-dev-processed-data \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
```

#### Configure CloudWatch Alarms

```bash
# DynamoDB throttling alarm
aws cloudwatch put-metric-alarm \
  --alarm-name streamforge-dynamodb-throttles \
  --alarm-description "Alert on DynamoDB throttling" \
  --metric-name ThrottledRequests \
  --namespace AWS/DynamoDB \
  --statistic Sum \
  --period 300 \
  --threshold 10 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 1
```

---

## Data Migration

### MongoDB to DynamoDB Migration Strategy

#### Phase 1: Export MongoDB Data

```bash
# Export processed_data collection
docker exec streamforge-mongodb-1 mongoexport \
  -u admin -p password \
  --authenticationDatabase admin \
  --db streamforge \
  --collection processed_data \
  --out /tmp/processed_data.json

# Copy from container to host
docker cp streamforge-mongodb-1:/tmp/processed_data.json ./mongodb_export_processed_data.json

# Export aggregated_metrics collection
docker exec streamforge-mongodb-1 mongoexport \
  -u admin -p password \
  --authenticationDatabase admin \
  --db streamforge \
  --collection aggregated_metrics \
  --out /tmp/aggregated_metrics.json

docker cp streamforge-mongodb-1:/tmp/aggregated_metrics.json ./mongodb_export_aggregated_metrics.json
```

#### Phase 2: Transform Data Format

Use the provided Python script to transform MongoDB exports to DynamoDB format:

```bash
# Transform processed_data
python3 scripts/transform_to_dynamodb.py \
  mongodb_export_processed_data.json \
  dynamodb_processed_data.json

# Transform aggregated_metrics
python3 scripts/transform_to_dynamodb.py \
  mongodb_export_aggregated_metrics.json \
  dynamodb_aggregated_metrics.json
```

**Transformation Script** (`scripts/transform_to_dynamodb.py`):

Already exists in the project. It converts MongoDB documents to DynamoDB format with proper type annotations (S, N, etc.).

#### Phase 3: Import to DynamoDB

```bash
# Import processed_data (batch write)
aws dynamodb batch-write-item \
  --request-items file://dynamodb_processed_data.json

# Import aggregated_metrics
aws dynamodb batch-write-item \
  --request-items file://dynamodb_aggregated_metrics.json
```

**Note:** DynamoDB batch-write-item accepts max 25 items per request. The transform script automatically chunks data.

#### Phase 4: Verify Migration

```bash
# Count items in DynamoDB
aws dynamodb scan \
  --table-name streamforge-dev-processed-data \
  --select COUNT

# Sample query
aws dynamodb query \
  --table-name streamforge-dev-processed-data \
  --index-name UserIdIndex \
  --key-condition-expression "userId = :uid" \
  --expression-attribute-values '{":uid":{"S":"user-001"}}'
```

### Data Validation

Create a validation script (`scripts/validate_migration.py`):

```python
#!/usr/bin/env python3
import boto3
import json

def validate_migration(mongo_file, dynamodb_table):
    """Compare record counts and sample data"""
    
    # Count MongoDB records
    with open(mongo_file, 'r') as f:
        mongo_count = len(f.readlines())
    
    # Count DynamoDB records
    dynamodb = boto3.client('dynamodb')
    response = dynamodb.scan(
        TableName=dynamodb_table,
        Select='COUNT'
    )
    dynamo_count = response['Count']
    
    print(f"MongoDB records: {mongo_count}")
    print(f"DynamoDB records: {dynamo_count}")
    print(f"Match: {mongo_count == dynamo_count}")

if __name__ == '__main__':
    validate_migration(
        'mongodb_export_processed_data.json',
        'streamforge-dev-processed-data'
    )
```

---

## Flink Job Deployment

### Option 1: Amazon Kinesis Data Analytics (Recommended)

#### Build Flink JAR

```bash
cd flink-jobs
mvn clean package -DskipTests

# JAR location: target/flink-jobs-1.0-SNAPSHOT.jar
```

#### Update Flink Code for DynamoDB

Create new DynamoDB sink class (`DynamoDBSink.java`):

```java
package com.streamforge;

import com.streamforge.model.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.HashMap;
import java.util.Map;

public class DynamoDBSink extends RichSinkFunction<Event> {
    
    private transient DynamoDbClient dynamoClient;
    private final String tableName;
    
    public DynamoDBSink(String tableName) {
        this.tableName = tableName;
    }
    
    @Override
    public void open(Configuration parameters) {
        dynamoClient = DynamoDbClient.builder().build();
    }
    
    @Override
    public void invoke(Event event, Context context) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s(event.getId()).build());
        item.put("timestamp", AttributeValue.builder().n(String.valueOf(event.getTimestamp())).build());
        item.put("type", AttributeValue.builder().s(event.getType()).build());
        item.put("userId", AttributeValue.builder().s(event.getUserId()).build());
        item.put("value", AttributeValue.builder().n(String.valueOf(event.getValue())).build());
        item.put("payload", AttributeValue.builder().s(event.getPayload()).build());
        item.put("processedAt", AttributeValue.builder().n(String.valueOf(System.currentTimeMillis())).build());
        
        PutItemRequest request = PutItemRequest.builder()
            .tableName(tableName)
            .item(item)
            .build();
        
        dynamoClient.putItem(request);
    }
    
    @Override
    public void close() {
        if (dynamoClient != null) {
            dynamoClient.close();
        }
    }
}
```

#### Create Kinesis Analytics Application

```bash
# Upload JAR to S3
aws s3 cp target/flink-jobs-1.0-SNAPSHOT.jar \
  s3://streamforge-dev-flink-checkpoints/application-code/

# Create Kinesis Analytics application
aws kinesisanalyticsv2 create-application \
  --application-name streamforge-processor \
  --runtime-environment FLINK-1_18 \
  --service-execution-role arn:aws:iam::123456789012:role/streamforge-dev-flink-role \
  --application-configuration file://kinesis-app-config.json
```

**Application Configuration** (`kinesis-app-config.json`):

```json
{
  "ApplicationCodeConfiguration": {
    "CodeContent": {
      "S3ContentLocation": {
        "BucketARN": "arn:aws:s3:::streamforge-dev-flink-checkpoints",
        "FileKey": "application-code/flink-jobs-1.0-SNAPSHOT.jar"
      }
    },
    "CodeContentType": "ZIPFILE"
  },
  "ApplicationSnapshotConfiguration": {
    "SnapshotsEnabled": true
  },
  "EnvironmentProperties": {
    "PropertyGroups": [
      {
        "PropertyGroupId": "ProducerConfigProperties",
        "PropertyMap": {
          "kafka.brokers": "b-1.streamforge.xxxxx.kafka.us-east-1.amazonaws.com:9092"
        }
      }
    ]
  },
  "FlinkApplicationConfiguration": {
    "CheckpointConfiguration": {
      "ConfigurationType": "CUSTOM",
      "CheckpointingEnabled": true,
      "CheckpointInterval": 30000,
      "MinPauseBetweenCheckpoints": 5000
    },
    "MonitoringConfiguration": {
      "ConfigurationType": "CUSTOM",
      "MetricsLevel": "APPLICATION",
      "LogLevel": "INFO"
    },
    "ParallelismConfiguration": {
      "ConfigurationType": "CUSTOM",
      "Parallelism": 2,
      "ParallelismPerKPU": 1,
      "AutoScalingEnabled": true
    }
  }
}
```

### Option 2: Self-Managed Flink on EC2

For more control, deploy Flink cluster on EC2 instances. (Details omitted for brevity - similar to Docker setup but with EC2 instances.)

---

## Frontend Deployment

### React Frontend Architecture

```
frontend/
├── public/
│   └── index.html
├── src/
│   ├── components/
│   │   ├── Dashboard.jsx       # Main dashboard
│   │   ├── EventList.jsx       # Recent events table
│   │   ├── MetricsChart.jsx    # Time-series chart
│   │   └── StatsCards.jsx      # Summary cards
│   ├── services/
│   │   └── api.js              # API client (AWS SDK)
│   ├── App.jsx
│   └── index.jsx
├── amplify/
│   └── backend-config.json     # Amplify configuration
├── package.json
└── README.md
```

### Step 1: Create React App

```bash
mkdir frontend
cd frontend

# Create React app with TypeScript
npx create-react-app . --template typescript

# Install AWS SDK and UI libraries
npm install @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb
npm install @aws-amplify/ui-react aws-amplify
npm install recharts date-fns
```

### Step 2: Configure AWS Amplify

```bash
# Initialize Amplify
amplify init

# Add authentication (optional)
amplify add auth

# Add API (REST API with Lambda)
amplify add api

# Push to cloud
amplify push
```

### Step 3: Deploy Frontend

```bash
# Build production bundle
npm run build

# Deploy to Amplify
amplify publish

# Or deploy to S3 + CloudFront
aws s3 sync build/ s3://streamforge-frontend
```

**Amplify Configuration** (`src/aws-exports.js`):

```javascript
const awsconfig = {
    region: 'us-east-1',
    userPoolId: 'us-east-1_XXXXXXXXX',
    userPoolWebClientId: 'XXXXXXXXXXXXXXXXXX',
    api: {
        endpoints: [
            {
                name: 'StreamForgeAPI',
                endpoint: 'https://api.streamforge.com',
                region: 'us-east-1'
            }
        ]
    }
};

export default awsconfig;
```

---

## Monitoring & Operations

### CloudWatch Dashboards

Create custom dashboard for StreamForge metrics:

```bash
aws cloudwatch put-dashboard \
  --dashboard-name StreamForge-Dashboard \
  --dashboard-body file://cloudwatch-dashboard.json
```

### Key Metrics to Monitor

1. **DynamoDB Metrics**
   - `ConsumedReadCapacityUnits`
   - `ConsumedWriteCapacityUnits`
   - `ThrottledRequests`
   - `UserErrors`

2. **Flink Metrics**
   - `numRecordsIn`
   - `numRecordsOut`
   - `checkpointDuration`
   - `lastCheckpointSize`

3. **MSK Metrics**
   - `BytesInPerSec`
   - `BytesOutPerSec`
   - `MessagesInPerSec`
   - `UnderReplicatedPartitions`

### Alerting

Set up SNS topic for alerts:

```bash
# Create SNS topic
aws sns create-topic --name streamforge-alerts

# Subscribe email
aws sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:123456789012:streamforge-alerts \
  --protocol email \
  --notification-endpoint your-email@example.com
```

---

## Troubleshooting

### DynamoDB Throttling

**Symptoms:** `ProvisionedThroughputExceededException`

**Solutions:**
- Switch to PAY_PER_REQUEST billing mode (already configured in Terraform)
- Add exponential backoff in Flink sink
- Enable DynamoDB auto-scaling (if using provisioned capacity)

### Flink Checkpoint Failures

**Symptoms:** Jobs restarting frequently, data loss

**Solutions:**
- Check S3 bucket permissions (IAM role)
- Increase checkpoint timeout
- Reduce checkpoint interval
- Monitor S3 bucket size

### High Costs

**Symptoms:** AWS bill exceeds estimate

**Solutions:**
- Enable DynamoDB TTL (automatic data deletion)
- Configure S3 lifecycle policies (checkpoint cleanup)
- Use Savings Plans for Kinesis Analytics
- Reduce MSK broker count in dev environment
- Set CloudWatch log retention to 7 days

---

## Summary Checklist

- [ ] AWS CLI and Terraform installed
- [ ] AWS credentials configured
- [ ] Terraform infrastructure deployed (`terraform apply`)
- [ ] DynamoDB tables created and verified
- [ ] MongoDB data exported
- [ ] Data transformed and imported to DynamoDB
- [ ] Flink JAR built with DynamoDB sink
- [ ] Kinesis Analytics application created
- [ ] React frontend deployed to Amplify
- [ ] CloudWatch dashboards configured
- [ ] SNS alerts configured
- [ ] Cost monitoring enabled

---

## Additional Resources

- [AWS DynamoDB Best Practices](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/best-practices.html)
- [Amazon MSK Documentation](https://docs.aws.amazon.com/msk/latest/developerguide/)
- [Kinesis Data Analytics Flink Guide](https://docs.aws.amazon.com/kinesisanalytics/latest/java/what-is.html)
- [AWS Amplify Documentation](https://docs.amplify.aws/)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)

---

**Document Version:** 1.0  
**Author:** StreamForge Team  
**Last Review:** December 23, 2025
