# StreamForge AWS Deployment Guide

**Version:** 1.0  
**Date:** December 19, 2025  
**Status:** Ready for deployment

---

## Overview

This guide provides step-by-step instructions for deploying StreamForge to AWS, migrating from the local MongoDB setup to AWS-managed services (DynamoDB, Kinesis/MSK, and EMR/Kinesis Analytics for Flink).

---

## Prerequisites

### AWS Account Setup
- AWS account with appropriate permissions
- AWS CLI installed and configured
- Terraform >= 1.0 installed
- Access keys configured: `aws configure`

### Local Environment
- StreamForge project fully tested locally
- Docker environment operational
- Flink job JAR built: `flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar`

---

## Phase 3: AWS Deployment Steps

### Day 11: Terraform Infrastructure Setup

#### 1. Create State Backend (COMPLETED)

✅ **Status:** Backend infrastructure created
- S3 bucket: `streamforge-terraform-state` (versioning enabled)
- DynamoDB table: `streamforge-terraform-locks` (ACTIVE)

<details>
<summary>Commands used (for reference)</summary>

```bash
# Create S3 bucket for Terraform state
aws s3api create-bucket \
  --bucket streamforge-terraform-state \
  --region us-east-1

# Enable versioning
aws s3api put-bucket-versioning \
  --bucket streamforge-terraform-state \
  --versioning-configuration Status=Enabled

# Create DynamoDB table for state locking
aws dynamodb create-table \
  --table-name streamforge-terraform-locks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```
</details>

#### 2. Initialize Terraform

```bash
cd terraform

# Initialize Terraform
terraform init

# Validate configuration
terraform validate

# Plan deployment
terraform plan -out=tfplan
```

#### 3. Deploy Infrastructure

```bash
# Apply Terraform configuration
terraform apply tfplan

# Note the outputs
terraform output
```

**Expected Outputs:**
- `dynamodb_processed_data_table`: streamforge-processed-data-dev
- `dynamodb_aggregated_metrics_table`: streamforge-aggregated-metrics-dev
- `dynamodb_dlq_table`: streamforge-dlq-dev
- `s3_checkpoint_bucket`: streamforge-flink-checkpoints-dev
- `flink_role_arn`: arn:aws:iam::ACCOUNT_ID:role/streamforge-flink-role-dev

---

### Day 12: DynamoDB Configuration

#### 1. Verify Table Creation

```bash
# List DynamoDB tables
aws dynamodb list-tables --query 'TableNames[?contains(@, `streamforge`)]'

# Describe processed_data table
aws dynamodb describe-table \
  --table-name streamforge-processed-data-dev \
  --query 'Table.{Name:TableName,Status:TableStatus,ItemCount:ItemCount}'
```

#### 2. Test Table Access

```bash
# Put a test item
aws dynamodb put-item \
  --table-name streamforge-processed-data-dev \
  --item '{
    "id": {"S": "test-1"},
    "timestamp": {"N": "1734619200000"},
    "type": {"S": "test"},
    "userId": {"S": "test-user"},
    "value": {"N": "42.5"},
    "payload": {"S": "test payload"}
  }'

# Query the test item
aws dynamodb get-item \
  --table-name streamforge-processed-data-dev \
  --key '{"id": {"S": "test-1"}, "timestamp": {"N": "1734619200000"}}'

# Delete test item
aws dynamodb delete-item \
  --table-name streamforge-processed-data-dev \
  --key '{"id": {"S": "test-1"}, "timestamp": {"N": "1734619200000"}}'
```

#### 3. Verify Billing Mode

```bash
# Confirm table is using PAY_PER_REQUEST mode
aws dynamodb describe-table \
  --table-name streamforge-processed-data-dev \
  --query 'Table.BillingModeSummary.BillingMode'

# Expected output: "PAY_PER_REQUEST"
# Note: PAY_PER_REQUEST mode handles scaling automatically - no Auto Scaling configuration needed
```

---

### Day 13: MongoDB to DynamoDB Migration

#### 1. Export MongoDB Data

```bash
# Export processed_data collection
docker exec streamforge-mongodb mongosh \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --quiet \
  --eval "db.getSiblingDB('streamforge').processed_data.find().forEach(printjson)" \
  > mongodb_export_processed_data.json

# Export aggregated_metrics collection
docker exec streamforge-mongodb mongosh \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --quiet \
  --eval "db.getSiblingDB('streamforge').aggregated_metrics.find().forEach(printjson)" \
  > mongodb_export_aggregated_metrics.json
```

#### 2. Transform Data Format

Create `scripts/transform_to_dynamodb.py`:

```python
#!/usr/bin/env python3
import json
import sys
from datetime import datetime

def transform_event(mongo_doc):
    """Transform MongoDB event document to DynamoDB format"""
    return {
        'id': {'S': mongo_doc.get('id', str(mongo_doc['_id']))},
        'timestamp': {'N': str(mongo_doc.get('timestamp', 0))},
        'type': {'S': mongo_doc.get('type', 'unknown')},
        'userId': {'S': mongo_doc.get('userId', 'unknown')},
        'value': {'N': str(mongo_doc.get('value', 0))},
        'payload': {'S': mongo_doc.get('payload', '')},
        'processedAt': {'N': str(int(datetime.now().timestamp() * 1000))}
    }

def main():
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    with open(input_file, 'r') as f:
        mongo_docs = [json.loads(line) for line in f if line.strip()]
    
    dynamodb_items = [transform_event(doc) for doc in mongo_docs]
    
    # Create batch write requests
    batch_requests = []
    for i in range(0, len(dynamodb_items), 25):
        batch = dynamodb_items[i:i+25]
        batch_requests.append({
            'streamforge-processed-data-dev': [
                {'PutRequest': {'Item': item}} for item in batch
            ]
        })
    
    with open(output_file, 'w') as f:
        json.dump(batch_requests, f, indent=2)

if __name__ == '__main__':
    main()
```

#### 3. Import to DynamoDB

```bash
./scripts/transform_to_dynamodb.py \
  mongodb_export_processed_data.json \
  dynamodb_import_processed_data.json
```

---

### Day 14: React Frontend Development

#### 1. Initialize React Project

```bash
# Create React app
cd /home/scotton/dev/projects/streamforge
npx create-react-app frontend

cd frontend

# Install dependencies
npm install @aws-amplify/ui-react aws-amplify
npm install recharts axios
```

#### 2. Configure AWS Amplify

```bash
# Initialize Amplify
amplify init

# Project name: streamforge-frontend
# Environment: dev
# Default editor: Visual Studio Code
# App type: javascript
# Framework: react
# Source directory: src
# Distribution directory: build
# Build command: npm run-script build
# Start command: npm run-script start

# Add API
amplify add api

# Service: REST
# API name: StreamForgeAPI
# Path: /events
# Lambda source: Create new Lambda function
```

#### 3. Create Basic UI Components

```javascript
// src/components/EventDashboard.js
import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';

function EventDashboard() {
  const [events, setEvents] = useState([]);
  const [metrics, setMetrics] = useState([]);
  
  useEffect(() => {
    // Fetch events from DynamoDB via API Gateway
    fetchEvents();
    fetchMetrics();
    
    // Poll every 5 seconds
    const interval = setInterval(() => {
      fetchEvents();
      fetchMetrics();
    }, 5000);
    
    return () => clearInterval(interval);
  }, []);
  
  const fetchEvents = async () => {
    // Implementation would query DynamoDB via API Gateway
    console.log('Fetching events...');
  };
  
  const fetchMetrics = async () => {
    // Implementation would query aggregated metrics
    console.log('Fetching metrics...');
  };
  
  return (
    <div className="dashboard">
      <h1>StreamForge Real-Time Dashboard</h1>
      
      <div className="metrics-summary">
        <div className="metric-card">
          <h3>Total Events</h3>
          <p>{events.length}</p>
        </div>
        <div className="metric-card">
          <h3>Active Users</h3>
          <p>{new Set(events.map(e => e.userId)).size}</p>
        </div>
      </div>
      
      <div className="chart-container">
        <h2>Events Over Time</h2>
        <LineChart width={800} height={400} data={metrics}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="timestamp" />
          <YAxis />
          <Tooltip />
          <Legend />
          <Line type="monotone" dataKey="count" stroke="#8884d8" />
        </LineChart>
      </div>
      
      <div className="events-table">
        <h2>Recent Events</h2>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Type</th>
              <th>User</th>
              <th>Value</th>
              <th>Timestamp</th>
            </tr>
          </thead>
          <tbody>
            {events.slice(0, 10).map(event => (
              <key={event.id}>
                <td>{event.id}</td>
                <td>{event.type}</td>
                <td>{event.userId}</td>
                <td>{event.value}</td>
                <td>{new Date(event.timestamp).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default EventDashboard;
```

---

### Day 15: AWS Amplify Deployment

#### 1. Build React Application

```bash
cd frontend

# Build production bundle
npm run build

# Test build locally
npx serve -s build
```

#### 2. Deploy to Amplify

```bash
# Push to Amplify
amplify push

# Deploy hosting
amplify add hosting

# Hosting type: Amplify Console
# Publish

amplify publish
```

#### 3. Configure Custom Domain (Optional)

```bash
# Add custom domain in Amplify Console
# streamforge.yourdomain.com

# Update DNS records
# CNAME: streamforge -> d123456abcdef.amplifyapp.com
```

---

## Deployment Verification

### 1. Check DynamoDB Tables

```bash
# Verify all tables exist
aws dynamodb list-tables | grep streamforge

# Check item counts
for table in streamforge-processed-data-dev streamforge-aggregated-metrics-dev streamforge-dlq-dev; do
  count=$(aws dynamodb scan --table-name $table --select COUNT --query 'Count')
  echo "$table: $count items"
done
```

### 2. Verify S3 Buckets

```bash
# List S3 buckets
aws s3 ls | grep streamforge

# Check checkpoint bucket
aws s3 ls s3://streamforge-flink-checkpoints-dev/
```

### 3. Test Amplify Frontend

```bash
# Get Amplify app URL
aws amplify list-apps --query 'apps[?name==`streamforge-frontend`].defaultDomain' --output text

# Open in browser or curl
curl https://main.d123456abcdef.amplifyapp.com
```

---

## Cost Estimation

### Monthly AWS Costs (Development Environment)

| Service | Usage | Estimated Cost |
|---------|-------|----------------|
| DynamoDB (PAY_PER_REQUEST) | 1M reads/writes | $2.50 |
| S3 (Checkpoints) | 10 GB storage | $0.23 |
| Kinesis Analytics (Flink) | 1 KPU, 24/7 | $35.04 |
| Amplify Hosting | Build minutes + hosting | $5.00 |
| CloudWatch Logs | 5 GB ingestion | $2.50 |
| **Total Estimated** | | **~$45/month** |

**Note:** Use AWS Free Tier where eligible. Stop Flink application when not in use to reduce costs.

---

## Rollback Procedures

### Rollback Terraform Changes

```bash
cd terraform

# Show current state
terraform show

# Revert to previous state
terraform state pull > terraform.tfstate.backup

# Destroy specific resource
terraform destroy -target=aws_dynamodb_table.processed_data
```

### Revert to MongoDB

```bash
# Stop Flink job
# Restart local Docker environment
cd docker && docker-compose up -d

# Flink job will automatically reconnect to MongoDB
```

---

## Troubleshooting

### DynamoDB Access Denied

```bash
# Check IAM policies
aws iam get-role-policy --role-name streamforge-flink-role-dev --policy-name streamforge-flink-policy

# Test credentials
aws sts get-caller-identity
```

### Amplify Build Failures

```bash
# Check build logs
aws amplify list-jobs --app-id <app-id> --branch-name main

# View specific job
aws amplify get-job --app-id <app-id> --branch-name main --job-id <job-id>
```

### Terraform State Locked

```bash
# Force unlock (use with caution)
terraform force-unlock <lock-id>
```

---

## Next Steps

After successful AWS deployment:

1. **Configure Monitoring:**
   - Set up CloudWatch dashboards
   - Configure alarms for DynamoDB throttling
   - Monitor Flink application metrics

2. **Enable CI/CD:**
   - Configure GitHub Actions for automated deployments
   - Set up staging environment
   - Implement blue-green deployments

3. **Security Hardening:**
   - Enable AWS WAF for Amplify
   - Configure VPC for Flink
   - Rotate access keys regularly

4. **Performance Optimization:**
   - Tune DynamoDB capacity
   - Configure Flink parallelism
   - Optimize checkpoint intervals

---

**Deployment Guide Version:** 1.0  
**Last Updated:** December 19, 2025  
**Author:** scotton  
**Project:** StreamForge Real-Time Data Streaming Platform
