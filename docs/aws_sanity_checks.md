# StreamForge AWS Infrastructure Sanity Checks

**Project:** StreamForge Real-Time Data Streaming Platform  
**Created:** December 22, 2025  
**Last Updated:** December 23, 2025  
**Status:** ✅ Infrastructure Documented (Phase 3 Complete)  
**Deployment Status:** ⏸️ Ready to deploy (requires AWS credentials)  
**Purpose:** Quick validation commands to verify AWS production deployment

---

## Quick Health Check (Run All)

```bash
# Run all checks in sequence
echo "=== Kinesis Streams ==="
aws kinesis list-streams --query 'StreamNames[?contains(@, `streamforge`)]' --output table

echo -e "\n=== Managed Flink Applications ==="
aws kinesisanalyticsv2 list-applications --query 'ApplicationSummaries[?contains(ApplicationName, `streamforge`)].{Name:ApplicationName,Status:ApplicationStatus}' --output table

echo -e "\n=== DynamoDB Tables ==="
aws dynamodb list-tables --query 'TableNames[?contains(@, `streamforge`)]' --output table

echo -e "\n=== Amplify Apps ==="
aws amplify list-apps --query 'apps[?contains(name, `streamforge`)].{Name:name,Status:defaultDomain}' --output table

echo -e "\n=== S3 Buckets ==="
aws s3 ls | grep streamforge

echo -e "\n=== IAM Roles ==="
aws iam list-roles --query 'Roles[?contains(RoleName, `streamforge`)].RoleName' --output table
```

---

## 1. Kinesis Data Streams

### List Kinesis streams
```bash
aws kinesis list-streams --query 'StreamNames[?contains(@, `streamforge`)]' --output table
```

**Expected Output:**
```
|  ListStreams  |
+---------------+
|  streamforge-input-stream-prod  |
+---------------+
```

### Describe stream
```bash
aws kinesis describe-stream --stream-name streamforge-input-stream-prod
```

**Expected Output:**
```json
{
    "StreamDescription": {
        "StreamName": "streamforge-input-stream-prod",
        "StreamARN": "arn:aws:kinesis:us-east-1:XXXXXXXXXXXX:stream/streamforge-input-stream-prod",
        "StreamStatus": "ACTIVE",
        "ShardCount": 3,
        "RetentionPeriodHours": 24,
        "EncryptionType": "KMS"
    }
}
```

### Get stream summary
```bash
aws kinesis describe-stream-summary --stream-name streamforge-input-stream-prod
```

### List shards
```bash
aws kinesis list-shards --stream-name streamforge-input-stream-prod
```

**Expected:** 3 active shards

### Check stream metrics (last hour)
```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name IncomingRecords \
  --dimensions Name=StreamName,Value=streamforge-input-stream-prod \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 3600 \
  --statistics Sum
```

### Test put record
```bash
aws kinesis put-record \
  --stream-name streamforge-input-stream-prod \
  --partition-key test-key-001 \
  --data '{"id": "test-001", "message": "hello from CLI", "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}'
```

---

## 2. Managed Apache Flink (Kinesis Analytics V2)

### List Flink applications
```bash
aws kinesisanalyticsv2 list-applications \
  --query 'ApplicationSummaries[?contains(ApplicationName, `streamforge`)]' \
  --output table
```

**Expected Output:**
```
|  ApplicationSummaries  |
+------------------------+
|  ApplicationName       | ApplicationStatus | RuntimeEnvironment  |
|  streamforge-processor-prod | RUNNING    | FLINK-1_18          |
+------------------------+
```

### Describe Flink application
```bash
aws kinesisanalyticsv2 describe-application \
  --application-name streamforge-processor-prod
```

**Expected Status:** `RUNNING`

### Get application details
```bash
aws kinesisanalyticsv2 describe-application \
  --application-name streamforge-processor-prod \
  --query 'ApplicationDetail.{Name:ApplicationName,Status:ApplicationStatus,Version:ApplicationVersionId,Created:CreateTimestamp,Updated:LastUpdateTimestamp}' \
  --output table
```

### Check application logs
```bash
# Get log stream name
LOG_GROUP="/aws/kinesis-analytics/streamforge-processor-prod"
aws logs describe-log-streams \
  --log-group-name $LOG_GROUP \
  --order-by LastEventTime \
  --descending \
  --max-items 5
```

### Tail application logs
```bash
aws logs tail /aws/kinesis-analytics/streamforge-processor-prod --since 1h --follow
```

### Check application metrics
```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/KinesisAnalytics \
  --metric-name numRecordsOut \
  --dimensions Name=Application,Value=streamforge-processor-prod \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 3600 \
  --statistics Sum
```

### List application snapshots (for stateful processing)
```bash
aws kinesisanalyticsv2 list-application-snapshots \
  --application-name streamforge-processor-prod
```

---

## 3. DynamoDB

### List StreamForge tables
```bash
aws dynamodb list-tables --query 'TableNames[?contains(@, `streamforge`)]' --output table
```

**Expected Tables:**
```
|  ListTables  |
+--------------+
|  streamforge-processed-data-prod  |
+--------------+
```

### Describe table
```bash
aws dynamodb describe-table --table-name streamforge-processed-data-prod
```

**Expected Output:**
```json
{
    "Table": {
        "TableName": "streamforge-processed-data-prod",
        "TableStatus": "ACTIVE",
        "KeySchema": [
            {
                "AttributeName": "id",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "timestamp",
                "KeyType": "RANGE"
            }
        ],
        "BillingModeSummary": {
            "BillingMode": "PAY_PER_REQUEST"
        }
    }
}
```

### Get table item count (approximate)
```bash
aws dynamodb describe-table \
  --table-name streamforge-processed-data-prod \
  --query 'Table.ItemCount'
```

### Scan recent items
```bash
aws dynamodb scan \
  --table-name streamforge-processed-data-prod \
  --limit 5 \
  --query 'Items[].{id:id.S,message:message.S,timestamp:timestamp.S}'
```

### Query specific item (if you know the key)
```bash
aws dynamodb get-item \
  --table-name streamforge-processed-data-prod \
  --key '{"id": {"S": "test-001"}, "timestamp": {"S": "2025-12-22T19:00:00Z"}}'
```

### Check table metrics (consumed capacity)
```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/DynamoDB \
  --metric-name ConsumedReadCapacityUnits \
  --dimensions Name=TableName,Value=streamforge-processed-data-prod \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 3600 \
  --statistics Sum
```

### Check table continuous backups
```bash
aws dynamodb describe-continuous-backups \
  --table-name streamforge-processed-data-prod
```

**Expected:** Point-in-time recovery should be ENABLED for production

---

## 4. S3 Buckets

### List StreamForge buckets
```bash
aws s3 ls | grep streamforge
```

**Expected Buckets:**
```
2025-XX-XX XX:XX:XX streamforge-flink-artifacts-prod
2025-XX-XX XX:XX:XX streamforge-logs-prod
2025-XX-XX XX:XX:XX streamforge-terraform-state-XXXXXXXXXX
```

### Check Flink artifacts bucket
```bash
aws s3 ls s3://streamforge-flink-artifacts-prod/
```

**Expected Output:**
```
                           PRE jars/
                           PRE checkpoints/
```

### List JAR files
```bash
aws s3 ls s3://streamforge-flink-artifacts-prod/jars/
```

**Expected Output:**
```
2025-XX-XX XX:XX:XX    XXXXX flink-jobs-1.0-SNAPSHOT.jar
```

### Check logs bucket
```bash
aws s3 ls s3://streamforge-logs-prod/
```

**Expected Output:**
```
                           PRE flink-application-logs/
                           PRE kinesis-firehose-logs/
```

### Verify bucket encryption
```bash
aws s3api get-bucket-encryption --bucket streamforge-flink-artifacts-prod
```

**Expected:** SSE-S3 or SSE-KMS encryption enabled

---

## 5. IAM Roles

### List StreamForge IAM roles
```bash
aws iam list-roles --query 'Roles[?contains(RoleName, `streamforge`)].{Name:RoleName,Created:CreateDate}' --output table
```

**Expected Roles:**
- `streamforge-flink-service-role-prod`
- `streamforge-kinesis-firehose-role-prod` (if using Firehose)
- `streamforge-lambda-role-prod` (if using Lambda for preprocessing)

### Check Flink service role policies
```bash
aws iam list-attached-role-policies --role-name streamforge-flink-service-role-prod
```

**Expected Output:**
```json
{
    "AttachedPolicies": [
        {
            "PolicyName": "streamforge-flink-kinesis-access-prod",
            "PolicyArn": "arn:aws:iam::XXXXXXXXXXXX:policy/streamforge-flink-kinesis-access-prod"
        },
        {
            "PolicyName": "streamforge-flink-dynamodb-access-prod",
            "PolicyArn": "arn:aws:iam::XXXXXXXXXXXX:policy/streamforge-flink-dynamodb-access-prod"
        },
        {
            "PolicyName": "streamforge-flink-s3-access-prod",
            "PolicyArn": "arn:aws:iam::XXXXXXXXXXXX:policy/streamforge-flink-s3-access-prod"
        }
    ]
}
```

### Check Flink role inline policies
```bash
aws iam list-role-policies --role-name streamforge-flink-service-role-prod
```

### Get policy details
```bash
aws iam get-policy \
  --policy-arn arn:aws:iam::XXXXXXXXXXXX:policy/streamforge-flink-kinesis-access-prod
```

---

## 6. AWS Amplify (React Frontend)

### List Amplify apps
```bash
aws amplify list-apps --query 'apps[?contains(name, `streamforge`)].{Name:name,AppId:appId,Domain:defaultDomain,Status:status}' --output table
```

**Expected App:**
- `streamforge-frontend-prod`

### Get app details
```bash
aws amplify get-app --app-id <app-id>
```

### List branches
```bash
aws amplify list-branches --app-id <app-id>
```

**Expected Branch:**
- `main` (auto-deployed from Git)

### Get branch details
```bash
aws amplify get-branch --app-id <app-id> --branch-name main
```

**Expected Status:** `ACTIVE`

### List recent deployments
```bash
aws amplify list-jobs --app-id <app-id> --branch-name main --max-results 5
```

### Check domain status
```bash
aws amplify get-domain-association --app-id <app-id> --domain-name streamforge.example.com
```

**Expected:** Domain status should be `AVAILABLE`

---

## 7. CloudWatch Logs

### List log groups
```bash
aws logs describe-log-groups --query 'logGroups[?contains(logGroupName, `streamforge`)].logGroupName' --output table
```

**Expected Log Groups:**
```
|  LogGroups  |
+-------------+
|  /aws/kinesis-analytics/streamforge-processor-prod  |
|  /aws/lambda/streamforge-preprocessor-prod          |
|  /aws/amplify/streamforge-frontend-prod             |
+-------------+
```

### Tail Flink application logs
```bash
aws logs tail /aws/kinesis-analytics/streamforge-processor-prod --since 30m --follow
```

### Search for errors in last hour
```bash
aws logs filter-log-events \
  --log-group-name /aws/kinesis-analytics/streamforge-processor-prod \
  --start-time $(date -u -d '1 hour ago' +%s)000 \
  --filter-pattern "ERROR"
```

### Get log insights query results
```bash
aws logs start-query \
  --log-group-name /aws/kinesis-analytics/streamforge-processor-prod \
  --start-time $(date -u -d '1 hour ago' +%s) \
  --end-time $(date -u +%s) \
  --query-string 'fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc | limit 20'
```

---

## 8. Terraform State

### Check Terraform state bucket
```bash
aws s3 ls | grep streamforge-terraform-state
```

**Expected Output:**
```
2025-XX-XX XX:XX:XX streamforge-terraform-state-XXXXXXXXXX
```

### List state files
```bash
aws s3 ls s3://streamforge-terraform-state-XXXXXXXXXX/
```

**Expected Output:**
```
                           PRE env:/
2025-XX-XX XX:XX:XX      XXXXX terraform.tfstate
```

### Check DynamoDB lock table
```bash
aws dynamodb describe-table --table-name streamforge-terraform-locks
```

**Expected Status:** `ACTIVE`

### List current locks (should be empty unless terraform is running)
```bash
aws dynamodb scan --table-name streamforge-terraform-locks --query 'Items[]'
```

---

## 9. CloudWatch Alarms

### List StreamForge alarms
```bash
aws cloudwatch describe-alarms --query 'MetricAlarms[?contains(AlarmName, `streamforge`)].{Name:AlarmName,State:StateValue,Metric:MetricName}' --output table
```

**Expected Alarms:**
- `streamforge-kinesis-iterator-age-alarm-prod`
- `streamforge-flink-checkpoint-failure-alarm-prod`
- `streamforge-dynamodb-throttle-alarm-prod`

### Check alarm state
```bash
aws cloudwatch describe-alarms --alarm-names streamforge-flink-checkpoint-failure-alarm-prod
```

**Expected State:** `OK` (not `ALARM` or `INSUFFICIENT_DATA`)

---

## 10. Cost Estimation

### Get current month's costs for StreamForge
```bash
aws ce get-cost-and-usage \
  --time-period Start=$(date -d "$(date +%Y-%m-01)" +%Y-%m-%d),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics BlendedCost \
  --group-by Type=TAG,Key=Project \
  --filter file://<(cat <<EOF
{
  "Tags": {
    "Key": "Project",
    "Values": ["streamforge"]
  }
}
EOF
)
```

### Get service-level breakdown
```bash
aws ce get-cost-and-usage \
  --time-period Start=$(date -d "$(date +%Y-%m-01)" +%Y-%m-%d),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics BlendedCost \
  --group-by Type=SERVICE \
  --filter file://<(cat <<EOF
{
  "Tags": {
    "Key": "Project",
    "Values": ["streamforge"]
  }
}
EOF
)
```

---

## 11. Comprehensive Validation Script

Save this as `validate_aws_infrastructure.sh`:

```bash
#!/bin/bash

# StreamForge AWS Infrastructure Validation Script
# Version: 1.0
# Date: December 22, 2025

set -e

echo "============================================"
echo "StreamForge AWS Infrastructure Check"
echo "============================================"
echo "Date: $(date)"
echo "Region: $(aws configure get region)"
echo "============================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_resource() {
  local name=$1
  local command=$2
  local expected=$3
  
  echo -n "Checking $name... "
  if result=$(eval $command 2>/dev/null); then
    if [ -n "$expected" ] && ! echo "$result" | grep -q "$expected"; then
      echo -e "${YELLOW}WARNING${NC}: Resource exists but unexpected output"
      echo "  Expected pattern: $expected"
    else
      echo -e "${GREEN}OK${NC}"
    fi
  else
    echo -e "${RED}FAILED${NC}"
    return 1
  fi
}

# 1. Kinesis Streams
echo "=== Kinesis Streams ==="
check_resource "Input Stream" "aws kinesis describe-stream --stream-name streamforge-input-stream-prod" "ACTIVE"
echo ""

# 2. Managed Flink
echo "=== Managed Apache Flink ==="
check_resource "Flink Application" "aws kinesisanalyticsv2 describe-application --application-name streamforge-processor-prod" "RUNNING"
echo ""

# 3. DynamoDB
echo "=== DynamoDB ==="
check_resource "Processed Data Table" "aws dynamodb describe-table --table-name streamforge-processed-data-prod" "ACTIVE"
echo ""

# 4. S3 Buckets
echo "=== S3 Buckets ==="
check_resource "Flink Artifacts Bucket" "aws s3 ls | grep streamforge-flink-artifacts-prod" "streamforge-flink-artifacts-prod"
check_resource "Logs Bucket" "aws s3 ls | grep streamforge-logs-prod" "streamforge-logs-prod"
check_resource "Terraform State Bucket" "aws s3 ls | grep streamforge-terraform-state" "streamforge-terraform-state"
check_resource "Flink JAR" "aws s3 ls s3://streamforge-flink-artifacts-prod/jars/" "flink-jobs"
echo ""

# 5. IAM Roles
echo "=== IAM Roles ==="
check_resource "Flink Service Role" "aws iam get-role --role-name streamforge-flink-service-role-prod" "streamforge-flink-service-role-prod"
echo ""

# 6. Amplify
echo "=== AWS Amplify ==="
if aws amplify list-apps --query 'apps[?contains(name, `streamforge`)]' --output text 2>/dev/null | grep -q "streamforge"; then
  check_resource "Amplify App" "aws amplify list-apps --query 'apps[?contains(name, \`streamforge\`)].name' --output text" "streamforge"
  echo -e "${GREEN}Amplify frontend is deployed${NC}"
else
  echo -e "${YELLOW}Amplify not deployed yet (expected for Phase 1)${NC}"
fi
echo ""

# 7. CloudWatch Logs
echo "=== CloudWatch Logs ==="
check_resource "Flink Application Logs" "aws logs describe-log-groups --query 'logGroups[?contains(logGroupName, \`streamforge-processor\`)].logGroupName' --output text" "streamforge-processor"
echo ""

# 8. Terraform State
echo "=== Terraform ==="
check_resource "Terraform Lock Table" "aws dynamodb describe-table --table-name streamforge-terraform-locks" "ACTIVE"
echo ""

echo "============================================"
echo "Validation Complete!"
echo "============================================"
echo ""
echo "Summary:"
echo "- All core infrastructure should show ${GREEN}OK${NC}"
echo "- ${YELLOW}WARNING${NC} indicates resource exists but may need review"
echo "- ${RED}FAILED${NC} indicates missing resource"
echo ""
echo "For detailed logs:"
echo "  aws logs tail /aws/kinesis-analytics/streamforge-processor-prod --since 1h"
echo ""
echo "Access Flink application:"
echo "  aws kinesisanalyticsv2 describe-application --application-name streamforge-processor-prod"
```

**Usage:**
```bash
chmod +x validate_aws_infrastructure.sh
./validate_aws_infrastructure.sh
```

---

## 12. Quick Troubleshooting Commands

### Restart Flink application
```bash
aws kinesisanalyticsv2 stop-application \
  --application-name streamforge-processor-prod

aws kinesisanalyticsv2 start-application \
  --application-name streamforge-processor-prod \
  --run-configuration '{}'
```

### Update Flink application with new JAR
```bash
# First upload new JAR to S3
aws s3 cp flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar \
  s3://streamforge-flink-artifacts-prod/jars/flink-jobs-1.0-SNAPSHOT.jar

# Update application
aws kinesisanalyticsv2 update-application \
  --application-name streamforge-processor-prod \
  --current-application-version-id <version-id> \
  --application-configuration-update file://flink-app-config-update.json
```

### Check Kinesis iterator age (indicates processing lag)
```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name GetRecords.IteratorAgeMilliseconds \
  --dimensions Name=StreamName,Value=streamforge-input-stream-prod \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Average,Maximum
```

### Check DynamoDB throttling
```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/DynamoDB \
  --metric-name UserErrors \
  --dimensions Name=TableName,Value=streamforge-processed-data-prod \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum
```

### Trigger Amplify rebuild
```bash
aws amplify start-job \
  --app-id <app-id> \
  --branch-name main \
  --job-type RELEASE
```

---

## Expected Results Summary

**Production Deployment (Phase 2+):**
- ✅ Kinesis Data Stream with 3 shards (ACTIVE)
- ✅ Managed Flink application (RUNNING)
- ✅ DynamoDB table with on-demand billing (ACTIVE)
- ✅ S3 buckets (artifacts, logs, terraform state)
- ✅ Flink JAR uploaded to S3
- ✅ IAM service roles with proper policies
- ✅ CloudWatch log groups capturing application logs
- ✅ CloudWatch alarms for monitoring (OK state)
- ✅ Terraform state backend configured
- ✅ AWS Amplify frontend (optional, Phase 3+)

**Current Status (Phase 1):**
- 🟡 Local infrastructure fully operational
- 🟡 AWS deployment not yet implemented
- 📝 AWS architecture documented in WARP.md
- 📝 Terraform configurations to be created in Phase 2

**Key Differences from AutoCorp:**
- StreamForge uses Kinesis instead of Kafka/DMS
- Uses Managed Flink (Kinesis Analytics) instead of self-hosted Flink
- Uses DynamoDB instead of raw S3 data lake
- Simpler architecture focused on real-time processing
- React frontend for data visualization vs. Athena/QuickSight

---

## Notes

- **Region:** All commands assume `us-east-1`. Update if different.
- **Profile:** Add `--profile <profile-name>` if not using default AWS profile
- **Permissions:** Requires read access to Kinesis, Managed Flink, DynamoDB, S3, IAM, Amplify, CloudWatch
- **Cost:** All read-only commands above are free or negligible cost
- **Naming Convention:** All resources use `streamforge-{resource}-{env}` pattern
- **Tags:** All resources tagged with `Project=streamforge` and `Environment=prod`

---

**Document Version:** 1.1  
**Created:** December 22, 2025  
**Last Updated:** December 23, 2025  
**Author:** scotton  
**Project:** StreamForge Real-Time Data Streaming Platform  
**Project Status:** ✅ 100% Complete (All 4 phases finished Dec 19, 2025)  
**AWS Status:** Infrastructure documented, ready to deploy when needed

**Note:** This document provides commands for a DEPLOYED AWS environment. The StreamForge project has complete Terraform infrastructure code (terraform/main.tf) and deployment documentation (docs/AWS_DEPLOYMENT.md), but actual AWS resources are NOT deployed (current cost: $0/month).
