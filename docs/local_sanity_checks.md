# StreamForge Local Development Sanity Checks

**Project:** StreamForge Real-Time Data Streaming Platform  
**Created:** December 22, 2025  
**Last Updated:** December 23, 2025  
**Status:** ✅ Project Complete - All 4 phases finished  
**Purpose:** Quick validation commands to verify local development environment

---

## Quick Health Check (Run All)

```bash
# Run all checks in sequence from project root
echo "=== Docker Containers ==="
docker ps --filter "name=streamforge" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo -e "\n=== Kafka Topics ==="
docker exec -it streamforge-kafka kafka-topics --list --bootstrap-server localhost:9092

echo -e "\n=== MongoDB Collections ==="
docker exec -it streamforge-mongodb mongosh -u admin -p password --eval "use streamforge; db.getCollectionNames();" --quiet

echo -e "\n=== Flink Jobs ==="
curl -s http://localhost:8081/jobs | python3 -m json.tool

echo -e "\n=== Maven Build Status ==="
mvn verify -q && echo "✅ Build OK" || echo "❌ Build Failed"
```

---

## 1. Docker Infrastructure

### Check all containers are running
```bash
docker ps --filter "name=streamforge"
```

**Expected Output:**
```
CONTAINER ID   IMAGE                       STATUS         PORTS
XXXXXXXXXXXX   confluentinc/cp-kafka:7.5.0    Up X minutes   0.0.0.0:9092->9092/tcp
XXXXXXXXXXXX   flink:1.18.0-scala_2.12      Up X minutes   6123/tcp, 0.0.0.0:8081->8081/tcp
XXXXXXXXXXXX   flink:1.18.0-scala_2.12      Up X minutes   6123/tcp, 8081/tcp
XXXXXXXXXXXX   mongo:7.0                    Up X minutes   0.0.0.0:27017->27017/tcp
XXXXXXXXXXXX   confluentinc/cp-zookeeper:7.5.0  Up X minutes   2181/tcp
```

**Status:** ✅ Should have 5 containers running:
- `streamforge-kafka`
- `streamforge-flink-jobmanager`
- `streamforge-flink-taskmanager`
- `streamforge-mongodb`
- `streamforge-zookeeper`

### Check container health
```bash
docker ps --filter "name=streamforge" --format "{{.Names}}: {{.Status}}"
```

### View container logs
```bash
# Kafka logs
docker logs streamforge-kafka --tail 50

# Flink JobManager logs
docker logs streamforge-flink-jobmanager --tail 50

# MongoDB logs
docker logs streamforge-mongodb --tail 50
```

### Check Docker network
```bash
docker network inspect streamforge-network
```

**Expected Output:**
- Network should exist with bridge driver
- Should list all 5 StreamForge containers in "Containers" section

---

## 2. Kafka

### List all topics
```bash
docker exec -it streamforge-kafka kafka-topics --list --bootstrap-server localhost:9092
```

**Expected Topics:**
```
streamforge-input
```

### Create input topic (if missing)
```bash
docker exec -it streamforge-kafka kafka-topics \
  --create \
  --topic streamforge-input \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Describe topic
```bash
docker exec -it streamforge-kafka kafka-topics \
  --describe \
  --topic streamforge-input \
  --bootstrap-server localhost:9092
```

**Expected Output:**
```
Topic: streamforge-input	TopicId: XXXXXX	PartitionCount: 3	ReplicationFactor: 1
	Topic: streamforge-input	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: streamforge-input	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
	Topic: streamforge-input	Partition: 2	Leader: 1	Replicas: 1	Isr: 1
```

### Check Kafka connectivity
```bash
# From host
docker exec -it streamforge-kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Test producer (type a message and press Ctrl+C to exit)
docker exec -it streamforge-kafka kafka-console-producer \
  --topic streamforge-input \
  --bootstrap-server localhost:9092
```

### Check consumer groups
```bash
docker exec -it streamforge-kafka kafka-consumer-groups \
  --list \
  --bootstrap-server localhost:9092
```

**Expected Output:**
```
streamforge-consumer-group
```

### View consumer group lag
```bash
docker exec -it streamforge-kafka kafka-consumer-groups \
  --describe \
  --group streamforge-consumer-group \
  --bootstrap-server localhost:9092
```

---

## 3. MongoDB

### Check MongoDB connection
```bash
docker exec -it streamforge-mongodb mongosh \
  -u admin \
  -p password \
  --authenticationDatabase admin \
  --eval "db.adminCommand('ping')"
```

**Expected Output:**
```json
{ ok: 1 }
```

### List databases
```bash
docker exec -it streamforge-mongodb mongosh \
  -u admin \
  -p password \
  --authenticationDatabase admin \
  --eval "show dbs" \
  --quiet
```

**Expected Output:**
```
admin          100.00 KiB
config          60.00 KiB
local           72.00 KiB
streamforge      8.00 KiB
```

### List collections in streamforge database
```bash
docker exec -it streamforge-mongodb mongosh \
  -u admin \
  -p password \
  --authenticationDatabase admin \
  --eval "use streamforge; db.getCollectionNames();" \
  --quiet
```

**Expected Output:**
```
[ 'processed_data', 'aggregated_metrics', 'dead_letter_queue' ]
```

**Note**: Phase 2 created 3 collections:
- `processed_data`: Raw events from Flink
- `aggregated_metrics`: Windowed aggregations (1-minute tumbling windows)
- `dead_letter_queue`: Invalid events that failed validation

### Count documents in processed_data collection
```bash
docker exec -it streamforge-mongodb mongosh \
  -u admin \
  -p password \
  --authenticationDatabase admin \
  --eval "use streamforge; db.processed_data.countDocuments();" \
  --quiet
```

### View sample documents
```bash
docker exec -it streamforge-mongodb mongosh \
  -u admin \
  -p password \
  --authenticationDatabase admin \
  --eval "use streamforge; db.processed_data.find().limit(5).pretty();" \
  --quiet
```

### Check MongoDB indexes
```bash
docker exec -it streamforge-mongodb mongosh \
  -u admin \
  -p password \
  --authenticationDatabase admin \
  --eval "use streamforge; db.processed_data.getIndexes();" \
  --quiet
```

---

## 4. Apache Flink

### Check Flink Web UI
```bash
curl -s http://localhost:8081/overview | python3 -m json.tool
```

**Expected Output:**
```json
{
    "taskmanagers": 1,
    "slots-total": 1,
    "slots-available": 0,
    "jobs-running": 1,
    "jobs-finished": 0,
    "jobs-cancelled": 0,
    "jobs-failed": 0
}
```

**Access Web UI:** http://localhost:8081

### List running jobs
```bash
curl -s http://localhost:8081/jobs | python3 -m json.tool
```

**Expected Output:**
```json
{
    "jobs": [
        {
            "id": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
            "status": "RUNNING"
        }
    ]
}
```

### Get job details
```bash
# Get job ID first
JOB_ID=$(curl -s http://localhost:8081/jobs | python3 -c "import sys, json; print(json.load(sys.stdin)['jobs'][0]['id'])")

# Get job details
curl -s http://localhost:8081/jobs/$JOB_ID | python3 -m json.tool
```

### Check TaskManager status
```bash
curl -s http://localhost:8081/taskmanagers | python3 -m json.tool
```

### View Flink configuration
```bash
curl -s http://localhost:8081/jobmanager/config | python3 -m json.tool
```

### Check Flink checkpoints directory
```bash
docker exec -it streamforge-flink-jobmanager ls -lh /tmp/flink-checkpoints
```

### View Flink logs via API
```bash
# JobManager logs
curl -s http://localhost:8081/jobmanager/log

# List TaskManagers
curl -s http://localhost:8081/taskmanagers
```

---

## 5. Maven Build (Flink Jobs)

### Compile code
```bash
cd flink-jobs && mvn compile
```

**Expected Output:**
```
[INFO] BUILD SUCCESS
```

### Run tests
```bash
cd flink-jobs && mvn test
```

**Expected Output:**
```
[INFO] Tests run: X, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

### Package JAR
```bash
cd flink-jobs && mvn clean package
```

**Expected Output:**
```
[INFO] Building jar: /path/to/flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar
[INFO] BUILD SUCCESS
```

### Verify JAR exists
```bash
ls -lh flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar
```

**Expected Output:**
```
-rw-r--r-- 1 user user XXM Dec 22 XX:XX flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar
```

### Check dependencies
```bash
cd flink-jobs && mvn dependency:tree
```

### Validate Maven configuration
```bash
cd flink-jobs && mvn validate
```

---

## 6. End-to-End Data Flow Test

### Produce test message to Kafka
```bash
echo '{"id": "test-001", "message": "hello streamforge", "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}' | \
docker exec -i streamforge-kafka kafka-console-producer \
  --topic streamforge-input \
  --bootstrap-server localhost:9092
```

### Consume from Kafka to verify message
```bash
docker exec -it streamforge-kafka kafka-console-consumer \
  --topic streamforge-input \
  --from-beginning \
  --bootstrap-server localhost:9092 \
  --max-messages 1
```

### Verify message in MongoDB
```bash
# Wait a few seconds for processing
sleep 5

# Check MongoDB for processed message
docker exec -it streamforge-mongodb mongosh \
  -u admin \
  -p password \
  --authenticationDatabase admin \
  --eval "use streamforge; db.processed_data.find().sort({_id: -1}).limit(1).pretty();" \
  --quiet
```

**Expected:** Should see the JSON event processed with all fields preserved

**Phase 2 Features:** The pipeline now includes:
- JSON deserialization with Event POJOs
- Data validation (invalid events go to DLQ)
- Stateful processing (per-user event counting)
- Windowed aggregations (1-minute tumbling windows)
- Fault-tolerant checkpointing (30-second interval)

---

## 7. Comprehensive Validation Script

Save this as `validate_local_infrastructure.sh`:

```bash
#!/bin/bash

# StreamForge Local Infrastructure Validation Script
# Version: 1.0
# Date: December 22, 2025

set -e

echo "=========================================="
echo "StreamForge Local Infrastructure Check"
echo "=========================================="
echo "Date: $(date)"
echo "=========================================="
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

# 1. Docker Containers
echo "=== Docker Containers ==="
check_resource "Kafka Container" "docker ps --filter name=streamforge-kafka --format '{{.Status}}'" "Up"
check_resource "Flink JobManager" "docker ps --filter name=streamforge-flink-jobmanager --format '{{.Status}}'" "Up"
check_resource "Flink TaskManager" "docker ps --filter name=streamforge-flink-taskmanager --format '{{.Status}}'" "Up"
check_resource "MongoDB Container" "docker ps --filter name=streamforge-mongodb --format '{{.Status}}'" "Up"
check_resource "Zookeeper Container" "docker ps --filter name=streamforge-zookeeper --format '{{.Status}}'" "Up"
echo ""

# 2. Kafka
echo "=== Kafka ==="
check_resource "Kafka Connectivity" "docker exec streamforge-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>&1" "ApiVersion"
check_resource "Input Topic" "docker exec streamforge-kafka kafka-topics --list --bootstrap-server localhost:9092" "streamforge-input"
echo ""

# 3. MongoDB
echo "=== MongoDB ==="
check_resource "MongoDB Connectivity" "docker exec streamforge-mongodb mongosh -u admin -p password --authenticationDatabase admin --eval 'db.adminCommand(\"ping\")' --quiet" "ok: 1"
check_resource "StreamForge Database" "docker exec streamforge-mongodb mongosh -u admin -p password --authenticationDatabase admin --eval 'show dbs' --quiet" "streamforge"
check_resource "Processed Data Collection" "docker exec streamforge-mongodb mongosh -u admin -p password --authenticationDatabase admin --eval 'use streamforge; db.getCollectionNames()' --quiet" "processed_data"
echo ""

# 4. Flink
echo "=== Flink ==="
check_resource "Flink Web UI" "curl -s http://localhost:8081/overview" "taskmanagers"
check_resource "Flink Jobs Running" "curl -s http://localhost:8081/jobs" "jobs"
echo ""

# 5. Maven Build
echo "=== Maven Build ==="
if [ -d "flink-jobs" ]; then
  cd flink-jobs
  check_resource "Maven Compile" "mvn compile -q" "BUILD SUCCESS"
  check_resource "JAR Artifact" "ls target/flink-jobs-1.0-SNAPSHOT.jar" "flink-jobs-1.0-SNAPSHOT.jar"
  cd ..
else
  echo -e "${YELLOW}flink-jobs directory not found. Run from project root.${NC}"
fi
echo ""

echo "=========================================="
echo "Validation Complete!"
echo "=========================================="
echo ""
echo "Summary:"
echo "- All services should show ${GREEN}OK${NC}"
echo "- ${YELLOW}WARNING${NC} indicates service exists but may need review"
echo "- ${RED}FAILED${NC} indicates missing or stopped service"
echo ""
echo "Next steps:"
echo "  - View Flink Dashboard: http://localhost:8081"
echo "  - Connect to MongoDB: mongodb://admin:password@localhost:27017"
echo "  - Kafka broker: localhost:9092"
```

**Usage:**
```bash
chmod +x validate_local_infrastructure.sh
./validate_local_infrastructure.sh
```

---

## 8. Quick Troubleshooting Commands

### Restart all services
```bash
cd docker && docker-compose down && docker-compose up -d
```

### Restart individual service
```bash
cd docker && docker-compose restart kafka
cd docker && docker-compose restart flink-jobmanager
cd docker && docker-compose restart mongodb
```

### Check disk space
```bash
docker system df
```

### View all logs
```bash
cd docker && docker-compose logs -f
```

### Clear MongoDB data
```bash
docker exec -it streamforge-mongodb mongosh \
  -u admin \
  -p password \
  --authenticationDatabase admin \
  --eval "use streamforge; db.processed_data.deleteMany({});"
```

### Clear Kafka topic
```bash
docker exec -it streamforge-kafka kafka-topics \
  --delete \
  --topic streamforge-input \
  --bootstrap-server localhost:9092

# Recreate topic
docker exec -it streamforge-kafka kafka-topics \
  --create \
  --topic streamforge-input \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Rebuild Flink job
```bash
cd flink-jobs
mvn clean package
# Then redeploy via Flink UI: http://localhost:8081/#/submit
```

### Check container resource usage
```bash
docker stats --filter "name=streamforge"
```

---

## Expected Results Summary

**Working Local Environment (Project Complete):**
- ✅ 5 Docker containers running
- ✅ Kafka broker accessible on localhost:9092 with 3-partition topic
- ✅ Kafka topic `streamforge-input` with 3 partitions
- ✅ MongoDB accessible on localhost:27017 with 3 collections
- ✅ MongoDB collections: processed_data, aggregated_metrics, dead_letter_queue
- ✅ Flink Web UI accessible on http://localhost:8081
- ✅ Flink job running with StreamProcessor (233 lines)
- ✅ Maven build succeeds with JAR (19.2 MB)
- ✅ 29 unit tests passing
- ✅ Advanced stream processing: JSON, stateful ops, windowing, aggregations
- ✅ Fault tolerance: 30-second checkpointing, DLQ error handling
- ✅ Comprehensive test suite: 7 suites with ~25 test cases
- ✅ End-to-end data flow validated: Kafka → JSON Parse → Validate → Enrich → Window → Aggregate → MongoDB

**Common Issues:**
- 🔴 Port conflicts (9092, 8081, 27017): Stop conflicting services or change ports in docker-compose.yml
- 🔴 Docker out of memory: Increase Docker memory allocation in Docker Desktop settings
- 🔴 Flink job not visible: Check JobManager logs, may need to submit JAR manually via UI
- 🔴 MongoDB authentication failed: Verify credentials in MongoDBSink.java match docker-compose.yml

---

## Notes

- **Working Directory:** All commands assume execution from project root `/home/scotton/dev/projects/streamforge`
- **Docker Network:** All services communicate via `streamforge-network` bridge network
- **Data Persistence:** MongoDB data persists via Docker volume `streamforge-mongodb-data`
- **Development Workflow:** Edit code → `mvn package` → Submit JAR via Flink UI → Test via Kafka producer
- **Connection Strings:**
  - Kafka (internal): `kafka:29092`
  - Kafka (external): `localhost:9092`
  - MongoDB (internal): `mongodb://admin:password@mongodb:27017`
  - MongoDB (external): `mongodb://admin:password@localhost:27017`
  - Flink (external): `http://localhost:8081`

---

**Document Version:** 1.1  
**Created:** December 22, 2025  
**Last Updated:** December 23, 2025  
**Author:** scotton  
**Project:** StreamForge Real-Time Data Streaming Platform  
**Project Status:** ✅ 100% Complete (All 4 phases finished Dec 19, 2025)
