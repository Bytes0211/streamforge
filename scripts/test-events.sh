#!/bin/bash

# Integration test script for StreamForge
# Generates and sends test events to Kafka, then verifies processing in MongoDB

set -e

echo "===  StreamForge Integration Test ==="
echo ""

# Kafka broker
KAFKA_BROKER="localhost:9092"
TOPIC="streamforge-input"

# MongoDB connection
MONGO_HOST="localhost"
MONGO_PORT="27017"
MONGO_USER="admin"
MONGO_PASS="password"

echo "Step 1: Generating test events..."

# Generate 100 test events
for i in {1..100}; do
    TIMESTAMP=$(date +%s%3N)
    EVENT_TYPE=$( [ $((i % 2)) -eq 0 ] && echo "click" || echo "view" )
    USER_ID="user$((i % 10))"
    VALUE=$(echo "scale=2; $i * 1.5" | bc)
    
    EVENT_JSON=$(cat <<EOF
{"id":"test-$i","type":"$EVENT_TYPE","userId":"$USER_ID","value":$VALUE,"timestamp":$TIMESTAMP,"payload":"test payload $i"}
EOF
)
    
    echo "$EVENT_JSON" | docker exec -i streamforge-kafka kafka-console-producer \
        --broker-list $KAFKA_BROKER \
        --topic $TOPIC > /dev/null 2>&1
    
    if [ $((i % 20)) -eq 0 ]; then
        echo "  Sent $i events..."
    fi
done

echo "Step 2: Waiting for processing (10 seconds)..."
sleep 10

echo "Step 3: Verifying processed data in MongoDB..."

# Check processed_data collection
PROCESSED_COUNT=$(docker exec streamforge-mongodb mongosh \
    --quiet \
    --username $MONGO_USER \
    --password $MONGO_PASS \
    --authenticationDatabase admin \
    --eval "db.getSiblingDB('streamforge').processed_data.countDocuments({})" 2>/dev/null)

echo "  Processed events: $PROCESSED_COUNT"

# Check dead letter queue
DLQ_COUNT=$(docker exec streamforge-mongodb mongosh \
    --quiet \
    --username $MONGO_USER \
    --password $MONGO_PASS \
    --authenticationDatabase admin \
    --eval "db.getSiblingDB('streamforge').dead_letter_queue.countDocuments({})" 2>/dev/null)

echo "  DLQ events: $DLQ_COUNT"

# Check aggregated metrics (may not be present if window hasn't closed)
METRICS_COUNT=$(docker exec streamforge-mongodb mongosh \
    --quiet \
    --username $MONGO_USER \
    --password $MONGO_PASS \
    --authenticationDatabase admin \
    --eval "db.getSiblingDB('streamforge').aggregated_metrics.countDocuments({})" 2>/dev/null)

echo "  Aggregated metrics: $METRICS_COUNT"

echo ""
echo "Step 4: Sample processed event..."
docker exec streamforge-mongodb mongosh \
    --quiet \
    --username $MONGO_USER \
    --password $MONGO_PASS \
    --authenticationDatabase admin \
    --eval "db.getSiblingDB('streamforge').processed_data.findOne()" 2>/dev/null

echo ""
echo "=== Test Complete ==="
echo ""
echo "Summary:"
echo "  - Sent: 100 events"
echo "  - Processed: $PROCESSED_COUNT events"
echo "  - Failed (DLQ): $DLQ_COUNT events"
echo "  - Metrics: $METRICS_COUNT records"

# Validate results
if [ "$PROCESSED_COUNT" -gt 50 ]; then
    echo ""
    echo "✓ TEST PASSED: At least 50% of events were processed"
    exit 0
else
    echo ""
    echo "✗ TEST FAILED: Less than 50% of events were processed"
    exit 1
fi
