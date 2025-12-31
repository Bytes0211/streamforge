# Kafka Data Generation Guide

This document describes how to generate test data for the StreamForge Kafka pipeline.

## Quick Start

### Using the Test Script (Recommended)
```bash
./scripts/test-events.sh
```
This generates 100 test events with the expected JSON format and verifies processing in MongoDB.

## Manual Data Generation

### Single Event
Generate a single event using kafka-console-producer:

```bash
echo '{"id":"test-1","type":"click","userId":"user1","value":10.5,"timestamp":'$(date +%s%3N)',"payload":"test data"}' | \
  docker exec -i streamforge-kafka kafka-console-producer \
  --broker-list localhost:9092 \
  --topic streamforge-input
```

### Batch Generation
Generate multiple events in a loop:

```bash
for i in {1..50}; do
  echo "{\"id\":\"event-$i\",\"type\":\"view\",\"userId\":\"user$((i % 5))\",\"value\":$i.0,\"timestamp\":$(date +%s%3N),\"payload\":\"data $i\"}" | \
    docker exec -i streamforge-kafka kafka-console-producer \
    --broker-list localhost:9092 \
    --topic streamforge-input
done
```

### Custom Event Types
Generate different event types (click, view):

```bash
# Click events
for i in {1..25}; do
  echo "{\"id\":\"click-$i\",\"type\":\"click\",\"userId\":\"user$((i % 10))\",\"value\":$(echo "scale=2; $i * 1.5" | bc),\"timestamp\":$(date +%s%3N),\"payload\":\"click event $i\"}" | \
    docker exec -i streamforge-kafka kafka-console-producer \
    --broker-list localhost:9092 \
    --topic streamforge-input
done

# View events
for i in {1..25}; do
  echo "{\"id\":\"view-$i\",\"type\":\"view\",\"userId\":\"user$((i % 10))\",\"value\":$(echo "scale=2; $i * 2.0" | bc),\"timestamp\":$(date +%s%3N),\"payload\":\"view event $i\"}" | \
    docker exec -i streamforge-kafka kafka-console-producer \
    --broker-list localhost:9092 \
    --topic streamforge-input
done
```

## Event Schema

### Required Fields
```json
{
  "id": "unique-id",
  "type": "click|view",
  "userId": "user123",
  "value": 42.5,
  "timestamp": 1703356441000,
  "payload": "optional data"
}
```

### Field Descriptions
- **id** (string): Unique identifier for the event
- **type** (string): Event type, typically "click" or "view"
- **userId** (string): User identifier
- **value** (number): Numeric value associated with the event
- **timestamp** (long): Unix timestamp in milliseconds
- **payload** (string): Additional event data or metadata

## Kafka Configuration

### Topic Details
- **Topic Name**: `streamforge-input`
- **Broker**: `localhost:9092` (external) or `kafka:29092` (internal)
- **Partitions**: 3
- **Consumer Group**: `streamforge-consumer-group`

### Verifying Data
Check if events are in Kafka:

```bash
docker exec streamforge-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic streamforge-input \
  --from-beginning \
  --max-messages 10
```

## Testing Data Processing

### Check MongoDB Collections
After generating data, verify processing:

```bash
# Processed events
docker exec streamforge-mongodb mongosh \
  --quiet \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('streamforge').processed_data.countDocuments({})"

# Dead letter queue
docker exec streamforge-mongodb mongosh \
  --quiet \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('streamforge').dead_letter_queue.countDocuments({})"

# Aggregated metrics
docker exec streamforge-mongodb mongosh \
  --quiet \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('streamforge').aggregated_metrics.countDocuments({})"
```

### View Sample Processed Data
```bash
docker exec streamforge-mongodb mongosh \
  --quiet \
  --username admin \
  --password password \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('streamforge').processed_data.findOne()"
```

## Advanced: Python Data Generator

For more complex data generation, create a Python script:

```python
#!/usr/bin/env python3
import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(100):
    event = {
        "id": f"python-event-{i}",
        "type": random.choice(["click", "view"]),
        "userId": f"user{random.randint(1, 10)}",
        "value": round(random.uniform(1.0, 100.0), 2),
        "timestamp": int(time.time() * 1000),
        "payload": f"Generated event {i}"
    }
    producer.send('streamforge-input', event)
    if (i + 1) % 20 == 0:
        print(f"Sent {i + 1} events...")

producer.flush()
print("Complete!")
```

**Note**: Requires `kafka-python` package: `pip install kafka-python`

## Troubleshooting

### Kafka not accepting connections
Ensure Kafka is running:
```bash
docker ps | grep kafka
```

### Events not being processed
Check Flink job status at http://localhost:8081

### Invalid JSON errors
Validate JSON format:
```bash
echo '{"id":"test","type":"click","userId":"user1","value":1.0,"timestamp":1703356441000,"payload":"test"}' | jq .
```
