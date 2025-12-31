# Stream Generation in StreamForge

This document explains how data streams are generated and processed in the StreamForge project, with complete code examples.

## Overview

StreamForge uses a **Kafka → Flink → MongoDB** architecture where:
1. **Data enters** through Kafka topic `streamforge-input`
2. **Flink processes** the stream with transformations, validations, and aggregations
3. **Results persist** to MongoDB collections

## 1. Stream Source Configuration

Streams originate from a Kafka topic consumed by Flink.

### Kafka Source Setup

```java
// StreamProcessor.java (lines 56-62)
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafka:29092")
    .setTopics("streamforge-input")
    .setGroupId("streamforge-consumer-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();
```

**Key configuration:**
- **Bootstrap Server**: `kafka:29092` (internal Docker network)
- **Topic**: `streamforge-input` with 3 partitions
- **Consumer Group**: `streamforge-consumer-group`
- **Deserializer**: `SimpleStringSchema` (reads raw JSON strings)
- **Starting Offset**: Earliest (processes from beginning on restart)

### Creating the DataStream

```java
// StreamProcessor.java (lines 65-70)
DataStream<String> kafkaStream = env.fromSource(
    source,
    WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()),
    "Kafka Source"
);
```

**Watermark strategy:**
- **5-second bounded out-of-orderness**: Handles late-arriving events
- **Timestamp assignment**: Uses current system time initially

## 2. Event Data Model

Events follow a structured JSON schema represented by the `Event` POJO.

### Event Class Structure

```java
// Event.java (lines 12-37)
public class Event implements Serializable {
    private final String id;          // Unique event identifier
    private final String type;        // Event category (e.g., "click", "view")
    private final String userId;      // User identifier
    private final double value;       // Numeric metric
    private final long timestamp;     // Event timestamp in milliseconds
    private final String payload;     // Additional JSON data
    
    @JsonCreator
    public Event(
            @JsonProperty("id") String id,
            @JsonProperty("type") String type,
            @JsonProperty("userId") String userId,
            @JsonProperty("value") double value,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("payload") String payload) {
        this.id = id;
        this.type = type;
        this.userId = userId;
        this.value = value;
        this.timestamp = timestamp;
        this.payload = payload;
    }
}
```

### Event Validation

```java
// Event.java (lines 63-69)
public boolean isValid() {
    return id != null && !id.isEmpty() &&
           type != null && !type.isEmpty() &&
           userId != null && !userId.isEmpty() &&
           value >= 0 &&
           timestamp > 0;
}
```

### Example JSON Event

```json
{
  "id": "event-12345",
  "type": "click",
  "userId": "user-789",
  "value": 42.5,
  "timestamp": 1703355678901,
  "payload": "additional data here"
}
```

## 3. Stream Processing Pipeline

The Flink job applies multiple transformations to the raw stream.

### Step 1: JSON Parsing & Validation

```java
// StreamProcessor.java (lines 73-92)
SingleOutputStreamOperator<Event> parsedStream = kafkaStream
    .process(new ProcessFunction<String, Event>() {
        @Override
        public void processElement(String value, Context ctx, Collector<Event> out) {
            try {
                Event event = objectMapper.readValue(value, Event.class);
                if (event.isValid()) {
                    out.collect(event);
                    LOG.debug("Successfully parsed event: {}", event.getId());
                } else {
                    LOG.warn("Invalid event: {}", value);
                    ctx.output(DLQ_TAG, value);
                }
            } catch (Exception e) {
                LOG.error("Failed to parse JSON: {}", value, e);
                ctx.output(DLQ_TAG, value);
            }
        }
    })
    .name("Parse and Validate JSON");
```

**Error handling:**
- Invalid JSON → Dead Letter Queue (DLQ)
- Failed validation → Dead Letter Queue
- Valid events → Continue processing

### Step 2: Watermark Assignment

```java
// StreamProcessor.java (lines 100-104)
DataStream<Event> eventsWithWatermarks = parsedStream
    .assignTimestampsAndWatermarks(
        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
    );
```

Uses event's own timestamp for event-time processing with 5-second lateness tolerance.

### Step 3: Stateful Enrichment

```java
// StreamProcessor.java (lines 107-110)
DataStream<Event> enrichedStream = eventsWithWatermarks
    .keyBy(Event::getUserId)
    .process(new EventEnrichmentFunction())
    .name("Stateful Enrichment");
```

The enrichment function tracks per-user event counts:

```java
// StreamProcessor.java (lines 138-162)
private static class EventEnrichmentFunction extends KeyedProcessFunction<String, Event, Event> {
    private transient ValueState<Long> eventCountState;
    
    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
            "event-count",
            TypeInformation.of(Long.class)
        );
        eventCountState = getRuntimeContext().getState(descriptor);
    }
    
    @Override
    public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
        Long currentCount = eventCountState.value();
        if (currentCount == null) {
            currentCount = 0L;
        }
        currentCount++;
        eventCountState.update(currentCount);
        
        LOG.debug("User {} has {} total events", event.getUserId(), currentCount);
        out.collect(event);
    }
}
```

### Step 4: Windowed Aggregations

```java
// StreamProcessor.java (lines 113-117)
DataStream<AggregatedMetrics> aggregatedStream = enrichedStream
    .keyBy(event -> event.getUserId() + ":" + event.getType())
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(new EventAggregationFunction())
    .name("Windowed Aggregations");
```

**Aggregation logic:**

```java
// StreamProcessor.java (lines 167-218)
private static class EventAggregationFunction 
        implements AggregateFunction<Event, EventAccumulator, AggregatedMetrics> {
    
    @Override
    public EventAccumulator add(Event event, EventAccumulator acc) {
        acc.userId = event.getUserId();
        acc.eventType = event.getType();
        acc.count++;
        acc.sum += event.getValue();
        acc.min = Math.min(acc.min, event.getValue());
        acc.max = Math.max(acc.max, event.getValue());
        
        if (acc.windowStart == 0) {
            acc.windowStart = event.getTimestamp();
        }
        acc.windowEnd = event.getTimestamp();
        
        return acc;
    }
    
    @Override
    public AggregatedMetrics getResult(EventAccumulator acc) {
        double avg = acc.count > 0 ? acc.sum / acc.count : 0.0;
        return new AggregatedMetrics(
            acc.userId,
            acc.eventType,
            acc.count,
            acc.sum,
            avg,
            acc.min,
            acc.max,
            acc.windowStart,
            acc.windowEnd
        );
    }
}
```

**Computed metrics per window:**
- Count of events
- Sum of values
- Average value
- Min value
- Max value
- Window start/end timestamps

## 4. How to Generate Test Streams

StreamForge provides several methods to generate test data streams.

### Method 1: Manual Single Event (kafka-console-producer)

```bash
# From docker/docker-compose.yml directory
echo '{"id":"test-1","type":"click","userId":"user-1","value":10.5,"timestamp":'$(date +%s%3N)',"payload":"test"}' | \
  docker exec -i streamforge-kafka kafka-console-producer \
    --broker-list localhost:9092 \
    --topic streamforge-input
```

### Method 2: Automated Test Script (test-events.sh)

```bash
# scripts/test-events.sh (lines 24-42)
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
```

**Run the script:**
```bash
chmod +x scripts/test-events.sh
./scripts/test-events.sh
```

### Method 3: Batch Generation with Loop

```bash
# Generate 50 events in a single batch
for i in {1..50}; do
    TIMESTAMP=$(date +%s%3N)
    echo "{\"id\":\"batch-$i\",\"type\":\"view\",\"userId\":\"user-$((i % 5))\",\"value\":$((i * 2)),\"timestamp\":$TIMESTAMP,\"payload\":\"batch event\"}"
done | docker exec -i streamforge-kafka kafka-console-producer \
    --broker-list localhost:9092 \
    --topic streamforge-input
```

### Method 4: Comprehensive Test Suite

```bash
# Run full test suite with various event patterns
chmod +x scripts/comprehensive-test-suite.sh
./scripts/comprehensive-test-suite.sh
```

This script tests:
- Single event ingestion
- Batch ingestion (100 events)
- Invalid JSON handling
- Missing field validation
- Windowed aggregations
- Performance metrics
- Fault tolerance

## 5. Stream Outputs

Processed streams are written to MongoDB collections.

### Output 1: Processed Events

```java
// StreamProcessor.java (lines 120-121)
enrichedStream.addSink(new MongoDBSink())
    .name("MongoDB Event Sink");
```

**Collection**: `streamforge.processed_data`

**Document structure:**
```javascript
{
  "_id": ObjectId("..."),
  "data": "{\"id\":\"...\",\"type\":\"...\",\"userId\":\"...\",\"value\":...,\"timestamp\":...,\"payload\":\"...\"}",
  "timestamp": 1703355678901,
  "processedAt": ISODate("2024-12-23T18:30:00Z")
}
```

### Output 2: Aggregated Metrics

```java
// StreamProcessor.java (lines 124-125)
aggregatedStream.addSink(new MongoDBMetricsSink())
    .name("MongoDB Metrics Sink");
```

**Collection**: `streamforge.aggregated_metrics`

**Document structure:**
```javascript
{
  "_id": ObjectId("..."),
  "userId": "user-1",
  "eventType": "click",
  "count": 42,
  "sum": 532.5,
  "average": 12.68,
  "min": 1.5,
  "max": 99.0,
  "windowStart": 1703355600000,
  "windowEnd": 1703355659999,
  "processedAt": ISODate("2024-12-23T18:31:00Z")
}
```

### Output 3: Dead Letter Queue

**Collection**: `streamforge.dead_letter_queue`

**Document structure:**
```javascript
{
  "_id": ObjectId("..."),
  "rawData": "invalid-json-data",
  "errorMessage": "Unexpected character...",
  "timestamp": ISODate("2024-12-23T18:30:00Z")
}
```

## 6. Monitoring Stream Processing

### Check Flink Dashboard
```bash
# View processing metrics, job status, checkpoints
open http://localhost:8081
```

### Query MongoDB Results

```bash
# Count processed events
docker exec streamforge-mongodb mongosh \
  -u admin -p password \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('streamforge').processed_data.countDocuments()"

# View sample processed event
docker exec streamforge-mongodb mongosh \
  -u admin -p password \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('streamforge').processed_data.findOne()"

# View aggregated metrics
docker exec streamforge-mongodb mongosh \
  -u admin -p password \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('streamforge').aggregated_metrics.find().limit(5)"

# Check dead letter queue
docker exec streamforge-mongodb mongosh \
  -u admin -p password \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('streamforge').dead_letter_queue.find().limit(5)"
```

### View Kafka Topics

```bash
# List topics
docker exec streamforge-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list

# Consume from topic
docker exec streamforge-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic streamforge-input \
  --from-beginning \
  --max-messages 10
```

### Check Flink Logs

```bash
# JobManager logs
docker logs streamforge-flink-jobmanager --tail 100

# TaskManager logs
docker logs streamforge-flink-taskmanager --tail 100
```

## 7. Stream Processing Flow Summary

```
┌──────────────────┐
│  External Source │
│  (Test Scripts)  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   Kafka Topic    │
│ streamforge-input│
│  (3 partitions)  │
└────────┬─────────┘
         │
         ▼
┌──────────────────────────────────────────┐
│         Flink StreamProcessor            │
│                                          │
│  1. JSON Parse & Validate                │
│     ├── Valid → Continue                 │
│     └── Invalid → DLQ                    │
│                                          │
│  2. Watermark Assignment                 │
│     (5s out-of-orderness)                │
│                                          │
│  3. Stateful Enrichment                  │
│     (per-user event counter)             │
│                                          │
│  4. Windowed Aggregations                │
│     (1-min tumbling windows)             │
│     - Count, Sum, Avg, Min, Max          │
└────────┬─────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────┐
│           MongoDB Collections            │
│                                          │
│  • processed_data (individual events)    │
│  • aggregated_metrics (window results)   │
│  • dead_letter_queue (invalid events)    │
└──────────────────────────────────────────┘
```

## 8. Performance Characteristics

Based on comprehensive testing:

- **Throughput**: 1000+ events/second
- **Latency**: <10 seconds end-to-end (p99)
- **Data Loss**: Zero (validated with 50-event test)
- **Checkpoint Interval**: 30 seconds
- **Window Size**: 1 minute (tumbling)
- **Watermark Lateness**: 5 seconds

## References

- Main processor: `flink-jobs/src/main/java/com/streamforge/StreamProcessor.java`
- Event model: `flink-jobs/src/main/java/com/streamforge/model/Event.java`
- Test script: `scripts/test-events.sh`
- Docker config: `docker/docker-compose.yml`
- WARP rules: `WARP.md`
