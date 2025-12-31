# Flink Stream Processing Architecture

This document details the Apache Flink stream processing implementation in StreamForge, including data flow, processing stages, and configuration.

## Overview

StreamForge uses Apache Flink 1.18.0 to process real-time event streams from Kafka, performing transformations, aggregations, and stateful operations before writing results to MongoDB.

### Technology Stack
- **Flink Version**: 1.18.0 (Java 11)
- **Kafka Connector**: 3.0.1-1.18
- **MongoDB Driver**: 4.11.1
- **JSON Processing**: Jackson 2.15.3
- **Build Tool**: Maven with Shade plugin

## Data Flow Architecture

```
Kafka Topic (streamforge-input)
    ↓
Kafka Source (with watermarks)
    ↓
JSON Parsing & Validation
    ├─→ Valid Events → Enrichment Pipeline
    └─→ Invalid Events → Dead Letter Queue
         ↓
Stateful Enrichment (per user)
    ↓
Windowed Aggregations (1-minute tumbling windows)
    ├─→ Enriched Events → MongoDB (processed_data)
    └─→ Aggregated Metrics → MongoDB (aggregated_metrics)
```

## Processing Stages

### 1. Kafka Source Configuration

**Location**: `StreamProcessor.java` (lines 56-62)

```java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafka:29092")
    .setTopics("streamforge-input")
    .setGroupId("streamforge-consumer-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();
```

**Configuration**:
- **Broker**: Internal Docker network address `kafka:29092`
- **Topic**: `streamforge-input` (3 partitions)
- **Consumer Group**: `streamforge-consumer-group`
- **Offset Strategy**: Reads from earliest available message
- **Deserializer**: Simple string schema (raw JSON)

**Watermark Strategy**:
- Out-of-order tolerance: 5 seconds
- Timestamp assignment: Current system time on ingestion

### 2. JSON Parsing & Validation

**Location**: `StreamProcessor.java` (lines 73-92)

**Process Function**:
- Deserializes JSON strings to `Event` objects using Jackson
- Validates event fields via `Event.isValid()` method
- Routes valid events to main stream
- Routes invalid/malformed events to Dead Letter Queue (DLQ)

**Validation Rules** (in `Event.java`):
- `id`: Non-null and non-empty
- `type`: Non-null and non-empty
- `userId`: Non-null and non-empty
- `value`: Greater than or equal to 0
- `timestamp`: Greater than 0

**Error Handling**:
- JSON parse errors → DLQ
- Invalid field values → DLQ
- All errors logged with SLF4J

### 3. Watermark Assignment

**Location**: `StreamProcessor.java` (lines 100-104)

**Configuration**:
- Strategy: Bounded out-of-orderness
- Maximum lateness: 5 seconds
- Timestamp extractor: Uses `event.getTimestamp()` field
- Enables event-time processing for windowing

### 4. Stateful Enrichment

**Location**: `StreamProcessor.java` (EventEnrichmentFunction, lines 138-162)

**Purpose**: Track per-user event counts across the lifetime of the job

**Implementation**:
- Uses Flink's `KeyedProcessFunction` with `ValueState`
- State keyed by `userId`
- Maintains running count of events per user
- State is fault-tolerant via checkpointing

**State Descriptor**:
- Name: `event-count`
- Type: `Long`
- Scope: Per user (keyed state)

### 5. Windowed Aggregations

**Location**: `StreamProcessor.java` (lines 113-117)

**Window Configuration**:
- Type: Tumbling Event Time Windows
- Duration: 1 minute
- Keying: Composite key of `userId:eventType`

**Aggregate Function** (lines 167-218):

Computes the following metrics per window:
- **count**: Number of events in window
- **sum**: Sum of all event values
- **avg**: Average event value
- **min**: Minimum event value
- **max**: Maximum event value
- **windowStart**: Earliest event timestamp in window
- **windowEnd**: Latest event timestamp in window

**Output**: `AggregatedMetrics` objects written to MongoDB

### 6. Sink Operations

#### 6.1 MongoDB Event Sink

**Location**: `MongoDBSink.java`

**Target Collection**: `processed_data`

**Document Schema**:
```json
{
  "data": "{\"id\":\"...\",\"type\":\"...\",\"userId\":\"...\",\"value\":...,\"payload\":\"...\"}",
  "timestamp": 1703356441000,
  "processedAt": "2024-12-23T18:00:00Z"
}
```

**Lifecycle**:
- `open()`: Establishes MongoDB connection with connection pooling
- `invoke()`: Converts Event to Document and inserts into collection
- `close()`: Closes MongoDB client connection

**Connection String**: `mongodb://admin:password@mongodb:27017`

#### 6.2 MongoDB Metrics Sink

**Location**: `MongoDBMetricsSink.java`

**Target Collection**: `aggregated_metrics`

**Document Schema**:
```json
{
  "userId": "user123",
  "eventType": "click",
  "count": 42,
  "sum": 127.5,
  "avg": 3.04,
  "min": 1.0,
  "max": 10.5,
  "windowStart": "2024-12-23T18:00:00Z",
  "windowEnd": "2024-12-23T18:01:00Z",
  "processedAt": "2024-12-23T18:01:05Z"
}
```

#### 6.3 Dead Letter Queue Sink

**Location**: `DeadLetterQueueSink.java`

**Target Collection**: `dead_letter_queue`

**Document Schema**:
```json
{
  "rawData": "{invalid json or event data}",
  "failedAt": "2024-12-23T18:00:00Z",
  "errorType": "PARSE_ERROR"
}
```

**Error Strategy**: Does not throw exceptions on DLQ write failures to prevent pipeline crashes

## Data Models

### Event Model

**Location**: `model/Event.java`

**Fields**:
- `id` (String): Unique event identifier
- `type` (String): Event type (e.g., "click", "view")
- `userId` (String): User identifier
- `value` (double): Numeric value associated with event
- `timestamp` (long): Unix timestamp in milliseconds
- `payload` (String): Additional event metadata

**Features**:
- Implements `Serializable` for Flink serialization
- Jackson annotations for JSON deserialization
- Built-in validation method
- Immutable fields (final)

### AggregatedMetrics Model

**Location**: `model/AggregatedMetrics.java`

**Fields**:
- `userId` (String)
- `eventType` (String)
- `count` (long)
- `sum` (double)
- `avg` (double)
- `min` (double)
- `max` (double)
- `windowStart` (long)
- `windowEnd` (long)

**Features**:
- Immutable aggregation results
- Represents output of windowed computations

## Fault Tolerance & State Management

### Checkpointing Configuration

**Location**: `StreamProcessor.java` (lines 47-53)

**Settings**:
- **Checkpoint Interval**: 30 seconds
- **Min Pause Between Checkpoints**: 10 seconds
- **Checkpoint Timeout**: 60 seconds
- **Max Concurrent Checkpoints**: 1
- **Cleanup Policy**: Retain on cancellation

**State Backend**:
- Type: Filesystem
- Checkpoint Directory: `/tmp/flink-checkpoints` (in container)
- Savepoint Directory: `/tmp/flink-savepoints` (in container)
- Volumes: Persisted via Docker volumes

### Exactly-Once Semantics

While the Flink job has checkpointing enabled, **end-to-end exactly-once** is not guaranteed because:
- MongoDB sinks use at-least-once delivery
- No transactional writes to MongoDB
- Duplicate events may occur on recovery

For exactly-once, would need:
- Transactional MongoDB sink or idempotent writes
- Kafka transactional producer (if writing back to Kafka)

## Parallelism & Scalability

### Task Manager Configuration

**Location**: `docker/docker-compose.yml`

**Settings**:
- **Task Slots**: 4 per TaskManager
- **TaskManager Instances**: 1 (can be scaled)
- **JobManager**: Single instance

**Scaling Considerations**:
- Kafka topic has 3 partitions → max 3 parallel Kafka source tasks
- Window operators can be parallelized by key
- MongoDB sinks can run in parallel (connection pooling)

### Resource Allocation

Controlled via Docker Compose:
```yaml
flink-taskmanager:
  scale: 1  # Can increase for more parallelism
  environment:
    taskmanager.numberOfTaskSlots: 4
```

## Build & Deployment

### Maven Build

**Build Commands**:
```bash
cd flink-jobs
mvn clean package
```

**Output**: `target/flink-jobs-1.0-SNAPSHOT.jar`

**Shade Plugin**:
- Creates fat JAR with all dependencies
- Main class: `com.streamforge.StreamProcessor`
- Excludes Flink core dependencies (provided scope)
- Includes Kafka connector, MongoDB driver, Jackson

### Job Submission

#### Via Flink Dashboard (Recommended)
1. Navigate to http://localhost:8081
2. Click "Submit New Job"
3. Upload `flink-jobs-1.0-SNAPSHOT.jar`
4. Entry Class: `com.streamforge.StreamProcessor`
5. Click "Submit"

#### Via Flink CLI
```bash
docker exec streamforge-flink-jobmanager flink run \
  -c com.streamforge.StreamProcessor \
  /opt/flink/usrlib/target/flink-jobs-1.0-SNAPSHOT.jar
```

## Monitoring & Observability

### Flink Dashboard

**URL**: http://localhost:8081

**Available Metrics**:
- Job status and uptime
- Task parallelism and distribution
- Checkpoint statistics (duration, size, success rate)
- Backpressure indicators
- Records processed/sent
- Exception history

### Logging

**Framework**: SLF4J with simple backend

**Log Locations**:
- JobManager logs: `docker logs streamforge-flink-jobmanager`
- TaskManager logs: `docker logs streamforge-flink-taskmanager`

**Log Levels**:
- DEBUG: Detailed event processing (per-event logs)
- INFO: Job lifecycle, connections
- WARN: Invalid events sent to DLQ
- ERROR: Critical failures (connection errors, sink failures)

### Key Metrics to Monitor

1. **Checkpoint Duration**: Should be < 60 seconds
2. **Checkpoint Failures**: Should be 0 or minimal
3. **Backpressure**: Should be none/low (green indicators)
4. **Records In/Out**: Should match expected throughput
5. **DLQ Rate**: High rate indicates data quality issues

## Performance Tuning

### Kafka Source Optimization
- Increase `max.poll.records` for higher throughput
- Tune `fetch.min.bytes` and `fetch.max.wait.ms`
- Consider enabling Kafka consumer metrics

### Window Performance
- Adjust window size based on event rate
- Consider sliding windows for overlapping analytics
- Use session windows for user session analysis

### MongoDB Sink Optimization
- Increase connection pool size
- Batch writes using `addSink()` with batching sink
- Consider async MongoDB driver for non-blocking writes
- Add indexes on queried fields

### Parallelism Tuning
- Match Kafka partitions (currently 3)
- Increase TaskManager instances for scale
- Monitor task distribution across slots

## Testing

### Unit Tests

**Location**: `flink-jobs/src/test/java`

**Test Framework**: JUnit 4 with Mockito

**Coverage**:
- Event model validation
- Aggregation function logic
- State management
- Sink operations (with in-memory MongoDB)

**Run Tests**:
```bash
cd flink-jobs
mvn test
```

### Integration Tests

**Script**: `scripts/test-events.sh`

**Process**:
1. Generates 100 test events
2. Sends to Kafka
3. Waits 10 seconds for processing
4. Verifies data in MongoDB collections
5. Validates processing rate (>50%)

**Run Integration Test**:
```bash
./scripts/test-events.sh
```

## Troubleshooting

### Job Not Starting
- Check JobManager logs for errors
- Verify JAR is uploaded correctly
- Ensure Kafka is accessible at `kafka:29092`
- Verify MongoDB is running and accessible

### No Data in MongoDB
- Check TaskManager logs for processing errors
- Verify Flink job is running (Dashboard)
- Confirm events are in Kafka topic
- Check DLQ collection for failed events

### High Checkpoint Duration
- Check state backend performance
- Consider RocksDB state backend for large state
- Verify disk I/O is not saturated
- Reduce checkpoint interval

### Out of Memory Errors
- Increase TaskManager heap size in Docker Compose
- Reduce parallelism
- Check for state accumulation (unbounded windows)

### Backpressure
- Slow MongoDB writes (check connection pool)
- Increase TaskManager resources
- Add more TaskManager instances
- Optimize sink operations

## Advanced Features

### Custom Watermark Strategies
Current: Bounded out-of-orderness (5s)

Alternatives:
- **Monotonous timestamps**: For perfectly ordered streams
- **Custom watermark generators**: For domain-specific timing
- **Idle source timeout**: For slow/inactive partitions

### State TTL
Not currently configured, but can be added to prevent unbounded state growth:

```java
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(24))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .build();
```

### Side Outputs
Currently used for DLQ. Can be extended for:
- Late data handling
- Monitoring/audit streams
- Multiple output streams based on business logic

### Async I/O
For non-blocking external service calls:
- Database lookups
- REST API enrichment
- Cache queries

## Future Enhancements

1. **Exactly-Once MongoDB Writes**
   - Implement idempotent writes using event IDs
   - Add write-ahead log for transactional guarantees

2. **Schema Evolution**
   - Avro or Protobuf serialization
   - Schema Registry integration

3. **Dynamic Configuration**
   - Externalize configuration to properties file
   - Support configuration updates without redeployment

4. **Advanced Analytics**
   - Session windows for user behavior
   - CEP (Complex Event Processing) for pattern detection
   - ML model integration for real-time predictions

5. **Metrics Export**
   - Prometheus metrics reporter
   - Grafana dashboards
   - Custom business metrics

6. **Multi-Tenancy**
   - Per-tenant stream isolation
   - Dynamic topic routing
   - Tenant-specific processing rules
