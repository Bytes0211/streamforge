# Data Stream Generation Overview

The data stream in StreamForge is generated through a **Kafka → Flink → MongoDB pipeline**.

## 1. Data Source: Kafka Topic

Data arrives via Kafka topic `streamforge-input`. The Flink job consumes raw JSON events:

```java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafka:29092")
    .setTopics("streamforge-input")
    .setGroupId("streamforge-consumer-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();
```

The Kafka source reads from topic `streamforge-input` as plain text JSON strings, starting from the earliest available offset.

## 2. Event Data Structure

Events are JSON objects with this schema:

```java
public class Event implements Serializable {
    private final String id;          // unique event ID
    private final String type;        // event category/type
    private final String userId;      // user identifier
    private final double value;       // numeric metric value
    private final long timestamp;     // event timestamp in ms
    private final String payload;     // additional JSON data
}
```

A typical JSON event looks like:

```json
{
  "id": "event-123",
  "type": "click",
  "userId": "user-456",
  "value": 42.5,
  "timestamp": 1671234567890,
  "payload": "{...}"
}
```

## 3. Stream Processing Pipeline

The Flink job applies transformations in sequence:

### Step 1: JSON Parsing & Validation

```java
kafkaStream.process(new ProcessFunction<String, Event>() {
    @Override
    public void processElement(String value, Context ctx, Collector<Event> out) {
        try {
            Event event = objectMapper.readValue(value, Event.class);
            if (event.isValid()) {
                out.collect(event);
            } else {
                ctx.output(DLQ_TAG, value);  // invalid events to dead letter queue
            }
        } catch (Exception e) {
            ctx.output(DLQ_TAG, value);  // parse errors to DLQ
        }
    }
})
```

Raw JSON strings are deserialized into Event objects. Invalid or malformed events are routed to a dead letter queue for error handling.

### Step 2: Stateful Enrichment

```java
DataStream<Event> enrichedStream = eventsWithWatermarks
    .keyBy(Event::getUserId)
    .process(new EventEnrichmentFunction())
```

Each event is keyed by `userId` and enriched with a running event counter stored in Flink state. This enables tracking of cumulative event counts per user across the stream.

### Step 3: Windowed Aggregations

```java
DataStream<AggregatedMetrics> aggregatedStream = enrichedStream
    .keyBy(event -> event.getUserId() + ":" + event.getType())
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(new EventAggregationFunction())
```

Events are grouped by `userId:type` and aggregated into 1-minute tumbling windows. For each window, the following metrics are computed:
- **count**: number of events
- **sum**: total value
- **average**: mean value
- **min**: minimum value
- **max**: maximum value

## 4. Data Sinks

Two MongoDB sinks persist the processed data:

### Raw Events Sink

Enriched events (after stateful processing) are written to the `processed_data` collection in MongoDB:

```java
enrichedStream.addSink(new MongoDBSink())

// Document structure:
{
  "data": "{\"id\":\"...\",\"type\":\"...\",\"userId\":\"...\",\"value\":...,\"payload\":\"...\"}",
  "timestamp": <event-timestamp>,
  "processedAt": <insertion-time>
}
```

### Aggregated Metrics Sink

Windowed aggregates are written to a separate MongoDB sink (MongoDBMetricsSink), storing:

```java
AggregatedMetrics {
  userId,
  eventType,
  count,
  sum,
  average,
  min,
  max,
  windowStart,
  windowEnd
}
```

## 5. Data Flow Summary

```
External Source → Kafka (streamforge-input)
  ↓
Flink StreamProcessor
  ├── JSON Parsing & Validation
  ├── Timestamp Assignment & Watermarking
  ├── Stateful Enrichment (event counting per user)
  ├── Windowed Aggregation (1-minute windows by userId:type)
  └── Error Handling (dead letter queue)
  ↓
MongoDB Sinks
  ├── processed_data collection (individual enriched events)
  └── metrics collection (aggregated metrics by window)
```

## 6. Fault Tolerance & Reliability

- **Checkpointing**: Flink creates checkpoints every 30 seconds for fault recovery
- **Watermarking**: 5-second bounded out-of-orderness windows handle late-arriving events
- **Error Handling**: Malformed or invalid events are captured in a dead letter queue for later analysis
- **State Backend**: Filesystem-based state with external checkpoint cleanup retention for debugging

## Infrastructure

All services communicate via the `streamforge-network` Docker bridge network:
- **Kafka**: `kafka:29092` (internal), `localhost:9092` (external)
- **MongoDB**: `mongodb://admin:password@mongodb:27017`
- **Flink Dashboard**: http://localhost:8081
