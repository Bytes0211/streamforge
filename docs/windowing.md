# Windowing Overview

Windowing in StreamForge divides the continuous stream into finite logical buckets, enabling time-based aggregations and metrics computation. The project uses **tumbling windows** with **watermark-based late event handling**.

## 1. Tumbling Windows

Tumbling windows are non-overlapping, fixed-size windows that partition the stream into disjoint time intervals. In StreamForge, **1-minute tumbling windows** are applied during aggregation:

```java
DataStream<AggregatedMetrics> aggregatedStream = enrichedStream
    .keyBy(event -> event.getUserId() + ":" + event.getType())
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(new EventAggregationFunction())
    .name("Windowed Aggregations");
```

### How It Works

1. **Keying**: Events are grouped by `userId:type` combination
   - Example: `user-456:click`, `user-789:purchase`
   - Each key maintains separate windows

2. **Window Assignment**: Each event is assigned to exactly one 1-minute window based on its event time
   - Event at timestamp `1671234567890` → assigned to window `[1671234540000, 1671234600000)`
   - Event at timestamp `1671234600100` → assigned to next window `[1671234600000, 1671234660000)`

3. **Aggregation**: Within each window, the `EventAggregationFunction` computes metrics:

```java
private static class EventAggregationFunction 
        implements AggregateFunction<Event, EventAccumulator, AggregatedMetrics> {
    
    @Override
    public EventAccumulator createAccumulator() {
        return new EventAccumulator();
    }
    
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
    
    @Override
    public EventAccumulator merge(EventAccumulator a, EventAccumulator b) {
        a.count += b.count;
        a.sum += b.sum;
        a.min = Math.min(a.min, b.min);
        a.max = Math.max(a.max, b.max);
        a.windowStart = Math.min(a.windowStart, b.windowStart);
        a.windowEnd = Math.max(a.windowEnd, b.windowEnd);
        return a;
    }
}
```

4. **Metrics Computed Per Window**:
   - **count**: Total number of events
   - **sum**: Total value across all events
   - **average**: Mean value (sum / count)
   - **min**: Minimum value
   - **max**: Maximum value
   - **windowStart**: First event timestamp in window
   - **windowEnd**: Last event timestamp in window

### Window Timeline Example

```
Timeline:
├─────────────────────┼─────────────────────┼─────────────────────┤
│  Window 1           │  Window 2           │  Window 3           │
│ [00:00 - 01:00)     │ [01:00 - 02:00)     │ [02:00 - 03:00)     │
├─────────────────────┼─────────────────────┼─────────────────────┤

Event Stream:
  event1(userId=u1, value=10) @ 00:15 → Window 1
  event2(userId=u1, value=20) @ 00:45 → Window 1
  event3(userId=u1, value=15) @ 01:30 → Window 2
  event4(userId=u2, value=25) @ 00:50 → Window 1

Window 1 Results (userId=u1):
  count=2, sum=30, avg=15, min=10, max=20

Window 2 Results (userId=u1):
  count=1, sum=15, avg=15, min=15, max=15
```

## 2. Watermarking Strategy

Watermarks are special markers in the stream that indicate progress in event time. StreamForge uses **bounded out-of-orderness watermarking** to handle late-arriving events:

```java
DataStream<Event> eventsWithWatermarks = parsedStream
    .assignTimestampsAndWatermarks(
        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
    );
```

### How Watermarking Works

1. **Timestamp Assignment**: Each event's timestamp is extracted from `event.getTimestamp()`
   - Uses the original event time from the source, not processing time

2. **Out-of-Orderness Tolerance**: 5-second window allows events arriving up to 5 seconds late
   - Watermark at time T = max(event_time) - 5 seconds
   - Events arriving after watermark passes are considered "late"

3. **Window Closure**: Windows close when the watermark advances past their end
   - Window `[00:00 - 01:00)` closes when watermark reaches `01:00`
   - Late events arriving after window closes are ignored or sidelined

### Timeline with Watermarking

```
Event Time: ──────────────────────────────────────────────→

Event Stream (with processing delays):
  event1 (time=00:10)  ← arrives on-time
  event2 (time=00:55)  ← arrives on-time
  event3 (time=00:05)  ← arrives 50ms late (within 5s tolerance)
  event4 (time=01:30)  ← arrives on-time
  watermark @ 01:25    ← current watermark

Window [00:00 - 01:00) Status:
  - events 1, 2, 3 included (all within tolerance)
  - window CLOSED (watermark passed 01:00)
  - any new events with time < 01:00 are late and dropped

Window [01:00 - 02:00) Status:
  - event 4 included
  - window OPEN (watermark at 01:25, still before 02:00)
```

### Watermark Propagation in Kafka Source

The Kafka source also applies watermarking:

```java
DataStream<String> kafkaStream = env.fromSource(
    source,
    WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()),
    "Kafka Source"
);
```

**Note**: This initial watermark uses `System.currentTimeMillis()` (processing time), which is overridden later when proper event time assignment occurs.

## 3. Key Window Concepts

### Window Trigger
Windows trigger (emit results) when:
- Watermark advances past the window end time
- No more on-time or late events can arrive

### Event Time vs Processing Time
- **Event Time**: When the event actually occurred (from event.timestamp)
- **Processing Time**: When Flink processes the event
- StreamForge uses **event time** for all windowing to ensure consistent results regardless of network delays

### Accumulation Mode
Flink uses **ACCUMULATE** mode (default):
- Window result includes all events assigned to that window
- Once window closes, no more updates are emitted
- Late events after window closure are discarded

## 4. Output to MongoDB

Aggregated metrics from windows are persisted to MongoDB:

```java
aggregatedStream.addSink(new MongoDBMetricsSink())
    .name("MongoDB Metrics Sink");
```

MongoDB stores each window result as a document containing the computed metrics, enabling historical analysis of time-series data.

## Summary

- **Windowing Type**: 1-minute tumbling windows
- **Window Key**: userId:eventType combination
- **Late Event Tolerance**: 5 seconds (via watermarks)
- **Aggregations**: count, sum, average, min, max per window
- **Timestamp Source**: Event's embedded timestamp field
- **Output**: Windowed metrics to MongoDB for persistence
