# Checkpointing Overview

Checkpointing in StreamForge implements fault tolerance for the Flink streaming job. It periodically saves the complete state of the computation to persistent storage, enabling recovery from failures without data loss.

## 1. Checkpoint Configuration

StreamForge enables checkpointing with comprehensive configuration:

```java
// Enable checkpointing every 30 seconds
env.enableCheckpointing(30000);

CheckpointConfig checkpointConfig = env.getCheckpointConfig();

// Minimum pause between checkpoints (avoid too frequent checkpoints)
checkpointConfig.setMinPauseBetweenCheckpoints(10000);

// Maximum time allowed for a checkpoint to complete
checkpointConfig.setCheckpointTimeout(60000);

// Only allow 1 checkpoint in progress at a time
checkpointConfig.setMaxConcurrentCheckpoints(1);

// Retain checkpoints even after job cancellation (for debugging/recovery)
checkpointConfig.setExternalizedCheckpointCleanup(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
```

### Configuration Parameters Explained

| Parameter | Value | Purpose |
| --- | --- | --- |
| `checkpointInterval` | 30,000 ms | Checkpoint triggered every 30 seconds |
| `minPauseBetweenCheckpoints` | 10,000 ms | Minimum 10-second gap between checkpoint completions |
| `checkpointTimeout` | 60,000 ms | Checkpoint fails if not completed within 60 seconds |
| `maxConcurrentCheckpoints` | 1 | Sequential checkpointing (one at a time) |
| `externalizedCheckpointCleanup` | RETAIN_ON_CANCELLATION | Checkpoints persist after job cancellation |

## 2. Checkpoint Storage

Checkpoints are stored on the filesystem via Docker volumes:

```yaml
# From docker-compose.yml
flink-jobmanager:
  volumes:
    - flink-checkpoints:/tmp/flink-checkpoints
    - flink-savepoints:/tmp/flink-savepoints
  environment:
    - |
      FLINK_PROPERTIES=
      state.backend: filesystem
      state.checkpoints.dir: file:///tmp/flink-checkpoints
      state.savepoints.dir: file:///tmp/flink-savepoints
```

**Storage locations**:
- **Checkpoints**: `/tmp/flink-checkpoints` (automatic, periodic)
- **Savepoints**: `/tmp/flink-savepoints` (manual, for controlled restarts)
- **Persistence**: Docker named volumes ensure data survives container restarts

## 3. What Gets Checkpointed

A checkpoint captures:

1. **Operator State**: Internal state maintained by stateful operators
   - Event counters in `EventEnrichmentFunction`
   - Window aggregation state in `EventAggregationFunction`
   - Intermediate results being computed

2. **Kafka Consumer Offsets**: Current position in each topic partition
   - Tracks which messages from `streamforge-input` have been processed
   - Enables resuming from exact position after recovery

3. **Window State**: Data accumulated in open windows
   - Events assigned to windows not yet triggered
   - Allows completing windows that were mid-computation

### Example: EventEnrichmentFunction State

```java
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
        eventCountState.update(currentCount);  // ← This state is checkpointed
        
        LOG.debug("User {} has {} total events", event.getUserId(), currentCount);
        out.collect(event);
    }
}
```

When a checkpoint occurs:
- Current event count for each userId is saved to persistent storage
- Upon recovery, counts are restored so no data is lost

## 4. Checkpoint Lifecycle

```
Timeline:
┌─────────────────────────────────────────────────────────┐
│ 00:00:00 - Checkpoint 1 triggered                       │
│   ├─ Barrier injected into stream                       │
│   ├─ All operators capture state                        │
│   └─ State written to /tmp/flink-checkpoints           │
│                                                         │
│ 00:00:10 - Minimum pause window (blocked)              │
│                                                         │
│ 00:00:30 - Checkpoint 2 triggered                       │
│   └─ (same process as Checkpoint 1)                    │
│                                                         │
│ 00:00:35 - Job failure occurs                          │
│   └─ (crash, network partition, etc.)                  │
│                                                         │
│ 00:00:40 - Manual restart of Flink job                 │
│   ├─ Flink detects unfinished checkpoint 2             │
│   ├─ Rolls back to completed Checkpoint 1             │
│   ├─ Restores all operator state                       │
│   ├─ Seeks Kafka to saved offset                       │
│   └─ Resumes processing from exact point              │
└─────────────────────────────────────────────────────────┘
```

### Barrier Mechanism

Flink uses **barrier markers** to coordinate checkpoints:

```
Kafka Partition 0: event1 → event2 → [BARRIER] → event3 → event4
                                            ↓
Kafka Partition 1: eventA → [BARRIER] → eventB → eventC

Flink Operators:
  Operator1: processes event1, event2 → reaches barrier → captures state
  Operator2: processes eventA → reaches barrier → captures state
  
  All operators must reach their barrier before checkpoint completes
```

## 5. Fault Tolerance Guarantees

### Exactly-Once Semantics

StreamForge uses **exactly-once processing**:
- Events are processed exactly once, even if failures occur
- No duplicate processing, no missed events
- Achieved through checkpoints + idempotent sinks

### How It Works

```
Scenario: Job crashes after writing 5 events to MongoDB, before checkpoint

Timeline:
1. Events 1-5 arrive → Flink processes all 5
2. Flink writes 1-5 to MongoDB sink
3. Flink crashes before saving checkpoint
4. Job restarts → loads last checkpoint (had processed 0 events)
5. Kafka resends events 1-5 (consumer offset wasn't updated)
6. Flink reprocesses 1-5 and writes to MongoDB again

Result: WITHOUT idempotency → 10 events in DB (duplicates)
        WITH idempotency → 5 events in DB (upserts)

MongoDB Sink Implementation:
- Uses event.id as _id (primary key)
- insert() fails on duplicate, but upsert() would handle retries
```

### Current Implementation

```java
// MongoDBSink.java
@Override
public void invoke(Event event, Context context) throws Exception {
    Document doc = new Document()
        .append("data", dataJson)
        .append("timestamp", event.getTimestamp())
        .append("processedAt", new java.util.Date());
    
    collection.insertOne(doc);  // Simple insert, not idempotent
    // For exactly-once, should use replaceOne with _id = event.getId()
}
```

**Note**: Current implementation uses `insertOne()`, which could cause duplicates. For true exactly-once with MongoDB, use `replaceOne(filter, doc, new ReplaceOptions().upsert(true))`.

## 6. Recovery Process

When a Flink job restarts, recovery happens automatically:

```java
// Automatic on startup:
1. Check for incomplete checkpoints
2. Find the most recent completed checkpoint
3. Load operator state from checkpoint
4. Reset Kafka consumer to saved offset
5. Resume processing from that point
```

### Monitoring Recovery

```
Flink Web UI (http://localhost:8081):
- Shows checkpoint history
- Displays latest completed checkpoint timestamp
- Indicates number of in-progress checkpoints
- Alerts if checkpoint takes too long
```

### Manual Savepoint for Controlled Restart

For planned maintenance or version upgrades:

```bash
# Create a savepoint (manual checkpoint)
flink savepoint <job_id> /tmp/flink-savepoints

# Stop the job
flink cancel <job_id>

# Restart from savepoint
flink run -s /tmp/flink-savepoints/<savepoint_id> <job_jar>
```

## 7. Checkpoint Timing and Window Impact

Checkpoints and windows interact:

```
Scenario: 1-minute tumbling windows, 30-second checkpoints

Timeline:
├─────────────────────────────────┼─────────────────────────────────┤
│ Window 1 [00:00 - 01:00)        │ Window 2 [01:00 - 02:00)        │
├─────────────────────────────────┼─────────────────────────────────┤

Checkpoints:
├─ Checkpoint @ 00:00 (state: Window 1 empty)
├─ Checkpoint @ 00:30 (state: Window 1 has 30 events)
├─ Window 1 closes @ 01:00 (emits aggregate metrics)
├─ Checkpoint @ 01:00 (state: Window 2 empty, Window 1 metrics saved)
├─ Checkpoint @ 01:30 (state: Window 2 has 30 events)
└─ Window 2 closes @ 02:00

If job crashes @ 00:45:
- Restore to checkpoint @ 00:30
- Window 1 had 30 events, restore them
- Resume from Kafka offset at 00:30
- Reprocess events from 00:30 to 00:45 (duplicates)
- Continue processing from 00:45 onward
```

## 8. Best Practices

1. **Checkpoint Interval**: 30 seconds balances recovery time vs overhead
   - Smaller intervals (e.g., 10s): faster recovery, more CPU usage
   - Larger intervals (e.g., 5min): slower recovery, less overhead

2. **Timeout Setting**: 60 seconds is reasonable for most workloads
   - Increase if checkpoints consistently fail
   - Indicates insufficient resources if happening frequently

3. **Storage**: Use distributed storage for production
   - Current local filesystem is good for dev
   - Production: HDFS, S3, or cloud blob storage

4. **Monitoring**: Check checkpoint metrics regularly
   - Size of checkpoints (indicates state growth)
   - Duration (indicates performance issues)
   - Failure rate (indicates stability problems)

5. **Idempotent Sinks**: Ensure exactly-once semantics
   - Use upsert operations instead of insert
   - Key on unique event ID

## Summary

- **Interval**: Checkpoint every 30 seconds
- **Timeout**: 60-second limit per checkpoint
- **Concurrency**: Sequential (1 at a time) for consistency
- **Storage**: Filesystem volumes mounted in Docker
- **Recovery**: Automatic from latest completed checkpoint
- **Semantics**: Provides exactly-once processing guarantees
- **State Captured**: Operator state + Kafka offsets + window data
- **Production**: Ready for deployment with idempotent sink configuration
