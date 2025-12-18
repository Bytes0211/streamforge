# MongoDB Schema Design

## Database: streamforge

### Collection: processed_data

#### Purpose
Stores processed events from the Flink streaming pipeline. Each document represents a single event that has been transformed by the stream processor.

#### Schema Structure

```json
{
  "_id": ObjectId,              // Auto-generated MongoDB ID
  "data": String,               // Processed data payload (currently uppercased)
  "timestamp": Long,            // Processing timestamp (epoch milliseconds)
  "processedAt": ISODate,       // Processing datetime (ISO 8601 format)
  "sourceOffset": Long,         // Kafka offset (optional, for future tracking)
  "partition": Int              // Kafka partition (optional, for future tracking)
}
```

#### Indexes

1. **Primary Index**: `_id` (automatic)
2. **Timestamp Index**: `timestamp` (descending) - for time-based queries
3. **ProcessedAt Index**: `processedAt` (descending) - for date-range queries

#### Validation Rules

- `data`: Required, must be string
- `timestamp`: Required, must be number
- `processedAt`: Required, must be date

#### Sample Document

```json
{
  "_id": ObjectId("507f1f77bcf86cd799439011"),
  "data": "HELLO WORLD",
  "timestamp": 1702828800000,
  "processedAt": ISODate("2025-12-17T08:12:00Z")
}
```

## Future Collections

### Collection: events_raw (Phase 2)
Store unprocessed events for replay/debugging

### Collection: aggregations (Phase 2)
Store windowed aggregation results

### Collection: dead_letter_queue (Phase 2)
Store failed processing attempts with error details

## Capacity Planning

- **Initial Size**: 100MB
- **Expected Growth**: 1GB/month (1000 events/sec * 30 days)
- **Retention Policy**: 90 days (to be implemented in Phase 2)

## Migration Notes

- DynamoDB migration (Phase 3) will map:
  - `_id` → `id` (String, partition key)
  - `timestamp` → `timestamp` (Number, sort key)
  - `data` → `data` (String)
  - `processedAt` → `processedAt` (String, ISO 8601)
