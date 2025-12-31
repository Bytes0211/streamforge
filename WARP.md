# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Build and Test Commands

### Local Infrastructure
```bash
# Start all services (Kafka, Flink, MongoDB)
cd docker && docker-compose up -d

# Stop all services
cd docker && docker-compose down

# View logs
cd docker && docker-compose logs -f [service-name]
```

### Flink Jobs (Maven)
```bash
# Build Flink job JAR
cd flink-jobs && mvn clean package

# Run tests
cd flink-jobs && mvn test

# Compile only
cd flink-jobs && mvn compile

# Clean build artifacts
cd flink-jobs && mvn clean
```

### Job Submission
The built JAR is located at `flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar`. Submit jobs via Flink dashboard at http://localhost:8081 or using Flink CLI from within the flink-jobmanager container.

### Monitoring
- **Flink Dashboard**: http://localhost:8081
- **Kafka**: localhost:9092
- **MongoDB**: localhost:27017 (credentials: admin/password)

## Code Architecture

### Component Overview
StreamForge is a real-time data streaming platform:
- **Architecture**: Kafka → Flink → MongoDB (all containerized)

### Flink Job Structure
- **StreamProcessor.java**: Main entry point. Sets up Kafka source, applies transformations, and routes to MongoDB sink
- **MongoDBSink.java**: Custom RichSinkFunction for writing processed data to MongoDB with connection lifecycle management

Key design patterns:
- Kafka source reads from topic `streamforge-input` using consumer group `streamforge-consumer-group`
- Stream processing uses simple map transformations (currently placeholder logic)
- MongoDB sink uses connection pooling via MongoClient lifecycle (open/invoke/close)
- State backend configured as filesystem with checkpoints in `/tmp/flink-checkpoints`

### Data Flow
```
External Sources → Kafka (streamforge-input) → Flink StreamProcessor
  → Transformation (map/filter/window) → MongoDBSink → MongoDB (processed_data collection)
```

### Technology Stack
- **Flink**: 1.18.0 (Java 11)
- **Kafka**: Confluent Platform 7.5.0 (Kafka 3.5.1)
- **MongoDB**: 7.0
- **Build**: Maven with Shade plugin for fat JAR packaging

### Maven Dependencies
All Flink dependencies use `provided` scope except connectors. Key dependencies:
- `flink-streaming-java`: Core streaming API
- `flink-connector-kafka`: Kafka integration
- `mongodb-driver-sync`: MongoDB client
- `flink-json` + `jackson-databind`: JSON processing

### Docker Network
All services communicate via `streamforge-network` bridge network. Internal Kafka address is `kafka:29092`, external is `localhost:9092`.

## Development Context

### Project Status
Local infrastructure and Flink jobs are fully functional with:
- Kafka topics with 3 partitions
- MongoDB schema with 3 collections
- Advanced stream processing (windowing, aggregations, stateful operations)
- Comprehensive testing suite

### Known Patterns
- Connection strings use Docker service names (e.g., `mongodb://admin:password@mongodb:27017`)
- Flink state backend uses filesystem checkpointing
- Stream processing includes JSON deserialization, stateful operations, and windowed aggregations
