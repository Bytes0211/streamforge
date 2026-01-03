# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.


## Build and Test Commands

### Local Infrastructure
```bash
# Start all services (Kafka, Flink, MongoDB)
cd docker && docker compose up -d

# Stop all services
cd docker && docker compose down

# View logs
cd docker && docker compose logs -f [service-name]

# Clean restart (removes all data volumes)
cd docker && docker compose down -v && docker compose up -d
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

# Return to project root
cd ..
```

### Job Submission
```bash
# Copy JAR to Flink container
docker cp flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar streamforge-flink-jobmanager:/opt/flink/

# Submit job
docker exec streamforge-flink-jobmanager flink run -d /opt/flink/flink-jobs-1.0-SNAPSHOT.jar

# List running jobs
docker exec streamforge-flink-jobmanager flink list -r

# Cancel a job (get JOB_ID from list command)
docker exec streamforge-flink-jobmanager flink cancel <JOB_ID>
```

Alternatively, submit via Flink dashboard at http://localhost:8081

### Monitoring & Testing
- **Flink Dashboard**: http://localhost:8081
- **Kafka**: localhost:9092
- **MongoDB**: localhost:27017 (credentials: admin/password)

```bash
# Check Docker services status
docker ps

# Test MongoDB connection
docker exec streamforge-mongodb mongosh --quiet --eval "db.runCommand({ ping: 1 })"

# View MongoDB data
docker exec streamforge-mongodb mongosh -u admin -p password --authenticationDatabase admin --eval "db.getSiblingDB('streamforge').processed_data.find().limit(5)"

# Send test event to Kafka
echo '{"id":"test-1","type":"click","userId":"user-1","value":10.5,"timestamp":'$(date +%s%3N)',"payload":"test"}' | \
  docker exec -i streamforge-kafka kafka-console-producer \
    --broker-list localhost:9092 \
    --topic streamforge-input

# View Kafka messages
docker exec streamforge-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic streamforge-input \
  --from-beginning \
  --max-messages 5

# Check Flink logs
docker logs streamforge-flink-jobmanager --tail 50
```

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

### Frontend (React + AWS Amplify)
```bash
# Install dependencies
cd frontend && npm install

# Run in development mode (uses mock data by default)
cd frontend && npm start

# Run tests
cd frontend && npm test

# Build for production
cd frontend && npm run build

# Return to project root
cd ..
```

**Note**: Frontend requires Node.js 16+ and npm. Set `REACT_APP_USE_MOCK_DATA=false` in `.env.local` to connect to real DynamoDB.

## Development Context

### Project Status
Local infrastructure and Flink jobs are fully functional with:
- Kafka topics with 3 partitions
- MongoDB schema with 3 collections
- Advanced stream processing (windowing, aggregations, stateful operations)
- Comprehensive testing suite (50 frontend tests + backend integration tests)
- Production-ready React frontend with AWS Amplify integration

### Known Patterns
- Connection strings use Docker service names (e.g., `mongodb://admin:password@mongodb:27017`)
- Flink state backend uses filesystem checkpointing
- Stream processing includes JSON deserialization, stateful operations, and windowed aggregations

### Common Issues
- If `docker compose` fails, ensure Docker Desktop is running
- If Maven builds fail, verify `JAVA_HOME` is set to Java 11
- If frontend won't start, run `npm install` in `frontend/` directory
- Container name is `streamforge-mongodb` (not `streamforge-mongodb-1`)
