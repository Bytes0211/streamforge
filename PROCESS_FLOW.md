# StreamForge Process Flow

This document contains Mermaid diagrams visualizing the StreamForge real-time streaming platform architecture.

## High-Level Architecture

```mermaid
    graph TB
    
        subgraph External["External Data Sources"]
            DS[Data Producers]
        end
        
        subgraph Docker["Docker Compose Environment"]
            subgraph Messaging["Message Broker"]
                ZK[Zookeeper<br/>:2181]
                KAFKA[Kafka Broker<br/>:9092<br/>Topic: streamforge-input<br/>3 partitions]
            end
            
            subgraph Processing["Stream Processing"]
                JM[Flink JobManager<br/>:8081]
                TM[Flink TaskManager<br/>4 slots]
            end
            
            subgraph Storage["Data Storage"]
                MONGO[(MongoDB<br/>:27017<br/>Database: streamforge)]
            end
        end
        
        subgraph Frontend["User Interface"]
            REACT[React Dashboard<br/>AWS Amplify]
        end
        
        DS -->|JSON Events| KAFKA
        ZK -.->|Coordination| KAFKA
        KAFKA -->|Consume| JM
        JM -->|Distribute| TM
        TM -->|Write| MONGO
        MONGO -->|Query| REACT
        
        style External fill:#e1f5ff
        style Docker fill:#fff4e1
        style Frontend fill:#e8f5e9
        style KAFKA fill:#ff9800
        style JM fill:#2196f3
        style TM fill:#2196f3
        style MONGO fill:#4caf50
        style REACT fill:#00bcd4
```

## Detailed Data Flow Pipeline

```mermaid

flowchart LR
    subgraph Input["Data Ingestion"]
        SOURCE[External Sources]
        PRODUCER[Kafka Producer]
    end
    
    subgraph Kafka["Kafka (Port 9092)"]
        TOPIC[streamforge-input<br/>3 partitions]
    end
    
    subgraph Flink["Flink Stream Processing"]
        CONSUMER[Kafka Consumer<br/>Group: streamforge-consumer-group]
        DESERIALIZE[JSON Deserializer<br/>Event POJO]
        VALIDATE{Data Validation<br/>isValid?}
        ENRICH[Stateful Processing<br/>ValueState: Event Count]
        WINDOW[Time Window<br/>1-minute tumbling]
        AGGREGATE[Aggregations<br/>count, sum, avg, min, max]
    end
    
    subgraph Sinks["MongoDB Sinks"]
        SINK1[(processed_data<br/>collection)]
        SINK2[(aggregated_metrics<br/>collection)]
        DLQ[(dead_letter_queue<br/>collection)]
    end
    
    SOURCE -->|Produce| PRODUCER
    PRODUCER -->|JSON| TOPIC
    TOPIC -->|Consume| CONSUMER
    CONSUMER -->|Raw String| DESERIALIZE
    DESERIALIZE -->|Event Object| VALIDATE
    
    VALIDATE -->|Valid| ENRICH
    VALIDATE -->|Invalid| DLQ
    
    ENRICH -->|Enriched Event| SINK1
    ENRICH -->|Stream| WINDOW
    WINDOW -->|Windowed Data| AGGREGATE
    AGGREGATE -->|Metrics| SINK2
    
    style Input fill:#e3f2fd
    style Kafka fill:#fff3e0
    style Flink fill:#e8f5e9
    style Sinks fill:#f3e5f5
    style VALIDATE fill:#ffeb3b
    style DLQ fill:#f44336,color:#fff
```

## Event Processing Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Produced: JSON Event Created
    
    Produced --> KafkaQueue: Send to Kafka
    KafkaQueue --> Consumed: Flink Consumes
    
    Consumed --> Deserialization: Parse JSON
    
    Deserialization --> Validation: Check Fields
    Validation --> Valid: All fields present
    Validation --> Invalid: Missing/null fields
    
    Invalid --> DeadLetterQueue: Write to DLQ
    DeadLetterQueue --> [*]
    
    Valid --> StateEnrichment: Add event count
    StateEnrichment --> ProcessedSink: Write raw event
    StateEnrichment --> Windowing: Continue processing
    
    Windowing --> WaitingForWindow: Buffer events
    WaitingForWindow --> WindowTriggered: 1 minute elapsed
    
    WindowTriggered --> Aggregation: Calculate metrics
    Aggregation --> MetricsSink: Write aggregations
    
    ProcessedSink --> [*]
    MetricsSink --> [*]
    
    note right of Validation
        Checks: id, type, userId,
        value, timestamp, payload
        are not null/empty
    end note
    
    note right of StateEnrichment
        Per-user event counter
        using ValueState
    end note
    
    note right of Aggregation
        Per user/type:
        count, sum, avg, min, max
    end note
```

## Flink Job Architecture

```mermaid
graph TB
    subgraph Main["StreamProcessor.java (Main Entry Point)"]
        ENV[Execution Environment<br/>Checkpoint: 30s interval]
        SOURCE[Kafka Source<br/>Bootstrap: kafka:29092]
        SCHEMA[Value-only Schema<br/>SimpleStringSchema]
    end
    
    subgraph Transform["Transformation Pipeline"]
        MAP1[Map: JSON to Event<br/>ObjectMapper]
        FILTER[Filter: isValid]
        KEY[KeyBy: userId]
        PROCESS[KeyedProcessFunction<br/>ValueState tracking]
        WINDOW[TumblingEventTimeWindows<br/>1 minute]
        AGG[AggregateFunction<br/>Metrics calculation]
    end
    
    subgraph Sinks["Output Sinks"]
        SINK1[MongoDBSink<br/>RichSinkFunction]
        SINK2[MongoDBMetricsSink<br/>RichSinkFunction]
        SINK3[DeadLetterQueueSink<br/>RichSinkFunction]
    end
    
    subgraph State["State Management"]
        CHECKPOINT[Filesystem Checkpoint<br/>/tmp/flink-checkpoints]
        SAVEPOINT[Savepoints<br/>/tmp/flink-savepoints]
    end
    
    ENV --> SOURCE
    SOURCE --> SCHEMA
    SCHEMA --> MAP1
    
    MAP1 --> FILTER
    MAP1 -.->|Invalid JSON| SINK3
    
    FILTER -->|Valid Events| KEY
    FILTER -.->|Invalid Events| SINK3
    
    KEY --> PROCESS
    PROCESS --> SINK1
    PROCESS --> WINDOW
    
    WINDOW --> AGG
    AGG --> SINK2
    
    ENV -.->|Manages| CHECKPOINT
    ENV -.->|Manages| SAVEPOINT
    
    PROCESS -.->|Uses| CHECKPOINT
    
    style Main fill:#e3f2fd
    style Transform fill:#e8f5e9
    style Sinks fill:#fff3e0
    style State fill:#f3e5f5
```

## MongoDB Collections Schema

```mermaid
erDiagram
    PROCESSED_DATA {
        string id PK
        string type
        string userId
        double value
        long timestamp
        string payload
        string processingTime
        int eventCount
    }
    
    AGGREGATED_METRICS {
        string windowStart
        string windowEnd
        string userId
        string eventType
        long count
        double sum
        double average
        double min
        double max
    }
    
    DEAD_LETTER_QUEUE {
        string id PK
        string rawMessage
        string errorMessage
        string timestamp
        string source
    }
    
    PROCESSED_DATA ||--o{ AGGREGATED_METRICS : "aggregates to"
    PROCESSED_DATA ||--o{ DEAD_LETTER_QUEUE : "errors go to"
```

## Docker Container Network

```mermaid
graph TB
    subgraph Network["streamforge-network (bridge)"]
        subgraph Coordination
            ZK[streamforge-zookeeper<br/>2181:2181]
        end
        
        subgraph Messaging
            KAFKA[streamforge-kafka<br/>9092:9092, 9101:9101<br/>Internal: kafka:29092]
        end
        
        subgraph Compute
            JM[streamforge-flink-jobmanager<br/>8081:8081]
            TM[streamforge-flink-taskmanager]
        end
        
        subgraph Database
            MONGO[streamforge-mongodb<br/>27017:27017<br/>Credentials: admin/password]
        end
        
        subgraph Volumes
            V1[(flink-checkpoints)]
            V2[(flink-savepoints)]
            V3[(mongodb-data)]
        end
    end
    
    subgraph Host["Host Machine (Windows)"]
        BROWSER[Web Browser<br/>localhost:8081]
        MVNBUILD[Maven Build<br/>flink-jobs/]
        PRODUCER[Test Producers<br/>localhost:9092]
    end
    
    ZK -.->|Coordinates| KAFKA
    KAFKA <-->|RPC| JM
    JM <-->|Task Distribution| TM
    TM <-->|Read/Write| MONGO
    
    JM -.->|Mounts| V1
    JM -.->|Mounts| V2
    TM -.->|Mounts| V1
    MONGO -.->|Mounts| V3
    
    BROWSER -->|HTTP| JM
    MVNBUILD -.->|Deploys JAR| JM
    PRODUCER -->|TCP| KAFKA
    
    style Network fill:#e1f5ff
    style Host fill:#fff4e1
    style Volumes fill:#f3e5f5
```

## Development Workflow

```mermaid
flowchart TD
    START([Developer])
    
    subgraph Local["Local Development"]
        CODE[Write/Modify Code<br/>Java, React]
        BUILD{Build Type?}
        MAVEN[mvn clean package<br/>Build Flink JAR]
        NPM[npm run build<br/>Build React App]
    end
    
    subgraph Docker["Docker Environment"]
        INFRA[docker compose up -d<br/>Start Services]
        DEPLOY[Copy JAR to Container<br/>Submit Flink Job]
        VERIFY[Verify Services<br/>docker ps]
    end
    
    subgraph Test["Testing"]
        UNIT[Unit Tests<br/>mvn test / npm test]
        INTEGRATION[Integration Tests<br/>Send test events]
        MONITOR[Monitor Results<br/>Flink UI, MongoDB]
    end
    
    subgraph Debug["Debug & Iterate"]
        LOGS[Check Logs<br/>docker logs]
        FIX[Fix Issues]
        RESTART[Restart Services<br/>if needed]
    end
    
    START --> CODE
    CODE --> BUILD
    
    BUILD -->|Backend| MAVEN
    BUILD -->|Frontend| NPM
    
    MAVEN --> INFRA
    NPM --> INFRA
    
    INFRA --> VERIFY
    VERIFY --> DEPLOY
    
    DEPLOY --> UNIT
    UNIT --> INTEGRATION
    INTEGRATION --> MONITOR
    
    MONITOR -->|Issues Found| LOGS
    LOGS --> FIX
    FIX --> RESTART
    RESTART --> CODE
    
    MONITOR -->|Success| END([Deployment Complete])
    
    style Local fill:#e3f2fd
    style Docker fill:#fff3e0
    style Test fill:#e8f5e9
    style Debug fill:#ffebee
```

## Checkpoint and Fault Tolerance

```mermaid
sequenceDiagram
    participant App as Flink Application
    participant JM as JobManager
    participant TM as TaskManager
    participant FS as Filesystem<br/>/tmp/flink-checkpoints
    participant Kafka as Kafka
    participant Mongo as MongoDB
    
    Note over App,Mongo: Normal Processing
    App->>TM: Process Events
    TM->>Kafka: Consume Messages
    TM->>Mongo: Write to Sinks
    
    Note over App,Mongo: Checkpoint Triggered (every 30s)
    JM->>TM: Initiate Checkpoint
    TM->>TM: Snapshot ValueState
    TM->>FS: Write State Snapshot
    TM->>Kafka: Commit Offsets
    TM->>JM: Checkpoint Complete
    
    Note over App,Mongo: Failure Scenario
    TM-xTM: Task Failure
    JM->>JM: Detect Failure
    JM->>FS: Read Last Checkpoint
    JM->>TM: Restore State
    TM->>Kafka: Reset to Checkpoint Offset
    
    Note over App,Mongo: Resume Processing
    TM->>Kafka: Continue from Checkpoint
    TM->>Mongo: Resume Writing
```

## Event Data Model

```mermaid
classDiagram
    class Event {
        -String id
        -String type
        -String userId
        -Double value
        -Long timestamp
        -String payload
        +Event()
        +isValid() boolean
        +getId() String
        +getType() String
        +getUserId() String
        +getValue() Double
        +getTimestamp() Long
        +getPayload() String
    }
    
    class AggregatedMetrics {
        -String windowStart
        -String windowEnd
        -String userId
        -String eventType
        -Long count
        -Double sum
        -Double average
        -Double min
        -Double max
        +AggregatedMetrics()
        +getWindowStart() String
        +getWindowEnd() String
        +getUserId() String
        +getEventType() String
        +getCount() Long
        +getSum() Double
        +getAverage() Double
        +getMin() Double
        +getMax() Double
    }
    
    class MongoDBSink {
        -MongoClient mongoClient
        -MongoCollection collection
        +open(Configuration)
        +invoke(Event, Context)
        +close()
    }
    
    class MongoDBMetricsSink {
        -MongoClient mongoClient
        -MongoCollection collection
        +open(Configuration)
        +invoke(AggregatedMetrics, Context)
        +close()
    }
    
    class DeadLetterQueueSink {
        -MongoClient mongoClient
        -MongoCollection collection
        +open(Configuration)
        +invoke(String, Context)
        +close()
    }
    
    Event --> MongoDBSink : processed by
    AggregatedMetrics --> MongoDBMetricsSink : processed by
    Event --> DeadLetterQueueSink : errors handled by
```

## Deployment Timeline (Gantt Chart)

```mermaid
gantt
    title StreamForge Project Timeline
    dateFormat YYYY-MM-DD
    
    section Phase 1: Local Dev
    Docker Infrastructure       :done, p1a, 2025-12-10, 1d
    Flink Job Development      :done, p1b, 2025-12-11, 2d
    MongoDB Schema             :done, p1c, 2025-12-13, 2d
    Local Testing              :done, p1d, 2025-12-15, 2d
    
    section Phase 2: Stream Processing
    JSON Processing            :done, p2a, 2025-12-17, 1d
    Stateful Operations        :done, p2b, 2025-12-17, 1d
    Windowing & Aggregations   :done, p2c, 2025-12-18, 1d
    Checkpointing              :done, p2d, 2025-12-18, 1d
    
    section Phase 3: AWS Docs
    Terraform Infrastructure   :done, p3a, 2025-12-19, 2d
    DynamoDB Schema            :done, p3b, 2025-12-19, 1d
    Migration Strategy         :done, p3c, 2025-12-20, 2d
    
    section Phase 4: Testing
    Test Suite Development     :done, p4a, 2025-12-19, 1d
    Integration Testing        :done, p4b, 2025-12-19, 1d
    Performance Testing        :done, p4c, 2025-12-19, 1d
    
    section Phase 5: Frontend
    React Components           :done, p5a, 2025-12-31, 1d
    AWS Amplify Integration    :done, p5b, 2025-12-31, 1d
    Frontend Testing           :done, p5c, 2025-12-31, 1d
```

---

## How to Use These Diagrams

### Viewing in VS Code
Install the "Markdown Preview Mermaid Support" extension to render these diagrams.

### Viewing in GitHub
GitHub natively supports Mermaid diagrams in markdown files.

### Exporting
Use tools like [Mermaid Live Editor](https://mermaid.live/) to export diagrams as PNG/SVG.

### Updating
Edit the Mermaid code blocks directly in this file to reflect architecture changes.
