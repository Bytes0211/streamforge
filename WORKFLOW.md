# StreamForge Project Workflow

This document contains Mermaid diagrams illustrating the StreamForge project workflow, architecture, and deployment process.

## Project Development Workflow

```mermaid
    graph TB
        Start([Project Start]) --> Phase1[Phase 1: Local Development]
        
        Phase1 --> Docker[Docker Infrastructure Setup]
        Docker --> Kafka[Kafka & Zookeeper]
        Docker --> Flink[Flink JobManager & TaskManager]
        Docker --> MongoDB[MongoDB Database]
        
        Kafka --> FlinkJob[Flink Job Development]
        Flink --> FlinkJob
        FlinkJob --> StreamProcessor[StreamProcessor.java]
        FlinkJob --> MongoSink[MongoDBSink.java]
        
        StreamProcessor --> KafkaTopics[Configure Kafka Topics]
        MongoSink --> MongoSchema[Design MongoDB Schema]
        
        KafkaTopics --> LocalTest[Local Testing]
        MongoSchema --> LocalTest
        
        LocalTest --> Phase1Complete{Phase 1 Complete?}
        Phase1Complete -->|No| FlinkJob
        Phase1Complete -->|Yes| Phase2[Phase 2: Stream Processing]
        
        Phase2 --> JSON[JSON Deserialization]
        JSON --> Validation[Data Validation]
        Validation --> Stateful[Stateful Processing]
        Stateful --> Windowing[Windowing Operations]
        Windowing --> Aggregations[Aggregations]
        Aggregations --> Checkpointing[Checkpointing Config]
        Checkpointing --> ErrorHandling[Error Handling]
        ErrorHandling --> PerfTest[Performance Testing]
        
        PerfTest --> Phase2Complete{Phase 2 Complete?}
        Phase2Complete -->|No| JSON
        Phase2Complete -->|Yes| Phase3[Phase 3: AWS Deployment]
        
        Phase3 --> Terraform[Create Terraform Modules]
        Terraform --> DynamoDB[Configure DynamoDB]
        DynamoDB --> DeployDB[Deploy DynamoDB]
        
        Phase3 --> Migration[MongoDB to DynamoDB Migration]
        Migration --> MigrationTest[Test Migration]
        
        DeployDB --> Frontend[Build React Frontend]
        Frontend --> Chatbot[Chatbot UI Components]
        Chatbot --> Amplify[Configure AWS Amplify]
        Amplify --> Deploy[Deploy to Amplify]
        
        MigrationTest --> Validation2[Validate Data Integrity]
        Deploy --> FinalTest[End-to-End Testing]
        Validation2 --> FinalTest
        
        FinalTest --> Complete([Project Complete])
        
        style Start fill:#90EE90
        style Complete fill:#90EE90
        style Phase1 fill:#87CEEB
        style Phase2 fill:#87CEEB
        style Phase3 fill:#87CEEB
        style LocalTest fill:#FFD700
        style PerfTest fill:#FFD700
        style FinalTest fill:#FFD700
```

## Data Flow Architecture

```mermaid
graph LR
    subgraph "Data Sources"
        DS1[External API]
        DS2[IoT Devices]
        DS3[User Events]
    end
    
    subgraph "Message Streaming"
        Kafka[Apache Kafka<br/>Topics: streamforge-input]
    end
    
    subgraph "Stream Processing"
        Flink[Apache Flink<br/>StreamProcessor]
        Transform[Transform & Validate]
        Window[Windowing & Aggregation]
        Flink --> Transform
        Transform --> Window
    end
    
    subgraph "Local Storage"
        MongoDB[(MongoDB<br/>processed_data)]
    end
    
    subgraph "Cloud Storage"
        DynamoDB[(Amazon DynamoDB)]
    end
    
    subgraph "Frontend"
        React[React App<br/>with Chatbot]
        Amplify[AWS Amplify]
        React --> Amplify
    end
    
    DS1 --> Kafka
    DS2 --> Kafka
    DS3 --> Kafka
    
    Kafka --> Flink
    Window --> MongoDB
    Window --> DynamoDB
    
    MongoDB -.Migration.-> DynamoDB
    DynamoDB --> React
    
    style Kafka fill:#FF6B6B
    style Flink fill:#4ECDC4
    style MongoDB fill:#95E1D3
    style DynamoDB fill:#F38181
    style React fill:#AA96DA
```

## Deployment Pipeline

```mermaid
graph TB
    subgraph "Development"
        Dev[Local Development]
        Dev --> Build[Maven Build]
        Build --> Test[Unit Tests]
    end
    
    subgraph "Local Environment"
        Test --> LocalDeploy[Deploy to Local Flink]
        LocalDeploy --> IntegrationTest[Integration Testing]
        IntegrationTest --> Validate[Validate Results]
    end
    
    subgraph "Infrastructure as Code"
        Validate --> TerraformPlan[Terraform Plan]
        TerraformPlan --> Review[Manual Review]
        Review --> TerraformApply[Terraform Apply]
    end
    
    subgraph "AWS Environment"
        TerraformApply --> CreateDynamoDB[Create DynamoDB Tables]
        CreateDynamoDB --> RunMigration[Run Migration Script]
        RunMigration --> DeployFrontend[Deploy React to Amplify]
    end
    
    subgraph "Validation"
        DeployFrontend --> SmokeTest[Smoke Tests]
        SmokeTest --> E2ETest[End-to-End Tests]
        E2ETest --> Monitor[Setup Monitoring]
    end
    
    Monitor --> Production([Production Ready])
    
    style Dev fill:#E3F2FD
    style Build fill:#BBDEFB
    style Test fill:#90CAF9
    style Production fill:#4CAF50
```

## Flink Job Execution Flow

```mermaid
sequenceDiagram
    participant K as Kafka
    participant F as Flink JobManager
    participant T as Flink TaskManager
    participant M as MongoDB
    participant D as DynamoDB
    
    Note over K,F: Job Submission
    F->>T: Deploy StreamProcessor Job
    T->>K: Subscribe to streamforge-input
    
    loop Real-time Processing
        K->>T: Consume Message
        T->>T: Deserialize JSON
        T->>T: Transform Data
        T->>T: Apply Windowing
        T->>T: Aggregate Results
        
        alt Local Environment
            T->>M: Write to MongoDB
            M-->>T: Acknowledge
        else Production Environment
            T->>D: Write to DynamoDB
            D-->>T: Acknowledge
        end
    end
    
    Note over T: Checkpoint State
    T->>F: Report Checkpoint Complete
    
    Note over F,T: Fault Recovery
    F->>T: Restore from Checkpoint
```

## MongoDB to DynamoDB Migration Flow

```mermaid
flowchart TD
    Start([Start Migration]) --> Connect[Connect to MongoDB]
    Connect --> Extract[Extract Collections]
    
    Extract --> Collections{For Each Collection}
    Collections --> ReadBatch[Read Batch of Documents]
    
    ReadBatch --> Transform[Transform Schema]
    Transform --> Validate[Validate Data]
    
    Validate --> ValidCheck{Valid?}
    ValidCheck -->|No| ErrorLog[Log Error]
    ValidCheck -->|Yes| WriteDynamo[Write to DynamoDB]
    
    ErrorLog --> MoreDocs{More Documents?}
    WriteDynamo --> MoreDocs
    
    MoreDocs -->|Yes| ReadBatch
    MoreDocs -->|No| NextCollection{More Collections?}
    
    NextCollection -->|Yes| Collections
    NextCollection -->|No| VerifyCount[Verify Row Counts]
    
    VerifyCount --> IntegrityCheck[Data Integrity Check]
    IntegrityCheck --> Complete([Migration Complete])
    
    style Start fill:#90EE90
    style Complete fill:#90EE90
    style ErrorLog fill:#FFB6C1
    style WriteDynamo fill:#87CEEB
```

## State Management & Checkpointing

```mermaid
stateDiagram-v2
    [*] --> Initializing
    
    Initializing --> Running: Job Submitted
    Running --> Checkpointing: Checkpoint Triggered
    Checkpointing --> Running: Checkpoint Complete
    
    Running --> Failing: Exception Occurred
    Failing --> Restarting: Attempt Recovery
    Restarting --> Running: Restore from Checkpoint
    
    Running --> Cancelling: User Cancellation
    Failing --> Cancelling: Max Retries Exceeded
    
    Cancelling --> Cancelled
    Cancelled --> [*]
    
    Running --> Finished: All Data Processed
    Finished --> [*]
    
    note right of Checkpointing
        State saved to filesystem
        RocksDB backend
        1 minute interval
    end note
    
    note right of Restarting
        Restore state from
        last successful checkpoint
    end note
```

## AWS Infrastructure Dependencies

```mermaid
graph TD
    subgraph "AWS Account"
        IAM[IAM Roles & Policies]
        
        subgraph "Data Layer"
            DynamoDB[(DynamoDB Tables)]
            S3[S3 Buckets<br/>For Flink Checkpoints]
        end
        
        subgraph "Compute Layer"
            Lambda[AWS Lambda<br/>Optional]
            Kinesis[Kinesis Data Streams<br/>Alternative to Kafka]
        end
        
        subgraph "Frontend Layer"
            Amplify[AWS Amplify]
            CloudFront[CloudFront CDN]
            React[React App]
        end
        
        subgraph "Monitoring"
            CloudWatch[CloudWatch Logs & Metrics]
            XRay[X-Ray Tracing]
        end
    end
    
    IAM --> DynamoDB
    IAM --> S3
    IAM --> Lambda
    IAM --> Amplify
    
    Kinesis --> Lambda
    Lambda --> DynamoDB
    
    S3 -.Checkpoint Storage.-> Lambda
    
    React --> Amplify
    Amplify --> CloudFront
    CloudFront --> React
    
    DynamoDB --> React
    
    CloudWatch -.Monitor.-> DynamoDB
    CloudWatch -.Monitor.-> Lambda
    CloudWatch -.Monitor.-> Amplify
    
    XRay -.Trace.-> Lambda
    
    style IAM fill:#FF9800
    style DynamoDB fill:#4CAF50
    style Amplify fill:#2196F3
    style CloudWatch fill:#9C27B0
```

## Technology Stack Layers

```mermaid
graph TB
    subgraph "Presentation Layer"
        UI[React UI Components]
        Chatbot[AI Chatbot Interface]
    end
    
    subgraph "API Layer"
        REST[REST API Endpoints]
        WebSocket[WebSocket for Real-time]
    end
    
    subgraph "Processing Layer"
        Streaming[Apache Flink<br/>Stream Processing]
        Batch[Batch Processing<br/>Optional]
    end
    
    subgraph "Message Layer"
        MQ[Apache Kafka<br/>Message Queue]
    end
    
    subgraph "Storage Layer"
        DevDB[(MongoDB<br/>Development)]
        ProdDB[(DynamoDB<br/>Production)]
    end
    
    subgraph "Infrastructure Layer"
        Docker[Docker Compose<br/>Local]
        Terraform[Terraform<br/>AWS IaC]
    end
    
    UI --> REST
    Chatbot --> WebSocket
    
    REST --> ProdDB
    WebSocket --> Streaming
    
    Streaming --> MQ
    MQ --> Streaming
    
    Streaming --> DevDB
    Streaming --> ProdDB
    
    Docker --> MQ
    Docker --> DevDB
    Docker --> Streaming
    
    Terraform --> ProdDB
    Terraform --> REST
    
    style UI fill:#E1BEE7
    style Streaming fill:#B2DFDB
    style MQ fill:#FFCCBC
    style DevDB fill:#C5E1A5
    style ProdDB fill:#F48FB1
```

---

## How to View These Diagrams

These Mermaid diagrams can be viewed in:

1. **GitHub/GitLab**: Automatically rendered in markdown files
2. **VS Code**: Install the "Markdown Preview Mermaid Support" extension
3. **Online**: [Mermaid Live Editor](https://mermaid.live)
4. **Documentation Sites**: GitBook, Docusaurus, MkDocs with Mermaid plugin

## Diagram Descriptions

- **Project Development Workflow**: Shows the complete development process from Phase 1 to Phase 3
- **Data Flow Architecture**: Illustrates how data moves through the system
- **Deployment Pipeline**: Details the CI/CD process from development to production
- **Flink Job Execution Flow**: Sequence diagram showing message processing
- **MongoDB to DynamoDB Migration**: Step-by-step migration process
- **State Management & Checkpointing**: Flink job state transitions
- **AWS Infrastructure Dependencies**: Cloud resources and their relationships
- **Technology Stack Layers**: System architecture by layer
