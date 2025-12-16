o# StreamForge

A real-time data streaming and processing platform built with Apache Kafka, Apache Flink, and modern cloud technologies.

## Architecture

### Local Development

- **Apache Kafka**: Message streaming platform
- **Apache Flink**: Stream processing engine (Java)
- **MongoDB**: Document database for local development
- **Docker Compose**: Container orchestration

### AWS Production

- **DynamoDB**: Managed NoSQL database
- **AWS Amplify**: React frontend hosting with chatbot
- **Terraform**: Infrastructure as Code

## Project Structure

```txt
streamforge/
├── docker/                 # Docker Compose configuration
│   └── docker-compose.yml
├── flink-jobs/            # Flink stream processing jobs (Java/Maven)
│   ├── pom.xml
│   └── src/
├── frontend/              # React application with AWS Amplify
│   └── streamforge-ui/
├── terraform/             # AWS infrastructure definitions
│   ├── dynamodb.tf
│   ├── amplify.tf
│   └── variables.tf
├── scripts/               # Utilities and migration tools
│   └── mongodb-to-dynamodb/
└── docs/                  # Additional documentation
```

## Prerequisites

- Docker & Docker Compose
- Java 11+ and Maven
- Node.js 18+ and npm
- Terraform
- AWS CLI (configured)

## Quick Start

### 1. Start Local Infrastructure

```bash
cd docker
docker-compose up -d
```

This starts:
- Kafka (localhost:9092)
- Zookeeper (localhost:2181)
- Flink JobManager (localhost:8081)
- Flink TaskManager
- MongoDB (localhost:27017)

### 2. Build and Run Flink Jobs

```bash
cd flink-jobs
mvn clean package
# Submit job to Flink cluster
./scripts/submit-job.sh
```

### 3. Run Frontend Locally

```bash
cd frontend/streamforge-ui
npm install
npm start
```

### 4. Deploy to AWS

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

## Migration: MongoDB to DynamoDB

```bash
cd scripts/mongodb-to-dynamodb
./migrate.sh
```

## Monitoring

- **Flink Dashboard**: http://localhost:8081
- **Kafka**: Use kafka-console-consumer or GUI tools

## Development Workflow

1. Develop Flink jobs in `flink-jobs/`
2. Test with local Kafka + MongoDB
3. Build React UI and test locally
4. Use Terraform to provision AWS resources
5. Migrate data from MongoDB to DynamoDB
6. Deploy frontend to Amplify

## Next Steps

- [ ] Configure Kafka topics
- [ ] Implement Flink streaming jobs
- [ ] Design MongoDB schema
- [ ] Build React chatbot UI
- [ ] Set up Terraform infrastructure
- [ ] Create migration scripts

## License

MIT
