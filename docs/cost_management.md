# Cost Management & Safe Destruction Guide

**Project:** StreamForge Real-Time Streaming Platform  
**Created:** December 31, 2025  
**Purpose:** Manage local compute costs and safely destroy/recreate Docker infrastructure

---

## Cost Analysis

### Current Local Resources (Docker Compose Infrastructure)

| Component | Resource | Local Cost | Can Destroy? |
|-----------|----------|------------|--------------|
| **Docker** | Zookeeper container | CPU/Memory | ✅ YES (data in volumes) |
| **Docker** | Kafka container | CPU/Memory | ✅ YES (data in volumes) |
| **Docker** | Flink JobManager | CPU/Memory | ✅ YES (checkpoints in volumes) |
| **Docker** | Flink TaskManager | CPU/Memory | ✅ YES (ephemeral) |
| **Docker** | MongoDB container | CPU/Memory | ✅ YES (data in volumes) |
| **Volumes** | mongodb-data | Disk (~100 MB) | ⚠️ KEEP (contains data) |
| **Volumes** | flink-checkpoints | Disk (~50 MB) | ⚠️ KEEP (state recovery) |
| **Volumes** | flink-savepoints | Disk (~10 MB) | ⚠️ KEEP (manual snapshots) |
| **Build** | Maven .m2 cache | Disk (~200 MB) | ⚠️ KEEP (speeds up builds) |
| **Build** | JAR artifact | Disk (~60 MB) | ✅ YES (can rebuild) |
| | **TOTAL** | **~420 MB disk, ~3 GB RAM when running** | |

**Key Insight:** StreamForge is a local development environment with zero cloud costs. The main "cost" is:
- Local compute resources (CPU/RAM) when containers are running
- Disk space for Docker volumes and Maven dependencies (~420 MB)
- Build time (5-10 minutes for full rebuild)

---

## Safe Destruction Strategy

### Option 1: Stop Containers Only (Recommended for Daily Use)

**What to Stop:**
- All 5 Docker containers (Zookeeper, Kafka, Flink x2, MongoDB)

**What to Keep:**
- Docker volumes (mongodb-data, flink-checkpoints, flink-savepoints)
- Docker images (no need to re-download)
- Built JAR artifact (flink-jobs/target/)
- Maven dependencies (~/.m2/repository)

**Resource Savings:** ~3 GB RAM, minimal CPU  
**Recreation Time:** 30 seconds with `docker compose up -d`  
**Data Loss:** None (all data persists in volumes)

**Commands:**
```bash
cd docker

# Stop all containers (preserves volumes)
docker compose stop

# Restart when needed
docker compose start

# Or stop and remove containers (still preserves volumes)
docker compose down
docker compose up -d
```

---

### Option 2: Destroy Containers + Clear Volumes (Clean Slate)

**What to Destroy:**
- All 5 Docker containers
- All Docker volumes (mongodb-data, checkpoints, savepoints)

**What to Keep:**
- Docker images (Flink, Kafka, MongoDB)
- Built JAR artifact
- Maven dependencies

**Resource Savings:** ~3 GB RAM + ~150 MB disk (volumes only)  
**Recreation Time:** 2-3 minutes (startup + MongoDB init)  
**Data Loss:** All MongoDB data, Flink state (checkpoints/savepoints)

**Commands:**
```bash
cd docker

# Stop and remove containers + volumes
docker compose down -v

# Recreate from scratch
docker compose up -d

# Re-initialize MongoDB schema
docker exec -i streamforge-mongodb mongosh \
  -u admin -p password \
  --authenticationDatabase admin \
  < ../scripts/init-mongodb.js

# Redeploy Flink job
docker cp ../flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar streamforge-flink-jobmanager:/opt/flink/
docker exec streamforge-flink-jobmanager \
  flink run -d /opt/flink/flink-jobs-1.0-SNAPSHOT.jar
```

---

### Option 3: Nuclear Option - Full Cleanup (Maximum Disk Recovery)

**⚠️ WARNING:** This removes all project artifacts and Docker resources.

**What Gets Destroyed:**
- All containers and volumes
- Docker images (will need to re-download)
- Built JAR artifacts
- Maven target directory

**What to Keep:**
- Source code (Java files, docker-compose.yml, scripts)
- Maven dependencies in ~/.m2 (optional: can clear to save ~200 MB)

**Resource Savings:** ~3 GB RAM + ~500 MB disk (images + volumes + artifacts)  
**Recreation Time:** 10-15 minutes (image download + build + init)  
**Risk:** HIGH - Must rebuild and reinitialize everything

**Commands:**
```bash
cd docker

# Stop and remove everything
docker compose down -v --rmi all

# Clean build artifacts
cd ../flink-jobs
mvn clean

# Full recreation (when needed)
cd ../docker
docker compose up -d  # Downloads images (~5 min)
cd ../flink-jobs
mvn clean package     # Builds JAR (~3 min)
# ... then redeploy job and init MongoDB
```

---

## Recommended Approach for Resource Management

### Current State Analysis

Your local environment is **already resource-optimized**:

- ✅ No cloud costs (100% local Docker)
- ✅ Containers only consume resources when running
- ✅ Minimal disk usage (~420 MB total)
- ✅ All services stop with `docker compose stop`

**Recommendation:** **Use Option 1 (stop containers) when not actively developing**

### Daily Workflow

**When You're Done Working:**
```bash
cd /home/scotton/dev/projects/streamforge/docker
docker compose stop
```

**When You Resume:**
```bash
docker compose start
# Everything resumes from where you left off (data persists)
```

**Weekly/Monthly Cleanup (Optional):**
```bash
# If you want to start fresh and clear test data
docker compose down -v
docker compose up -d
# Re-initialize MongoDB and redeploy Flink job
```

---

## Safe Destruction Commands

### 1. Check Current Resource Usage

```bash
# See running containers
docker compose ps

# Check disk usage
docker system df

# See volume sizes
docker volume ls
docker volume inspect streamforge_mongodb-data --format '{{.Mountpoint}}' | xargs du -sh

# Check RAM usage
docker stats --no-stream
```

### 2. Selective Container Destruction

```bash
cd docker

# Stop only MongoDB (keep Kafka/Flink running)
docker compose stop mongodb

# Stop only Flink (keep Kafka/MongoDB running)
docker compose stop flink-jobmanager flink-taskmanager

# Stop only Kafka stack (keep Flink/MongoDB running)
docker compose stop kafka zookeeper
```

### 3. Preview Before Destroy

**Always check what will be removed:**

```bash
# List what will be removed (containers only)
docker compose down --dry-run

# Check volumes before deleting
docker volume ls | grep streamforge

# Check if Flink has active jobs before stopping
curl -s http://localhost:8081/jobs | jq '.jobs[] | select(.status=="RUNNING")'
```

### 4. Backup Before Destruction

```bash
# Backup MongoDB data before destroying volumes
docker exec streamforge-mongodb mongodump \
  -u admin -p password \
  --authenticationDatabase admin \
  -d streamforge \
  -o /data/backup

docker cp streamforge-mongodb:/data/backup ./mongodb-backup-$(date +%Y%m%d)

# Restore later
docker cp ./mongodb-backup-20251231 streamforge-mongodb:/data/restore
docker exec streamforge-mongodb mongorestore \
  -u admin -p password \
  --authenticationDatabase admin \
  /data/restore/streamforge
```

---

## Recreation Strategy

### Quick Recreation (After docker compose stop)

```bash
cd docker

# Start all services
docker compose start

# Verify services are running
docker compose ps
curl http://localhost:8081  # Flink dashboard
```

**Time:** 30 seconds  
**Data Loss:** None  
**Job State:** Resumes from last checkpoint

---

### Full Recreation (After docker compose down -v)

```bash
cd docker

# Start all services (recreate containers + volumes)
docker compose up -d

# Wait for services to be ready (30 seconds)
sleep 30

# Re-initialize MongoDB schema
docker exec -i streamforge-mongodb mongosh \
  -u admin -p password \
  --authenticationDatabase admin \
  < ../scripts/init-mongodb.js

# Redeploy Flink job
docker cp ../flink-jobs/target/flink-jobs-1.0-SNAPSHOT.jar streamforge-flink-jobmanager:/opt/flink/
docker exec streamforge-flink-jobmanager \
  flink run -d /opt/flink/flink-jobs-1.0-SNAPSHOT.jar

# Verify pipeline
../scripts/test-events.sh
```

**Time:** 3-5 minutes  
**Data Loss:** All MongoDB data and Flink state  
**Job State:** Fresh start (no checkpoints)

---

## What NOT to Destroy

### Critical Resources (Be Careful)

1. **Source Code**
   ```bash
   # DON'T DO THIS
   rm -rf /home/scotton/dev/projects/streamforge
   ```
   **Why:** This is your project source code. Destroying this loses all work.

2. **Maven Dependencies (Optional)**
   ```bash
   # DON'T DO THIS unless you want to re-download everything
   rm -rf ~/.m2/repository
   ```
   **Why:** Maven will re-download all dependencies (~500 MB, 5-10 min). Only clear if you need disk space.

3. **Docker Images (When Active)**
   ```bash
   # DON'T DO THIS while containers are running
   docker rmi flink:1.18-java11 confluentinc/cp-kafka:7.5.0 mongo:7.0
   ```
   **Why:** Running containers need their images. Stop containers first.

4. **MongoDB Data Volume (If Contains Important Data)**
   ```bash
   # DON'T DO THIS if you have test data you want to keep
   docker volume rm streamforge_mongodb-data
   ```
   **Why:** This permanently deletes all MongoDB data. Backup first if needed.

---

## Resource-Saving Best Practices

### 1. Stop Containers When Not in Use

Containers only consume CPU/RAM when running:
```bash
# Stop everything (takes 5 seconds)
docker compose stop

# Or use Docker Desktop GUI to stop containers
```

**Savings:** ~3 GB RAM, ~10% CPU (idle baseline)

### 2. Use Selective Service Startup

Only start what you need:
```bash
# Just MongoDB (for schema work)
docker compose up -d mongodb

# Just Kafka + Zookeeper (for messaging tests)
docker compose up -d zookeeper kafka

# Full stack minus TaskManager (save 1 GB RAM)
docker compose up -d zookeeper kafka flink-jobmanager mongodb
```

### 3. Periodic Volume Cleanup

MongoDB and Flink volumes grow over time:
```bash
# Check volume sizes
docker system df -v

# Clean old checkpoints (inside Flink container)
docker exec streamforge-flink-jobmanager find /tmp/flink-checkpoints -mtime +7 -delete

# Reset MongoDB data (clears test data)
docker compose down -v
docker compose up -d
# Re-initialize schema
```

### 4. Maven Build Optimization

Speed up builds and save disk:
```bash
cd flink-jobs

# Skip tests when rebuilding
mvn clean package -DskipTests

# Clean only target directory (keep dependencies)
mvn clean

# Offline mode (if dependencies already cached)
mvn package -o
```

### 5. Docker Image Cleanup

Remove unused images periodically:
```bash
# Remove dangling images (saves disk)
docker image prune

# Remove unused images (be careful - will re-download)
docker image prune -a --filter "until=168h"  # Older than 7 days
```

---

## Resource Monitoring

### Check Current Usage

```bash
# Real-time container stats
docker stats

# Disk usage by component
docker system df -v

# Specific volume sizes
du -sh /var/lib/docker/volumes/streamforge_*

# Memory usage per container
docker stats --no-stream --format "table {{.Container}}\t{{.MemUsage}}"
```

### Set Resource Limits (Optional)

Add to `docker-compose.yml`:
```yaml
services:
  flink-jobmanager:
    image: flink:1.18-java11
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 2G
        reservations:
          memory: 1G
```

---

## Emergency Stop (If System Slows Down)

### If Your Machine is Running Slow

**1. Stop StreamForge Services Immediately:**
```bash
cd /home/scotton/dev/projects/streamforge/docker
docker compose stop
```

**2. Check for Runaway Processes:**
```bash
# Check if Flink jobs are stuck in loop
docker logs streamforge-flink-jobmanager --tail 100 | grep ERROR

# Check Kafka disk usage
docker exec streamforge-kafka du -sh /var/lib/kafka/data
```

**3. Force Kill If Necessary:**
```bash
# Last resort - force kill all containers
docker compose kill

# Then clean up
docker compose down
```

---

## Safe Destruction Workflow

### Step-by-Step Safe Destroy Process

```bash
# 1. Navigate to Docker directory
cd /home/scotton/dev/projects/streamforge/docker

# 2. Check current state
docker compose ps

# 3. Stop running Flink jobs (optional)
curl -X PATCH http://localhost:8081/jobs/<JOB_ID>?mode=cancel

# 4. Check for important data in MongoDB
docker exec streamforge-mongodb mongosh \
  -u admin -p password \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('streamforge').processed_data.countDocuments()"

# 5. Backup if needed (optional)
docker exec streamforge-mongodb mongodump \
  -u admin -p password \
  --authenticationDatabase admin \
  -d streamforge \
  -o /data/backup
docker cp streamforge-mongodb:/data/backup ./backup-$(date +%Y%m%d)

# 6. Stop containers (reversible - keeps volumes)
docker compose stop

# OR destroy containers + volumes (irreversible - clears data)
docker compose down -v

# 7. Verify destruction
docker compose ps  # Should show nothing or "exited"
docker volume ls | grep streamforge  # Check volumes

# 8. Document what you did
echo "Stopped StreamForge containers on $(date)" >> ../destruction_log.txt
```

---

## When to Destroy Resources

### Destroy/Stop If:
- ✅ Done working for the day (just `docker compose stop`)
- ✅ Need to free up RAM for other work
- ✅ Want fresh test environment (use `down -v`)
- ✅ Finished with project for >30 days

### Keep Running If:
- ❌ Actively developing or testing
- ❌ Running integration tests
- ❌ Demonstrating project to someone
- ❌ Minimal impact on system resources (<10% RAM usage)

---

## Conclusion & Recommendation

### For Your Current Situation

**Recommended Action: STOP CONTAINERS WHEN NOT IN USE**

**Reasoning:**
1. **Local Development:** No cloud costs, only local compute resources
2. **Quick Restart:** 30 seconds to resume with `docker compose start`
3. **Data Persistence:** Volumes preserve all data between restarts
4. **Resource Usage:** ~3 GB RAM when running, 0 GB when stopped
5. **Build Artifacts:** JAR and dependencies cached for fast rebuilds

**Daily Workflow:**
```bash
# Start of day
cd ~/dev/projects/streamforge/docker && docker compose start

# End of day
docker compose stop
```

**Weekly Cleanup (Optional):**
```bash
# Clear test data and start fresh
docker compose down -v
docker compose up -d
# Re-initialize MongoDB + redeploy Flink job (3 minutes)
```

### Resource Usage Summary

| Scenario | RAM Usage | Disk Usage | Startup Time |
|----------|-----------|------------|--------------|
| **All Running** | ~3 GB | ~420 MB | - |
| **All Stopped** | 0 GB | ~420 MB | 30s restart |
| **Volumes Cleared** | 0 GB | ~270 MB | 3-5min rebuild |
| **Full Cleanup** | 0 GB | ~50 MB | 10-15min rebuild |

---

## Quick Reference Commands

### Safe to Run Anytime (No Destruction)
```bash
# Check resource usage
docker stats --no-stream
docker system df

# Check running services
docker compose ps

# View logs
docker compose logs -f [service-name]
```

### Selective Destruction (Reversible)
```bash
# Stop containers (keeps volumes)
docker compose stop

# Stop and remove containers (keeps volumes)
docker compose down

# Remove specific volume
docker volume rm streamforge_mongodb-data
```

### Nuclear Option (Use with Caution)
```bash
# Destroy everything (containers + volumes + images)
docker compose down -v --rmi all

# Clean all Docker resources
docker system prune -a --volumes
```

---

**Document Version:** 1.0  
**Created:** December 31, 2025  
**Last Updated:** December 31, 2025  
**Author:** scotton  
**Project:** StreamForge Real-Time Streaming Platform  
**Status:** Local development environment - Recommendation: Stop containers daily, clear volumes weekly if needed
