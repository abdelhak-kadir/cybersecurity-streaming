# Cybersecurity Streaming — Speed Layer

Real-time threat detection using Kafka + Spark Streaming + Cassandra,
fully containerized with Docker.

## Architecture

```
CSV file
   ↓
kafka_producer.py  →  Kafka (cybersecurity-logs)  →  spark_streaming.py
                                                           ↓
                                              3 detection rules
                                              (brute-force, signatures, volume)
                                                           ↓
                                                      Cassandra
                                              (cybersecurity.realtime_threats)
```

## Services in Docker Compose

| Container        | Role                              | Port  |
|------------------|-----------------------------------|-------|
| zookeeper        | Required by Kafka                 | 2181  |
| kafka            | Message broker                    | 9092  |
| kafka-setup      | Creates the topic (runs once)     | —     |
| cassandra        | Stores detected threats           | 9042  |
| cassandra-init   | Creates schema (runs once)        | —     |
| spark-master     | Spark cluster master              | 8080  |
| spark-worker     | Spark cluster worker              | —     |
| producer         | Sends CSV logs to Kafka           | —     |
| spark-streaming  | Runs the 3 detection jobs         | —     |

## Quick Start

### 1. Prerequisites
- Docker Desktop installed and running
- Docker Compose v2+

### 2. Add your dataset
Place the CSV file here:
```
data/cybersecurity_logs.csv
```
Download from: https://www.kaggle.com/datasets/aryan208/cybersecurity-threat-detection-logs

> If no CSV is present, the producer runs in **demo mode** with synthetic data automatically.

### 3. Start everything
```bash
make up
# or without make:
docker compose up -d --build
```

### 4. Check all containers are running
```bash
make status
# or:
docker compose ps
```

### 5. Watch the Spark streaming logs
```bash
make logs s=spark-streaming
```

### 6. Verify threats are detected in Cassandra
```bash
make verify
# or:
docker exec -it cassandra cqlsh -e \
  "USE cybersecurity; SELECT * FROM realtime_threats LIMIT 20;"
```

### 7. Open Spark Web UI
Visit: http://localhost:8080

## Useful Commands

```bash
make logs s=kafka-producer      # watch producer logs
make logs s=spark-streaming     # watch detection logs
make logs s=cassandra           # watch cassandra logs
make watch-kafka                # see raw messages in Kafka
make shell-cassandra            # open cqlsh shell
make shell-kafka                # open kafka bash shell
make restart-producer           # restart producer only
make clean                      # stop + delete all containers + volumes
```

## Detection Rules

| Detection     | Rule                                              | Score |
|---------------|---------------------------------------------------|-------|
| Brute-force   | 5+ blocked requests from same IP in 1 minute     | 80    |
| Signatures    | sqlmap / nikto / OR 1=1 / UNION SELECT in request | 95   |
| Volume anomaly| >10 MB transferred by same IP in 10 seconds      | 70    |

## GitHub Workflow

```bash
# This is orobida's branch
git checkout develop
git checkout -b feature/streaming-kafka-spark

# Work on files in streaming/
git add streaming/ docker/ docker-compose.yml Makefile
git commit -m "feat(streaming): add Kafka producer + Spark detection + Cassandra"
git push origin feature/streaming-kafka-spark

# Open Pull Request: feature/streaming-kafka-spark → develop
```
