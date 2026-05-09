# Real-Time Cybersecurity Threat Detection — Lambda Architecture

A production-grade big data pipeline that ingests network security logs, detects threats in real time using Spark Structured Streaming, builds batch analytics with HDFS + HBase, trains an ML threat classifier, and serves everything through a FastAPI backend with a live Grafana dashboard.

> Dataset: [Cybersecurity Threat Detection Logs](https://www.kaggle.com/datasets/teamincribo/cyber-security-attacks) — 6 million network log events (834 MB CSV).

---

## Architecture

```
                        ┌─────────────────────────────────────────────┐
                        │              Kafka Producer                  │
                        │         (replays CSV at 10 events/s)        │
                        └──────────────────┬──────────────────────────┘
                                           │
                                           ▼
                              ┌────────────────────────┐
                              │   Apache Kafka :9092    │
                              │  topic: cybersecurity-  │
                              │         logs            │
                              └────────────┬───────────┘
                                           │
                     ┌─────────────────────┴─────────────────────┐
                     │                                           │
                     ▼                                           ▼
        ┌────────────────────────┐              ┌────────────────────────────┐
        │     SPEED LAYER        │              │       BATCH LAYER          │
        │  Spark Structured      │              │  Spark Batch (scheduled)   │
        │  Streaming             │              │                            │
        │  • Brute-force detect  │              │  01. CSV → HDFS (Parquet)  │
        │  • Port scan detect    │              │  02. Top IPs by score      │
        │  • Attack signatures   │              │  03. Threat volume stats   │
        │  • Multi-step attack   │              │  04. Attack evolution      │
        │    correlation         │              │  05. Brute-force patterns  │
        │  • Adaptive IP scoring │              │  06. SQLi / XSS detection  │
        └──────────┬─────────────┘              │  07. Write results → HBase │
                   │                            │  08. ML training (RF + LR) │
                   ▼                            └────────────┬───────────────┘
        ┌────────────────────────┐                           │
        │  Cassandra (speed DB)  │                           ▼
        │  • realtime_threats    │              ┌────────────────────────────┐
        │  • ip_threat_summary   │              │  HDFS + HBase (batch DB)   │
        │  • correlated_attacks  │              │  • ip_reputation           │
        └──────────┬─────────────┘              │  • threat_timeline         │
                   │                            │  • attack_patterns         │
                   └──────────────┬─────────────┘  • ML model (HDFS)        │
                                  │             └────────────┬───────────────┘
                                  ▼                          │
                     ┌────────────────────────┐              │
                     │     SERVING LAYER      │◄─────────────┘
                     │   FastAPI :8000        │
                     │  /api/threats/live     │
                     │  /api/stats/top-ips    │
                     │  /api/ml/summary       │
                     │  /api/scoring/adaptive │
                     └──────────┬─────────────┘
                                │
                                ▼
                     ┌────────────────────────┐
                     │   Grafana :3000        │
                     │   (Infinity plugin)    │
                     │   Live dashboard       │
                     └────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Role |
|-------|-----------|------|
| Ingestion | Apache Kafka 7.4.0 + Zookeeper | Event streaming backbone |
| Speed processing | Apache Spark 3.3.0 Structured Streaming | Real-time threat detection |
| Batch processing | Apache Spark 3.3.0 (batch mode) | Historical analytics & ML |
| Speed storage | Apache Cassandra 4.1 | Low-latency threat queries |
| Batch storage | Apache HBase 2.1 + HDFS 3.2.1 | Aggregated analytics |
| ML | PySpark MLlib — Random Forest + Logistic Regression | Threat classification |
| Serving | FastAPI + Python 3.9 | REST API over both stores |
| Visualization | Grafana 10.4.2 + Infinity datasource | Live dashboard |
| Reverse proxy | Caddy 2 | Automatic HTTPS via Let's Encrypt |
| Infrastructure | Terraform + DigitalOcean | Cloud provisioning |
| Configuration | Ansible | Droplet setup & deployment |

---

## Project Structure

```
cybersecurity-streaming/
├── streaming/
│   ├── kafka_producer.py        # Replays CSV into Kafka
│   └── spark_streaming.py       # 6 parallel streaming queries → Cassandra
├── batch/
│   └── scripts/
│       ├── 01_load_hdfs.py      # CSV → HDFS Parquet (partitioned by date)
│       ├── 02_top_ips.py        # Top IPs by threat score
│       ├── 03_Threat_Volume_Analysis.py
│       ├── 03_port_scans.py
│       ├── 04_Attack_Evolution.py
│       ├── 05_brute_force.py
│       ├── 06_SQLi_XSS.py
│       └── 07_hbase_storage.py  # Write all results → HBase tables
├── ml/
│   └── scripts/
│       ├── 08_ml_threat_classification.py  # Train RF + LR, export ml_summary.json
│       └── 09_ml_predict.py
├── serving/
│   ├── main.py                  # FastAPI app — queries Cassandra + HBase + JSON
│   ├── db/
│   │   ├── cassandra_client.py
│   │   └── hbase_client.py
│   └── Dockerfile
├── grafana/
│   ├── provisioning/datasources/infinity.yml
│   └── dashboards/cybersecurity.json
├── docker/
│   ├── spark/                   # Custom Spark image with Python deps
│   ├── kafka-setup/             # Topic creation script
│   ├── cassandra-init.cql       # Keyspace + table DDL
│   └── cassandra-reset.cql
├── caddy/
│   └── Caddyfile.j2             # Ansible-rendered Caddy config (HTTPS)
├── terraform/
│   ├── main.tf                  # VPC, 3 droplets, firewalls, DNS
│   ├── variables.tf
│   └── outputs.tf
├── ansible/
│   ├── generate_inventory.sh    # Terraform outputs → inventory.ini
│   └── playbooks/
│       ├── install_docker.yml
│       ├── deploy_infra.yml     # Droplet 1: Kafka + Cassandra
│       ├── deploy_compute.yml   # Droplet 2: Spark + HBase + HDFS
│       ├── deploy_serve.yml     # Droplet 3: FastAPI + Grafana + Caddy
│       ├── deploy_batch.yml     # Run full batch pipeline
│       ├── verify.yml           # Health checks across all droplets
│       └── sync_ml_summary.yml  # Push updated ML metrics to serve
├── docker-compose.yml           # Local development (all-in-one)
├── docker-compose.infra.yml     # Droplet 1 services
├── docker-compose.compute.yml   # Droplet 2 services
├── docker-compose.serve.yml     # Droplet 3 services
├── .env.example                 # Environment variable template
└── Makefile                     # All commands — local and cloud
```

---

## Running Locally

### Prerequisites

- Docker Desktop (≥ 4.x) with at least **10 GB RAM** allocated
- `make`
- The dataset CSV in `data/cybersecurity_threat_detection_logs.csv`  
  _(Download from [Kaggle](https://www.kaggle.com/datasets/teamincribo/cyber-security-attacks))_

### Setup

```bash
git clone <repo>
cd cybersecurity-streaming

# Copy env template and adjust if needed (defaults work for local)
cp .env.example .env

# Place the CSV
mkdir -p data
# → put cybersecurity_threat_detection_logs.csv in data/
```

### Start the full stack

```bash
# Start all services (Kafka, Cassandra, HBase, HDFS, Spark, FastAPI, Grafana)
make all

# Or start layers individually:
make batch    # batch stack only (HDFS + HBase + Spark)
make stream   # streaming stack only (Kafka + Cassandra + Spark streaming)
```

### Run the batch pipeline

After the stack is up and the producer has been sending data for a few minutes:

```bash
make batch-load       # Load CSV into HDFS (takes ~10 min for full dataset)
make batch-analytics  # Run analytics scripts (02–06)
make batch-hbase      # Write results to HBase (07)
make batch-all        # All of the above in sequence
```

### Run ML training

```bash
make ml-train   # Train Random Forest + Logistic Regression (~20 min)
make ml-predict # Run predictions on full dataset
```

### Access

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| FastAPI docs | http://localhost:8000/docs | — |
| Spark master UI | http://localhost:8080 | — |
| HDFS namenode | http://localhost:9870 | — |

### Useful commands

```bash
make status          # Show running containers
make logs            # Tail all logs
make shell-kafka     # Open Kafka shell
make shell-cassandra # Open cqlsh
make shell-spark     # Open Spark shell
make down            # Stop everything
make clean           # Stop and remove all volumes
```

---

## Cloud Deployment (DigitalOcean — 3 Droplets)

The full stack is split across 3 droplets connected via a private VPC. One command provisions everything from scratch.

### Target architecture

```
Droplet 1 "infra"    s-2vcpu-4gb  ($24/mo)   Kafka + Zookeeper + Cassandra
Droplet 2 "compute"  s-4vcpu-8gb  ($48/mo)   Spark + HBase + HDFS + producer + streaming
Droplet 3 "serve"    s-2vcpu-2gb  ($12/mo)   FastAPI + Grafana + Caddy (HTTPS)
```

### One-time prerequisites

```bash
# 1. Install tools
brew install terraform ansible

# 2. Generate SSH key pair for the cluster
ssh-keygen -t ed25519 -f ~/.ssh/do_cyber -C "cyber-cluster"

# 3. Create a DigitalOcean API token (read + write)
#    → https://cloud.digitalocean.com/account/api/tokens

# 4. Point your domain's nameservers at DigitalOcean
#    ns1.digitalocean.com / ns2.digitalocean.com / ns3.digitalocean.com

# 5. Fill in .env (copy from .env.example)
cp .env.example .env
# Edit .env — set at minimum:
#   DIGITALOCEAN_TOKEN=dop_v1_...
#   DOMAIN=your-domain.com
#   GRAFANA_SUBDOMAIN=grafana.your-domain.com
#   ACME_EMAIL=you@example.com
#   ANSIBLE_SSH_KEY=~/.ssh/do_cyber
#   TF_VAR_ssh_public_key_path=~/.ssh/do_cyber.pub
#   TF_VAR_ssh_private_key_path=~/.ssh/do_cyber

# 6. Initialize Terraform
make tf-init
```

### Deploy everything

```bash
make deploy-all
```

This runs the full pipeline in order:

| Step | Command | What it does |
|------|---------|-------------|
| 1 | `tf-apply` | Provision VPC, 3 droplets, firewalls, DNS records |
| 2 | `gen-inventory` | Write `ansible/inventory.ini` from Terraform outputs |
| 3 | `ansible-install-docker` | Install Docker CE on all 3 droplets |
| 4 | `ansible-deploy-infra` | Start Kafka, Cassandra, create topic + schema |
| 5 | `ansible-deploy-compute` | Start HDFS, HBase, Spark, producer, streaming |
| 6 | `ansible-deploy-serve` | Start FastAPI, Grafana (with Infinity plugin), Caddy |
| 7 | `ansible-deploy-batch` | Run full batch pipeline (CSV→HDFS→analytics→HBase) |
| 8 | `ansible-verify` | Health checks across all droplets |

Total time: ~30–45 minutes (batch pipeline dominates).

### Access after deployment

```
Grafana:  https://grafana.your-domain.com   (admin / your password)
FastAPI:  internal only by default
          set EXPOSE_API=true in .env to expose at https://api.your-domain.com
```

HTTPS certificates are provisioned automatically by Caddy via Let's Encrypt on first request.

### Individual deployment commands

```bash
make tf-apply                # Provision cloud infrastructure only
make tf-destroy              # Tear down all cloud resources
make tf-ips                  # Print droplet IPs from Terraform state

make ansible-deploy-infra    # Redeploy infra layer only
make ansible-deploy-compute  # Redeploy compute layer only
make ansible-deploy-serve    # Redeploy serve layer only
make ansible-deploy-batch    # Re-run batch pipeline (idempotent)
make ansible-verify          # Health checks only

make sync-ml-summary         # Push updated ml_summary.json from compute→serve
                             # (run after retraining the ML model)
```

### Firewall rules

| Droplet | Public ports | VPC-only ports |
|---------|-------------|----------------|
| infra | 22 (SSH) | 9092 (Kafka) from compute; 9042 (Cassandra) from compute + serve |
| compute | 22 (SSH) | 9090 (HBase Thrift) from serve |
| serve | 22 (SSH), 80 (HTTP), 443 (HTTPS) | — |

---

## Environment Variables

All configuration lives in `.env` (copy from `.env.example`).

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `kafka:29092` | Kafka bootstrap server |
| `CASSANDRA_HOST` | `cassandra` | Cassandra hostname |
| `CASSANDRA_PORT` | `9042` | Cassandra port |
| `HBASE_HOST` | `hbase` | HBase Thrift host |
| `HBASE_PORT` | `9090` | HBase Thrift port |
| `SPARK_WORKER_MEMORY` | `2G` | Memory per Spark worker |
| `SPARK_WORKER_CORES` | `2` | Cores per Spark worker |
| `CSV_FILE` | `/data/cybersecurity_threat_detection_logs.csv` | Dataset path inside container |
| `SEND_DELAY` | `0.1` | Seconds between Kafka messages (0.1 = 10/s) |
| `MAX_MESSAGES` | `0` | 0 = loop forever |
| `CHECKPOINT_BASE` | `/tmp/spark-checkpoints-v3` | Spark streaming checkpoint dir |
| `ML_SUMMARY_PATH` | `/data/ml_summary.json` | ML metrics file for serving layer |
| `GF_SECURITY_ADMIN_PASSWORD` | `admin` | Grafana admin password |
| `DOMAIN` | — | Root domain (cloud deployment) |
| `GRAFANA_SUBDOMAIN` | — | Full Grafana subdomain |
| `ACME_EMAIL` | — | Let's Encrypt registration email |
| `EXPOSE_API` | `false` | Expose FastAPI publicly via Caddy |
| `DIGITALOCEAN_TOKEN` | — | DO API token (cloud deployment) |
| `ANSIBLE_SSH_KEY` | `~/.ssh/do_cyber` | SSH key for Ansible |

---

## API Endpoints

The FastAPI serving layer merges data from Cassandra (speed) and HBase (batch).

### Health

```
GET /health
→ {"status": "ok", "cassandra": true, "hbase": true}
```

### Speed layer (Cassandra — real-time)

```
GET /api/threats/live?minutes=60&limit=100
GET /api/threats/correlated?minutes=60&limit=100
GET /api/scoring/adaptive?minutes=60&limit=50
```

### Batch layer (HBase — historical)

```
GET /api/stats/top-ips?limit=10
GET /api/stats/threat-volume
GET /api/stats/threat-timeline?days=30
GET /api/stats/geo-threats
```

### ML layer

```
GET /api/ml/summary
GET /api/ml/metrics
GET /api/ml/prediction-counts
GET /api/ml/feature-importance?limit=13
```

Interactive docs: `http://localhost:8000/docs`

---

## Grafana Dashboard

The dashboard (`grafana/dashboards/cybersecurity.json`) uses the **Infinity** datasource plugin to query the FastAPI REST API directly — no additional database connector needed.

**Panels:**

| Panel | Source | Data |
|-------|--------|------|
| System Health | `/health` | Cassandra + HBase connectivity |
| Live Threat Feed | Speed layer | Last 60 min threats |
| Top 10 Malicious IPs | Batch layer | IP reputation scores |
| Attack Timeline | Batch layer | 30-day threat trend |
| Threat Volume | Batch layer | Bytes by threat label |
| Threat Origins Map | Batch layer | Geo-located threat IPs |
| ML Model Status | ML layer | Training status + F1 score |
| ML Metrics Table | ML layer | Random Forest vs Logistic Regression |
| ML Prediction Distribution | ML layer | Benign / Suspicious / Malicious counts |
| ML Feature Importance | ML layer | Top predictive features |
| Correlated Multi-Step Attacks | Speed layer | Multi-stage attack chains |
| Adaptive IP Risk Scores | Speed layer | Time-decayed risk scores per IP |

---

## ML Model Results

Trained on 4.8M events (80/20 split), 5-fold cross-validation:

| Model | Accuracy | F1 Score | Precision |
|-------|----------|----------|-----------|
| Random Forest | 96.95% | 96.64% | 96.74% |
| Logistic Regression | 95.57% | 94.77% | 95.45% |

Top feature by importance: `path_length` (0.75), followed by `has_sqli` (0.11).

---

## Known Limitations

- **Batch memory contention**: the batch pipeline and Spark streaming share the same 8 GB compute droplet. Batch jobs are capped at `--driver-memory 1g --executor-memory 1500m` to avoid OOM-killing the streaming job. Larger droplets remove this constraint.
- **CSV timestamps**: the dataset's `timestamp` column contains historical dates. The streaming layer uses Kafka arrival time (`current_timestamp()`) for windowing — not CSV timestamps.
- **Geo-location**: the geo-threats endpoint uses offline IP geolocation; accuracy varies for private/internal IPs.

---

## License

MIT
