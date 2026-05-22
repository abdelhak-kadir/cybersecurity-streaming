# Real-Time Cybersecurity Threat Detection — Lambda Architecture

A production-grade big data pipeline that ingests network security logs, detects threats in real time via Spark Structured Streaming, builds historical analytics with HDFS + HBase, trains an ML threat classifier, and serves everything through a FastAPI backend with a live Grafana dashboard and automated email alerting.

> **Source code:** [github.com/oubellaismail/cybersecurity-streaming](https://github.com/oubellaismail/cybersecurity-streaming)  
> **Dataset:** [Cybersecurity Threat Detection Logs](https://www.kaggle.com/datasets/aryan208/cybersecurity-threat-detection-logs) — 6.18M network log events (874 MB CSV)  
> **Note:** A live demo was hosted at `grafana.project-demo.tech` and `api.project-demo.tech`. Since the project has been defended, the cloud infrastructure has been shut down due to hosting costs.

---

## Architecture

```
                    ┌─────────────────────────────────────┐
                    │  Dataset — 6.18M log events, 874 MB  │
                    └──────────────┬──────────────────────┘
                           ┌───────┴────────┐
                           │                │
                           ▼                ▼
                 ┌──────────────────┐  ┌───────────────────┐
                 │  Kafka Producer  │  │   HDFS (Parquet)  │
                 │   10 events/s    │  │  year/month/day   │
                 └────────┬─────────┘  └────────┬──────────┘
                          │                     │
                          ▼                     ▼
         ╔════════════════════════╗  ╔══════════════════════════════╗
         ║      SPEED LAYER       ║  ║         BATCH LAYER          ║
         ║                        ║  ║                              ║
         ║  Kafka (3 partitions)  ║  ║  Spark Batch — 7 scripts     ║
         ║          │             ║  ║  + ML: Random Forest 97.1%   ║
         ║          ▼             ║  ║          │                   ║
         ║  Spark Structured      ║  ║          ▼                   ║
         ║  Streaming             ║  ║  HBase (batch views)         ║
         ║  • Brute-force         ║  ║  • ip_reputation             ║
         ║  • Attack signatures   ║  ║  • attack_patterns           ║
         ║  • Volume anomaly      ║  ║  • threat_timeline           ║
         ║  • Port scan           ║  ║  • ML model (HDFS)           ║
         ║  • APT kill-chain      ║  ╚══════════════╤═══════════════╝
         ║          │             ║                 │
         ║          ▼             ║                 │
         ║  Cassandra (24h TTL)   ║                 │
         ║  • realtime_threats    ║                 │
         ║  • ip_threat_summary   ║                 │
         ║  • correlated_attacks  ║                 │
         ╚══════════╤═════════════╝                 │
                    └──────────────────┬────────────┘
                                       │
                                       ▼
              ╔════════════════════════════════════════╗
              ║            SERVING LAYER               ║
              ║   FastAPI :8000  —  15 REST endpoints  ║
              ║   /api/ip/{ip}  /api/threats/live      ║
              ║   /api/scoring/adaptive  /api/ml/*     ║
              ║   /metrics  (Prometheus format)        ║
              ╚══════════════╤═════════════════════════╝
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
 ┌────────────────────────┐   ┌─────────────────────────────────┐
 │   Prometheus :9090     │   │   Grafana :3000                 │
 │   scrapes /metrics     ├──▶│   Infinity plugin  (dashboard)  │
 │   every 15 seconds     │   │   Unified Alerting (4 rules)    │
 └────────────────────────┘   └──────────────────┬──────────────┘
                                                 │ threshold breach
                                                 ▼
                               ┌──────────────────────────────────┐
                               │  SendGrid SMTP :2525             │
                               │  From: alert@project-demo.tech   │
                               └──────────────────┬───────────────┘
                                                  ▼
                                         Email notification

──────────────────── PUBLIC ACCESS (cloud) ────────────────────────
  Browser ──▶ Caddy :443  (HTTPS · automatic Let's Encrypt cert)
               ├── grafana.project-demo.tech ──▶ Grafana :3000
               └── api.project-demo.tech    ──▶ FastAPI :8000
                                                (EXPOSE_API=true)
```

---

## Tech Stack

| Layer | Technology | Version | Role |
|-------|-----------|---------|------|
| Ingestion | Apache Kafka + Zookeeper | 7.4.0 (CP) | Durable event log, 3-partition topic |
| Speed processing | Spark Structured Streaming | 3.3.0 | Real-time threat detection (5 rules) |
| Batch processing | Spark Batch | 3.3.0 | Historical analytics + ML training |
| Speed storage | Apache Cassandra | 4.1 | High-write, native 24h TTL |
| Batch storage | HBase + HDFS | 2.1 / 3.2.1 | Pre-computed batch views |
| ML | PySpark MLlib — Random Forest | 3.3.0 | 97.1% accuracy on 6.18M records |
| Serving | FastAPI + Python | 0.115 / 3.12 | 15-endpoint REST API, Lambda merge |
| Metrics | Prometheus | 2.51.0 | Pull-based scraping of `/metrics` every 15s |
| Dashboard & Alerting | Grafana + Infinity plugin | 10.4.2 | Live panels + 4 email alert rules |
| Email delivery | SendGrid SMTP | — | Authenticated sender `alert@project-demo.tech` |
| Reverse proxy | Caddy 2 | 2-alpine | Automatic HTTPS via Let's Encrypt |
| Infrastructure | Terraform + DigitalOcean | 1.x | 3-droplet VPC cluster provisioning |
| Configuration | Ansible | 2.x | Idempotent deployment over SSH |
| Containerisation | Docker Compose | v2 | Multi-profile local + per-droplet cloud |

---

## Project Structure

```
cybersecurity-streaming/
├── streaming/
│   ├── kafka_producer.py           # Replays CSV into Kafka (10 ev/s)
│   └── spark_streaming.py          # 5 detection rules + correlation → Cassandra
├── batch/scripts/
│   ├── 01_load_hdfs.py             # CSV → HDFS Parquet (partitioned by date)
│   ├── 02_top_ips.py               # Top IPs by threat score → HBase
│   ├── 03_Threat_Volume_Analysis.py
│   ├── 03_port_scans.py
│   ├── 04_Attack_Evolution.py
│   ├── 05_brute_force.py
│   ├── 06_SQLi_XSS.py
│   └── 07_hbase_storage.py         # Persist all views → HBase tables
├── ml/scripts/
│   ├── 08_ml_threat_classification.py  # Train Random Forest, export ml_summary.json
│   └── 09_ml_predict.py
├── serving/
│   ├── main.py                     # FastAPI — 14 endpoints + /metrics
│   ├── requirements.txt            # includes prometheus-client
│   ├── db/cassandra_client.py
│   ├── db/hbase_client.py
│   └── Dockerfile
├── prometheus/
│   └── prometheus.yml              # Scrape config: serving:8000/metrics, 15s interval
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   ├── infinity.yml        # Infinity REST datasource → FastAPI
│   │   │   └── prometheus.yml      # Prometheus datasource (uid: prometheus)
│   │   └── alerting/
│   │       ├── rules.yml           # 4 Unified Alerting rules (A→B→C pipeline)
│   │       ├── contact-points.yml  # SendGrid email contact point
│   │       └── notification-policies.yml
│   └── dashboards/cybersecurity.json
├── caddy/
│   └── Caddyfile.j2                # Ansible-rendered HTTPS config
├── terraform/
│   ├── main.tf                     # VPC, 3 droplets, firewalls, DNS
│   ├── variables.tf
│   └── outputs.tf
├── ansible/
│   ├── generate_inventory.sh       # terraform output → inventory.ini
│   └── playbooks/
│       ├── install_docker.yml
│       ├── deploy_infra.yml        # Droplet 1: Kafka + Cassandra
│       ├── deploy_compute.yml      # Droplet 2: Spark + HBase + HDFS
│       ├── deploy_serve.yml        # Droplet 3: FastAPI + Prometheus + Grafana + Caddy
│       ├── deploy_batch.yml        # Run full batch pipeline
│       ├── verify.yml              # Health checks across all droplets
│       └── sync_ml_summary.yml
├── docker-compose.yml              # Local dev (profiles: batch / stream / serve / test)
├── docker-compose.infra.yml        # Droplet 1 services
├── docker-compose.compute.yml      # Droplet 2 services
├── docker-compose.serve.yml        # Droplet 3 services
├── .env.example
└── Makefile                        # All commands — local and cloud
```

---

## Running Locally

### Prerequisites

- Docker Desktop (≥ 4.x) with at least **10 GB RAM** allocated
- `make`
- Dataset CSV in `data/cybersecurity_threat_detection_logs.csv`  
  _(Download from [Kaggle](https://www.kaggle.com/datasets/aryan208/cybersecurity-threat-detection-logs))_

### Setup

```bash
git clone https://github.com/oubellaismail/cybersecurity-streaming
cd cybersecurity-streaming
cp .env.example .env
mkdir -p data
# → place cybersecurity_threat_detection_logs.csv in data/
```

### Start the stack

```bash
make all          # Start full stack (batch + stream + serve profiles)

# Or by layer:
make batch        # HDFS + HBase + Spark batch
make stream       # Kafka + Cassandra + Spark streaming
```

### Run the batch pipeline

```bash
make batch-load       # CSV → HDFS Parquet (~10 min for 6.18M rows)
make batch-analytics  # Scripts 02–06 (analytics)
make batch-hbase      # Script 07 — write results to HBase
make batch-all        # All of the above in sequence
```

### Run ML training

```bash
make ml-train    # Train Random Forest on 4.94M records (~12 min)
make ml-predict  # Run predictions on held-out 1.24M records
```

### Local access

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| FastAPI docs | http://localhost:8000/docs | — |
| FastAPI metrics | http://localhost:8000/metrics | — |
| Spark master UI | http://localhost:8080 | — |
| HDFS namenode | http://localhost:9870 | — |

### Useful commands

```bash
make status           # Show running containers
make logs             # Tail all logs
make shell-cassandra  # Open cqlsh
make shell-kafka      # Open Kafka shell
make serving-test     # Smoke-test all 14 API endpoints via curl
make test             # Unit tests inside Docker
make down             # Stop everything
make clean            # Stop and remove all volumes
```

---

## Cloud Deployment (DigitalOcean — 3 Droplets)

```
Droplet 1 "infra"    s-2vcpu-4gb  (~$24/mo)   Kafka + Zookeeper + Cassandra
Droplet 2 "compute"  s-4vcpu-8gb  (~$48/mo)   Spark + HBase + HDFS + producer + streaming
Droplet 3 "serve"    s-2vcpu-2gb  (~$12/mo)   FastAPI + Prometheus + Grafana + Caddy
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

# 5. Fill in .env
cp .env.example .env
# Set at minimum:
#   DIGITALOCEAN_TOKEN, DOMAIN, GRAFANA_SUBDOMAIN, ACME_EMAIL
#   SENDGRID_API_KEY, ALERT_EMAIL
#   TF_VAR_ssh_public_key_path / TF_VAR_ssh_private_key_path

make tf-init
```

### Deploy everything

```bash
make deploy-all
```

| Step | What it does |
|------|-------------|
| `tf-apply` | Provision VPC, 3 droplets, firewalls |
| `gen-inventory` | Write `ansible/inventory.ini` from Terraform outputs |
| `ansible-install-docker` | Install Docker CE on all 3 droplets |
| `ansible-deploy-infra` | Start Kafka, Cassandra, create topic + schema |
| `ansible-deploy-compute` | Start HDFS, HBase, Spark, producer, streaming |
| `ansible-deploy-serve` | Start FastAPI, Prometheus, Grafana, Caddy |
| `ansible-deploy-batch` | Run full batch pipeline (CSV→HDFS→analytics→HBase) |
| `ansible-verify` | Health checks across all droplets |

Total: ~30–45 minutes (batch pipeline dominates).

### Cloud access

```
Grafana:  https://grafana.your-domain.com   (admin / your GF_SECURITY_ADMIN_PASSWORD)
FastAPI:  internal only by default
          set EXPOSE_API=true to expose at https://api.your-domain.com/docs
```

### Individual commands

```bash
make tf-apply                # Provision infrastructure only
make tf-destroy              # Tear down all cloud resources
make tf-ips                  # Print droplet IPs

make ansible-deploy-infra    # Redeploy Droplet 1
make ansible-deploy-compute  # Redeploy Droplet 2
make ansible-deploy-serve    # Redeploy Droplet 3 (injecting secrets from .env)
make ansible-deploy-batch    # Re-run batch pipeline
make ansible-verify          # Health checks only
make sync-ml-summary         # Push updated ml_summary.json from compute→serve
```

### Firewall rules

| Droplet | Public | VPC-only |
|---------|--------|----------|
| infra | 22 (SSH) | 9092 (Kafka) from compute; 9042 (Cassandra) from compute + serve |
| compute | 22 (SSH) | 9090 (HBase Thrift) from serve |
| serve | 22 (SSH), 80 (HTTP), 443 (HTTPS) | — |

---

## API Endpoints

The FastAPI serving layer merges data from Cassandra (speed view) and HBase (batch view).

```
GET /health                          → {"status":"ok","cassandra":true,"hbase":true}
GET /metrics                         → Prometheus text format (6 cyber_* gauges)

# Speed layer (Cassandra — real-time)
GET /api/threats/live?minutes=60&limit=100
GET /api/threats/correlated?minutes=60&limit=100
GET /api/scoring/adaptive?minutes=60&limit=50
GET /api/ip/{ip}                     → Lambda merge: max(batch_score, realtime_score)

# Batch layer (HBase — historical)
GET /api/stats/top-ips?limit=10
GET /api/stats/threat-volume
GET /api/stats/threat-timeline?days=30
GET /api/stats/geo-threats
GET /api/stats/attack-patterns
GET /api/stats/attacks-by-protocol   → Attack distribution by protocol

# ML layer
GET /api/ml/summary
GET /api/ml/metrics
GET /api/ml/prediction-counts
GET /api/ml/feature-importance?limit=13
```

Interactive docs: `http://localhost:8000/docs`

---

## Grafana Dashboard & Alerting

### Dashboard panels

The main dashboard uses the **Infinity** datasource to query the FastAPI REST API directly.

| Panel | Data source |
|-------|-------------|
| Live Threat Feed | `/api/threats/live` |
| Threat Origin Map | `/api/stats/geo-threats` |
| Adaptive IP Risk Scores | `/api/scoring/adaptive` |
| Top 10 Malicious IPs | `/api/stats/top-ips` |
| Attack Patterns (SQLi/XSS/port-scan) | `/api/stats/attack-patterns` |
| Threat Timeline (30 days) | `/api/stats/threat-timeline` |
| Threat Volume by Label | `/api/stats/threat-volume` |
| Multi-Step Correlations | `/api/threats/correlated` |
| Attack Types by Protocol | `/api/stats/attacks-by-protocol` |
| ML Model Summary | `/api/ml/summary` + `/api/ml/metrics` |

### Alerting pipeline

Grafana Unified Alerting evaluates 4 rules every 60 seconds against Prometheus data:

| Rule | Condition | Fires after | Severity |
|------|-----------|-------------|----------|
| Critical Threat Detected | Any IP with score ≥ 90 active | 1 min | critical |
| Threat Wave | > 100 active threats in 60 min | 5 min | warning |
| APT Kill-Chain Detected | Max threat score reaches 100 | 1 min | critical |
| Brute-Force Wave | > 50 brute-force alerts in 60 min | 2 min | warning |

**Delivery:** SendGrid SMTP on port 2525 (port 587 is blocked by DigitalOcean; 2525 is SendGrid's designated alternative). The sender domain `project-demo.tech` is authenticated with SPF + DKIM records in DigitalOcean DNS.

---

## ML Results

Trained on a stratified 80/20 split — 4.94M training records, 1.24M held-out test records.

| Metric | Value |
|--------|-------|
| Accuracy | **97.11%** |
| F1 score (macro) | 0.971 |
| Precision (macro) | 0.972 |
| Recall (macro) | 0.971 |
| Baseline (majority class) | 34.00% |

Per-class breakdown:

| Class | Precision | Recall | F1 |
|-------|-----------|--------|----|
| benign | 0.979 | 0.981 | 0.980 |
| suspicious | 0.958 | 0.952 | 0.955 |
| malicious | 0.979 | 0.981 | 0.980 |

Top features by Gini importance: `is_blocked` > `has_tool_ua` > `bytes_log`.  
Results available at runtime via `GET /api/ml/feature-importance`.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `kafka:29092` | Kafka bootstrap server |
| `CASSANDRA_HOST` | `cassandra` | Cassandra hostname |
| `CASSANDRA_PORT` | `9042` | Cassandra port |
| `HBASE_HOST` | `hbase` | HBase Thrift host |
| `HBASE_PORT` | `9090` | HBase Thrift port |
| `SPARK_WORKER_MEMORY` | `2G` | Memory per Spark worker |
| `SPARK_WORKER_CORES` | `2` | Cores per Spark worker |
| `CSV_FILE` | `/data/cybersecurity_threat_detection_logs.csv` | Dataset path |
| `SEND_DELAY` | `0.1` | Seconds between Kafka messages (0.1 = 10/s) |
| `MAX_MESSAGES` | `0` | 0 = loop forever |
| `ML_SUMMARY_PATH` | `/data/ml_summary.json` | ML metrics file |
| `GF_SECURITY_ADMIN_PASSWORD` | `admin` | Grafana admin password |
| `SENDGRID_API_KEY` | — | SendGrid API key for alert emails |
| `ALERT_EMAIL` | — | Recipient address for alert notifications |
| `DOMAIN` | — | Root domain (cloud only) |
| `GRAFANA_SUBDOMAIN` | — | Full Grafana subdomain |
| `ACME_EMAIL` | — | Let's Encrypt registration email |
| `EXPOSE_API` | `false` | Expose FastAPI publicly via Caddy |
| `DIGITALOCEAN_TOKEN` | — | DO API token (cloud only) |
| `ANSIBLE_SSH_KEY` | `~/.ssh/do_cyber` | SSH key for Ansible |

---

## Known Limitations

- **No Kafka replication.** Topic created with `replication-factor=1`. A single broker failure causes data loss. Production requires RF ≥ 3.
- **HBase Thrift API is deprecated.** `happybase` uses Thrift 1; future HBase upgrades may break compatibility.
- **Static ML model.** Trained once on a fixed corpus; no concept drift detection or incremental retraining.
- **ip-api.com rate limit.** 45 requests/min for unauthenticated callers; the geo-threats endpoint may return incomplete results under load.
- **No API authentication.** The FastAPI layer has no auth or rate limiting — unsuitable for public exposure without a gateway.
