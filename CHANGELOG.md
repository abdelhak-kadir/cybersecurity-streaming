# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- Detection rule 5: ML-based real-time classification in `spark_streaming.py`.
  The trained Random Forest pipeline is loaded from HDFS at streaming job startup
  and applied to every raw Kafka event. Non-benign predictions are written to
  Cassandra as `attack_type = "ml-malicious"` (score 85) or `"ml-suspicious"`
  (score 60), making them visible in the live Grafana threat feed alongside
  rule-based detections. The stream starts gracefully if the model is not yet
  trained (`make ml-train` required first).
- `ML_MODEL_PATH` env var added to `.env.example` (default:
  `hdfs://namenode:9000/models/threat_classifier`)
- `ml-malicious` added to `EXPLOIT_STAGES` so ML-detected exploits can trigger
  the APT kill-chain correlation rule

---

## [1.0.0] - 2026-05-09

First complete, production-deployed release of the Lambda Architecture
cybersecurity threat detection platform.

### Added

**Speed Layer**
- Kafka producer replaying the 6.18M-row CSV dataset at 10 events/second
- Spark Structured Streaming with 5 real-time detection rules:
  brute-force (1-min window), attack signatures (sqlmap/nikto/SQLi),
  volume anomaly (>10 MB in 10 s), port scan, APT kill-chain correlation
- Cassandra speed store with 24-hour TTL — three tables:
  `realtime_threats`, `ip_threat_summary`, `correlated_attacks`

**Batch Layer**
- HDFS ingestion: CSV → Parquet partitioned by year/month/day
- 7 Spark batch scripts: top IPs, port scan detection, threat volume,
  attack evolution, brute-force patterns, SQLi/XSS extraction, HBase write
- HBase batch store with 3 tables: `ip_reputation`, `attack_patterns`,
  `threat_timeline`

**Machine Learning**
- PySpark MLlib Random Forest classifier on 6.18M records
- 13 engineered features from raw log fields
- 97.11% accuracy / macro F1 0.971 on 1.24M held-out records
- Model and metrics exported to `ml_summary.json`; served via API

**Serving Layer**
- FastAPI with 14 REST endpoints merging batch (HBase) and speed (Cassandra)
  views using Lambda max-merge logic
- `/metrics` endpoint in Prometheus exposition format (6 cyber threat gauges)
- Adaptive IP risk scoring with human-readable provenance reasoning
- IP geolocation enrichment via ip-api.com
- Auto-generated OpenAPI docs at `/docs`

**Observability & Alerting**
- Prometheus scraping `/metrics` every 15 seconds with 15-day retention
- Grafana Unified Alerting with 4 threshold rules (A→B→C pipeline):
  Critical Threat, Threat Wave, APT Kill-Chain, Brute-Force Wave
- Email delivery via SendGrid SMTP on port 2525 (bypasses DigitalOcean
  outbound block on 587)
- Custom authenticated sender domain `alert@project-demo.tech`
  with SPF + DKIM DNS records

**Dashboard**
- Grafana 10.4.2 dashboard with 9 panels, 5-second auto-refresh
- Panels: Live Threat Feed, Threat Origin Map, Adaptive Risk Scores,
  Top Malicious IPs, Attack Patterns, Threat Timeline, Threat Volume,
  Multi-Step Correlations, ML Model Summary
- Infinity plugin datasource querying FastAPI directly (no extra TSDB)

**Cloud Deployment**
- Terraform IaC: 3-droplet DigitalOcean cluster with private VPC,
  per-droplet firewalls, and DNS records
- 6 Ansible playbooks for fully automated deployment:
  `install_docker`, `deploy_infra`, `deploy_compute`, `deploy_serve`,
  `deploy_batch`, `verify`
- Caddy reverse proxy with automatic HTTPS via Let's Encrypt
- Single-command deployment: `make deploy-all` (~30–45 min end-to-end)
- Live at `https://grafana.project-demo.tech`

**Developer Experience**
- Docker Compose multi-profile setup (`batch` / `stream` / `serve` / `test`)
- `.env` / `.env.example` for all configuration
- Makefile with targets for every local and cloud operation
- pytest suite: unit tests (schemas) + integration tests (API endpoints)

### Fixed
- Kafka arrival time used for streaming window detection instead of
  historical CSV timestamps
- HBase row key redesigned for efficient `ALERT_` prefix scans
- Brute-force batch script: wrapped `severity` in `first()` aggregate
- Cassandra column names corrected (`last_seen`, `threat_score`)
- Adaptive scoring empty-reasons bug: added base-score explanation branch
  for IPs already scoring ≥ 95

---

## Version Policy

This project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html):

- **PATCH** `1.0.x` — bug fixes, configuration corrections, dependency updates
- **MINOR** `1.x.0` — new endpoints, new alert rules, new dashboard panels,
  new batch scripts (backwards-compatible additions)
- **MAJOR** `x.0.0` — breaking API changes, architectural overhaul
  (e.g., Kappa Architecture migration, auth layer)
