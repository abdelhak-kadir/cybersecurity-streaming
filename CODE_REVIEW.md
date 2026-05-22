# Code Review Report — Cybersecurity Streaming Platform

**Date**: 2026-05-15
**Scope**: Full codebase review across all layers (streaming, serving, batch, ML, infrastructure)
**Architecture**: Lambda Architecture (Kafka + Spark Streaming + Cassandra | HDFS + Spark Batch + HBase | FastAPI + Grafana)

---

## Executive Summary

The project implements a complete Lambda Architecture for cybersecurity threat detection with 5 detection engines (4 rule-based + 1 ML), a 15-endpoint REST API, and a 10-panel Grafana dashboard. The architecture is well-designed and the separation of concerns across layers is clean.

However, the review identified **52 issues** across all layers:

| Severity | Count |
|----------|-------|
| Critical | 11    |
| Major    | 27    |
| Minor    | 14    |

**Top risks**: driver-side `collect()` on large datasets, missing authentication, production debug flags, and silent error swallowing across multiple components.

---

## 1. Streaming Layer

### spark_streaming.py

| # | Severity | Issue | Line(s) |
|---|----------|-------|---------|
| 1 | **CRITICAL** | `batch_df.collect()` pulls entire batch into driver memory. High-volume streams will cause OOM. Should use distributed writes. | 218 |
| 2 | **CRITICAL** | `min(event_times)` called without checking list is non-empty. Raises `ValueError` on empty list, crashing the batch processor. | 229, 251 |
| 3 | **MAJOR** | Thread-unsafe global Cassandra session. Concurrent executors may create race conditions or duplicate sessions via unguarded `global _cassandra_session`. | 149-154 |
| 4 | **MAJOR** | Unsafe ML pipeline assumption — `_ml_model.stages[0].labels` assumes stage 0 is always a StringIndexer without validation. | 81-82 |
| 5 | **MAJOR** | Nullable timestamp cast to `TimestampType` without null handling. Missing timestamps silently become NULL, breaking downstream filtering. | 39-40 |
| 6 | **MAJOR** | Port scan filter `probe_count >= 1` is always true after `groupBy` — condition is meaningless. Should be a higher threshold. | 369 |
| 7 | **MAJOR** | String interpolation in CQL table names. Not immediately exploitable but violates defense-in-depth. | 193-216 |
| 8 | **MINOR** | `failOnDataLoss=false` silently drops Kafka messages on broker failure without alerting. | 109 |
| 9 | **MINOR** | 30-second watermark on 10-second windows risks late data double-counting. | 118 |
| 10 | **MINOR** | Exception handlers print errors but don't log or propagate. Masks systemic failures. | 85, 224, 264 |

### kafka_producer.py

| # | Severity | Issue | Line(s) |
|---|----------|-------|---------|
| 11 | **MAJOR** | No `acks` configuration. Defaults to `acks=1`, risking message loss on broker failure. Should use `acks='all'`. | 29-33 |
| 12 | **MAJOR** | Unbatched sends — each message is an individual network round-trip. Add `batch_size` and `linger_ms`. | 73-77 |
| 13 | **MAJOR** | `producer.send()` return value never checked. Failed messages silently dropped. | 73-77 |
| 14 | **MAJOR** | `int(row.get("bytes_transferred", 0) or 0)` silently coerces empty strings to 0, corrupting data. | 51 |
| 15 | **MINOR** | `producer.flush()` blocks indefinitely with no timeout. | 85 |
| 16 | **MINOR** | Demo mode `while True` loop has no exit condition except Ctrl+C. | 104 |

### cassandra_setup.py

| # | Severity | Issue | Line(s) |
|---|----------|-------|---------|
| 17 | **MAJOR** | `replication_factor = 1` means any node failure causes total data loss. | 32 |
| 18 | **MINOR** | Cluster object created in `wait_for_cassandra` is never closed — resource leak. | 18 |

---

## 2. Serving Layer

### main.py

| # | Severity | Issue | Line(s) |
|---|----------|-------|---------|
| 19 | **CRITICAL** | No IP input validation on `/api/ip/{ip}`. Raw string passed to Cassandra query. Should validate with `ipaddress.ip_address()`. | 165-166 |
| 20 | **CRITICAL** | No CORS configuration. Any origin can access all endpoints. | — |
| 21 | **CRITICAL** | External geolocation API called over plain HTTP (`http://ip-api.com/batch`), not HTTPS. | 85 |
| 22 | **MAJOR** | Global `_geo_cache` dict mutated without thread synchronization. Race condition under concurrent requests. | 39-79 |
| 23 | **MAJOR** | `/metrics` endpoint recalculates aggregates from 5000 rows on every Prometheus scrape (default 15s). Should cache within scrape interval. | 137-147 |
| 24 | **MAJOR** | No rate limiting on any endpoint. | — |

### db/cassandra_client.py

| # | Severity | Issue | Line(s) |
|---|----------|-------|---------|
| 25 | **MAJOR** | `ALLOW FILTERING` forces full table scan. Acceptable with 24h TTL but should be documented as known risk. | 58-61, 77 |
| 26 | **MAJOR** | No connection pool configuration. Default settings may exhaust connections under load. | 13 |
| 27 | **MINOR** | No explicit query timeout. Queries can hang indefinitely if Cassandra is unresponsive. | 13 |

### db/hbase_client.py

| # | Severity | Issue | Line(s) |
|---|----------|-------|---------|
| 28 | **MAJOR** | Silent failure on batch reads — returns empty dict `{}`. Callers can't distinguish "no data" from "HBase down". | 53-63 |
| 29 | **MAJOR** | Over-fetching anti-pattern: scans `limit * 5` rows then sorts in Python. Inefficient for large limits. | 82-83 |
| 30 | **MINOR** | `_decode_row` assumes all HBase values are UTF-8. Binary data would raise. | 29 |

### Dockerfile & requirements.txt

| # | Severity | Issue | Line(s) |
|---|----------|-------|---------|
| 31 | **CRITICAL** | `--reload` flag enabled in production Dockerfile CMD. Auto-restarts break connections. Should use `--workers 4` instead. | Dockerfile:17 |
| 32 | **MAJOR** | Python 3.9 base image reached EOL October 2025. Should upgrade to 3.12. | Dockerfile:1 |
| 33 | **MAJOR** | `fastapi==0.111.0` is outdated (2024). Missing security patches. | requirements.txt:1 |

---

## 3. Batch & ML Layer

### Batch Scripts

| # | Severity | Issue | Line(s) |
|---|----------|-------|---------|
| 34 | **CRITICAL** | `07_hbase_storage.py`: `.collect()` on full 6M-row DataFrames followed by row-by-row HBase puts. O(n) iteration in driver. Should use bulk writes. | 57-67, 75-90, 113-127 |
| 35 | **CRITICAL** | `03_port_scans.py`: Fundamental design flaw — uses `dest_ip` as proxy for port scan detection without actual port data. | 23-47 |
| 36 | **MAJOR** | `04_Attack_Evolution.py`: Regex patterns too broad — `r"'|--|#"` false-positives on benign strings ("it's", comments). XSS pattern `r"<|>"` matches any HTML. | 23-25 |
| 37 | **MAJOR** | `04_Attack_Evolution.py`: Overwrites `threat_label` with pattern matching, losing ground-truth label. Data leakage risk. | 31-34 |
| 38 | **MAJOR** | `01_load_hdfs.py`: `inferSchema=True` on unknown CSV can cause type mismatches. Should use explicit schema. | 18 |

### ML Pipeline

| # | Severity | Issue | Line(s) |
|---|----------|-------|---------|
| 39 | **CRITICAL** | `ml/features.py`: `log1p()` on nullable `bytes_transferred` produces null feature, breaking VectorAssembler. | 49 |
| 40 | **CRITICAL** | `ml/storage.py`: Same `.collect()` + row-by-row HBase put pattern. Will timeout on >100K predictions. | 89-96 |
| 41 | **MAJOR** | `08_ml_threat_classification.py`: No stratified sampling in train/test split. Class imbalance (92% benign, 6% suspicious, 2% malicious) biases evaluation. | 63 |
| 42 | **MAJOR** | `ml/features.py`: Regex patterns duplicated from batch layer without synchronization. Pattern drift risk. | 14-16 |
| 43 | **MAJOR** | `ml/features.py`: `clean()` silently drops rows with no logging of dropout rate. | 73 |
| 44 | **MAJOR** | `ml/models.py`: Comment says "100 arbres" but code uses `numTrees=30`. Documentation out of sync with implementation. | 39, 84 |

---

## 4. Infrastructure & Deployment

### Terraform (terraform/main.tf)

| # | Severity | Issue | Line(s) |
|---|----------|-------|---------|
| 45 | **CRITICAL** | SSH port 22 open to `0.0.0.0/0` on all 3 droplets. Should restrict to operator IP or VPN. | 67, 105, 138 |
| 46 | **MAJOR** | Spark Master UI (8080), HBase admin (16010) exposed in docker-compose. Verify terraform firewalls block them externally. | docker-compose.yml |

### Docker

| # | Severity | Issue | Line(s) |
|---|----------|-------|---------|
| 47 | **CRITICAL** | Default Grafana admin password is "admin" in compose files and .env.example. | docker-compose.yml:324 |
| 48 | **MAJOR** | All containers run as root. No user isolation in any Dockerfile. | all Dockerfiles |
| 49 | **MINOR** | Base images not pinned to patch version (e.g., `python:3.9-bullseye` vs `python:3.9.18-bullseye`). | all Dockerfiles |

### Ansible

| # | Severity | Issue | Line(s) |
|---|----------|-------|---------|
| 50 | **MINOR** | `replace` module for IP injection is fragile. Should use Ansible templating with Jinja2. | deploy_infra.yml:23-27 |
| 51 | **MINOR** | `docker compose run` commands re-execute on every playbook run. Not idempotent. | deploy_infra.yml:56-60 |
| 52 | **MINOR** | Hardcoded domain `grafana.project-demo.tech` in verify.yml. Should use variable. | verify.yml:74 |

---

## 5. Cross-Cutting Concerns

### Error Handling
Every layer silently swallows exceptions with `print()` instead of structured logging. There is no centralized error tracking (no Sentry, no structured logs, no alerting on failures). Failures in HBase, Cassandra, or Kafka are hidden behind empty responses.

**Recommendation**: Replace `print()` with Python `logging` module. Add error counters to Prometheus metrics. Distinguish "no data" from "backend down" in API responses.

### Security
- No authentication or authorization on any API endpoint
- No TLS between internal services (Kafka, Cassandra, HBase, Spark)
- Grafana default credentials
- SSH open to the internet
- External API called over HTTP

**Recommendation**: Add API key or JWT authentication. Enable mTLS for inter-service communication. Restrict SSH to bastion/VPN.

### Performance
The most severe performance issue appears in 3 separate files: calling `.collect()` on multi-million-row DataFrames and then iterating row-by-row in the driver to write to HBase. This is an anti-pattern that defeats the purpose of distributed computing.

**Recommendation**: Use Spark's native HBase connector (`saveAsHadoopDataset`) or HBase bulk load for batch writes. For streaming, use `foreachBatch` with connection pooling.

### Testing
No test files exist anywhere in the project. No unit tests, no integration tests, no API contract tests.

**Recommendation**: Add pytest for serving layer (mock Cassandra/HBase). Add Spark testing with `pyspark.testing`. Add API contract tests with httpx test client.

---

## Priority Action Items

### P0 — Fix Before Production
1. Remove `--reload` from serving Dockerfile
2. Restrict SSH to operator IP in terraform
3. Change Grafana default password
4. Add IP input validation on `/api/ip/{ip}`
5. Add CORS middleware configuration
6. Guard `min(event_times)` against empty list in streaming

### P1 — Fix Before Scale
7. Replace `.collect()` + row-by-row HBase writes with bulk operations (3 files)
8. Add Kafka producer `acks='all'` and batching
9. Add connection pooling config for Cassandra client
10. Fix thread-unsafe geo cache with `threading.Lock()`
11. Upgrade Python 3.9 to 3.12 and FastAPI to latest

### P2 — Improve Quality
12. Add structured logging across all layers
13. Add stratified sampling for ML train/test split
14. Centralize attack regex patterns (shared module)
15. Add healthchecks for all Docker services
16. Add test suite (pytest + Spark testing)

---

*Generated by comprehensive code review across 35+ source files.*
