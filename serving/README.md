# Serving Layer — CyberSecurity Lambda Architecture

FastAPI application that merges **batch views** (HBase) and **speed views** (Cassandra)
into a single unified REST API.

## Architecture

```
HBase (batch views)          Cassandra (speed views)
  ip_reputation          +     realtime_threats
  attack_patterns              ip_threat_summary
  threat_timeline
        │                            │
        └──────────┬─────────────────┘
                   ▼
           FastAPI serving layer  :8000
                   │
          ┌────────┴──────────┐
          │  REST API clients  │
          │  (curl / frontend) │
          └────────────────────┘
```

## Quick start

### 1. Add the serving service to docker-compose.yml

Paste the contents of `docker-compose.serving.yml` into your `docker-compose.yml`
under the `services:` key (before the `volumes:` section).

### 2. Add Makefile targets

Append the contents of `Makefile.serving` to your root `Makefile`.

### 3. Start the serving layer

```bash
# Start everything (if not already running)
make up

# Then start the serving layer
make serving-up

# Watch logs
make serving-logs
```

### 4. Open interactive API docs

```
http://localhost:8000/docs
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Connectivity check for Cassandra + HBase |
| GET | `/api/ip/{ip}` | IP reputation — batch + realtime merged |
| GET | `/api/threats/live` | Live threat feed (Cassandra, last N min) |
| GET | `/api/stats/top-ips` | Top malicious IPs from batch layer |
| GET | `/api/stats/attack-patterns` | SQLi/XSS/port-scan from HBase |
| GET | `/api/stats/threat-timeline` | Daily threat counts over time |
| GET | `/api/stats/threat-volume` | Bytes transferred by threat label |

## Query parameters

### `/api/threats/live`
| Param | Default | Description |
|-------|---------|-------------|
| `minutes` | 60 | Lookback window (1–1440) |
| `limit` | 100 | Max rows (1–1000) |
| `attack_type` | — | Filter: `brute-force`, `attack-signature`, `data-exfiltration` |
| `min_score` | 0 | Minimum threat score (0–100) |

### `/api/stats/top-ips`
| Param | Default | Description |
|-------|---------|-------------|
| `limit` | 10 | Max IPs to return (1–50) |

### `/api/stats/attack-patterns`
| Param | Default | Description |
|-------|---------|-------------|
| `attack_type` | — | Filter: `SQLi`, `XSS`, `port_scan` |
| `limit` | 50 | Max patterns (1–200) |

### `/api/stats/threat-timeline`
| Param | Default | Description |
|-------|---------|-------------|
| `days` | 30 | Lookback window in days (1–365) |
| `threat_label` | — | Filter: `malicious`, `suspicious` |

## Merge logic for `/api/ip/{ip}`

The IP reputation endpoint combines data from both layers:

```
merged_reputation_score = max(batch_reputation_score, realtime_threat_score)
attack_types            = union(batch_attack_types, realtime_attack_types)
total_realtime_alerts   = from ip_threat_summary (Cassandra)
nb_batch_attacks        = from ip_reputation (HBase)
recent_events           = last 10 events from realtime_threats (Cassandra)
```

## Smoke tests

```bash
# Run all tests
make serving-test

# Test a specific IP
make serving-ip IP=192.168.1.10

# Or with curl directly
curl "http://localhost:8000/api/ip/192.168.1.10"
curl "http://localhost:8000/api/threats/live?minutes=30&min_score=70"
curl "http://localhost:8000/api/stats/attack-patterns?attack_type=SQLi"
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CASSANDRA_HOST` | `cassandra` | Cassandra hostname |
| `CASSANDRA_PORT` | `9042` | Cassandra port |
| `HBASE_HOST` | `hbase` | HBase Thrift hostname |
| `HBASE_PORT` | `9090` | HBase Thrift port |