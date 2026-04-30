-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS cybersecurity
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

-- Use keyspace
USE cybersecurity;

-- Main threats table (written by spark_streaming.py)
-- TTL of 86400s (24h) ensures only active threats are retained, per spec.
-- Primary key is idempotent: re-writing the same (ip, timestamp) just upserts.
CREATE TABLE IF NOT EXISTS realtime_threats (
    ip_source    TEXT,
    last_seen    TIMESTAMP,
    threat_score INT,
    attack_type  TEXT,
    PRIMARY KEY (ip_source, last_seen)
) WITH CLUSTERING ORDER BY (last_seen DESC)
  AND default_time_to_live = 86400;

-- Summary table: one row per IP, updated atomically by spark_streaming.py.
-- attack_types is SET<TEXT> so additions are idempotent and require no read.
-- threat_score and total_alerts are NOT stored here — they are derived by the
-- serving layer from realtime_threats (bounded by TTL, so always last-24h data).
CREATE TABLE IF NOT EXISTS ip_threat_summary (
    ip_source    TEXT PRIMARY KEY,
    last_seen    TIMESTAMP,
    attack_types SET<TEXT>
);

-- Correlated multi-step attacks detected by spark_streaming.py.
-- Stores the stages behind the final "multi-step-attack" alert for dashboard drill-down.
CREATE TABLE IF NOT EXISTS correlated_attacks (
    ip_source    TEXT,
    last_seen    TIMESTAMP,
    first_seen   TIMESTAMP,
    stages       SET<TEXT>,
    threat_score INT,
    PRIMARY KEY (ip_source, last_seen)
) WITH CLUSTERING ORDER BY (last_seen DESC)
  AND default_time_to_live = 86400;
