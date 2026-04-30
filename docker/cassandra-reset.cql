-- cassandra-reset.cql
-- DESTRUCTIVE: drops and recreates all cybersecurity tables.
-- Run manually only — NOT part of the normal startup path.
--
-- Usage (while the stack is running):
--   docker exec -i cassandra cqlsh < docker/cassandra-reset.cql
-- Or via Makefile:
--   make reset-schema

USE cybersecurity;

DROP TABLE IF EXISTS realtime_threats;
DROP TABLE IF EXISTS ip_threat_summary;
DROP TABLE IF EXISTS ip_alert_counters;

-- Recreate with current schema
CREATE TABLE IF NOT EXISTS realtime_threats (
    ip_source    TEXT,
    last_seen    TIMESTAMP,
    threat_score INT,
    attack_type  TEXT,
    PRIMARY KEY (ip_source, last_seen)
) WITH CLUSTERING ORDER BY (last_seen DESC)
  AND default_time_to_live = 86400;

CREATE TABLE IF NOT EXISTS ip_threat_summary (
    ip_source    TEXT PRIMARY KEY,
    last_seen    TIMESTAMP,
    attack_types SET<TEXT>
);
