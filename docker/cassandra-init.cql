-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS cybersecurity
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

-- Use keyspace
USE cybersecurity;

-- Main threats table (written by spark_streaming.py)
CREATE TABLE IF NOT EXISTS realtime_threats (
    ip_source    TEXT,
    last_seen    TIMESTAMP,
    threat_score INT,
    attack_type  TEXT,
    PRIMARY KEY (ip_source, last_seen)
) WITH CLUSTERING ORDER BY (last_seen DESC);

-- Optional: summary table (one row per IP, updated on each detection)
CREATE TABLE IF NOT EXISTS ip_threat_summary (
    ip_source     TEXT PRIMARY KEY,
    last_seen     TIMESTAMP,
    threat_score  INT,
    attack_types  LIST<TEXT>,
    total_alerts  INT
);
