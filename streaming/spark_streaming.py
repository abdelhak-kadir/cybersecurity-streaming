"""
spark_streaming.py
------------------
Consumes logs from Kafka topic 'cybersecurity-logs' and applies
3 real-time detection rules:

  1. Brute-force    — 5+ blocked requests from same IP in 1 minute
  2. Attack signatures — known tool strings in user_agent / request_path
  3. Volume anomaly — >10 MB transferred by same IP in 10 seconds

Detected threats are written to Cassandra table: cybersecurity.realtime_threats
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count,
    sum as spark_sum, current_timestamp, lit ,collect_set, max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType
)
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time
# ── Config ────────────────────────────────────────────────────────────────
KAFKA_BROKER      = os.getenv("KAFKA_BROKER",   "kafka:29092")
KAFKA_TOPIC       = "cybersecurity-logs"
CASSANDRA_HOST    = os.getenv("CASSANDRA_HOST",  "cassandra")
CASSANDRA_KEYSPACE = "cybersecurity"
CASSANDRA_TABLE   = "realtime_threats"
SUMMARY_TABLE      = "ip_threat_summary"
CHECKPOINT_BASE    = "/tmp/spark-checkpoints"
 
TEN_MB = 10 * 1024 * 1024  # 10 485 760 bytes


# ── Spark session ─────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("CyberThreatDetection") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark session started. Waiting for messages from Kafka...")


# ── JSON schema (must match what kafka_producer.py sends) ─────────────────
log_schema = StructType([
    StructField("timestamp",         TimestampType(), True),
    StructField("source_ip",         StringType(),    True),
    StructField("dest_ip",           StringType(),    True),
    StructField("protocol",          StringType(),    True),
    StructField("action",            StringType(),    True),
    StructField("threat_label",      StringType(),    True),
    StructField("bytes_transferred", LongType(),      True),
    StructField("user_agent",        StringType(),    True),
    StructField("request_path",      StringType(),    True),
])


# ── Read stream from Kafka ────────────────────────────────────────────────
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse the JSON value into proper columns
logs = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), log_schema).alias("d")) \
    .select("d.*") \
    .withWatermark("timestamp", "30 seconds")


# ── Helper: write a stream to Cassandra ───────────────────────────────────
def write_to_cassandra(stream, query_name: str):
    """Write each micro-batch to the Cassandra realtime_threats table."""
    def save_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            return
        print(f"[{query_name}] Writing {batch_df.count()} threats (batch {batch_id})")
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE) \
            .mode("append") \
            .save()

    return stream.writeStream \
        .outputMode("update") \
        .foreachBatch(save_batch) \
        .queryName(query_name) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{query_name}") \
        .start()

# ── Helper: upsert cumulative summary in ip_threat_summary ───────────────
def _get_cassandra_session():
    """Return a Cassandra session (called inside each executor task)."""
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)
    return session

def upsert_summary(batch_df, batch_id):
    """
    For each (ip_source, attack_type) pair in the batch:
      - Read the existing row from ip_threat_summary
      - Merge: max threat_score, append new attack types, increment total_alerts
      - Write back with UPDATE … IF EXISTS / INSERT
    """
    if batch_df.count() == 0:
        return
 
    # Aggregate the micro-batch: one row per ip_source
    aggregated = (
        batch_df
        .groupBy("ip_source")
        .agg(
            spark_max("threat_score").alias("batch_max_score"),
            collect_set("attack_type").alias("batch_attack_types"),
            count("*").alias("batch_alert_count"),
            spark_max("last_seen").alias("batch_last_seen"),
        )
    ).collect()
 
    session = _get_cassandra_session()
 
    select_stmt = session.prepare(
        f"SELECT threat_score, attack_types, total_alerts "
        f"FROM {SUMMARY_TABLE} WHERE ip_source = ?"
    )
 
    upsert_stmt = session.prepare(
        f"""
        UPDATE {SUMMARY_TABLE}
        SET last_seen    = ?,
            threat_score = ?,
            attack_types = ?,
            total_alerts = ?
        WHERE ip_source = ?
        """
    )
 
    for row in aggregated:
        ip            = row["ip_source"]
        new_score     = row["batch_max_score"]
        new_types     = set(row["batch_attack_types"])
        new_alerts    = row["batch_alert_count"]
        new_last_seen = row["batch_last_seen"]
 
        # Read existing row (if any)
        existing = session.execute(select_stmt, [ip]).one()
 
        if existing:
            merged_score  = max(new_score, existing.threat_score or 0)
            merged_types  = list(set(existing.attack_types or []) | new_types)
            merged_alerts = (existing.total_alerts or 0) + new_alerts
        else:
            merged_score  = new_score
            merged_types  = list(new_types)
            merged_alerts = new_alerts
 
        session.execute(upsert_stmt, [
            new_last_seen,
            merged_score,
            merged_types,
            merged_alerts,
            ip,
        ])
        print(
            f"[summary] {ip} → score={merged_score}, "
            f"types={merged_types}, total_alerts={merged_alerts}"
        )
 
    session.cluster.shutdown()


# ── DETECTION 1: Brute-force ──────────────────────────────────────────────
# Rule: same IP gets blocked 5+ times within a 1-minute window
brute_force = logs \
    .filter(col("action") == "blocked") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("source_ip")
    ) \
    .agg(count("*").alias("blocked_count")) \
    .filter(col("blocked_count") >= 5) \
    .select(
        col("source_ip").alias("ip_source"),
        current_timestamp().alias("last_seen"),
        lit(80).alias("threat_score"),
        lit("brute-force").alias("attack_type"),
    )

q1 = write_to_cassandra(brute_force, "brute-force-stream")
print("Detection 1 started: brute-force")


# ── DETECTION 2: Attack signatures ────────────────────────────────────────
# Rule: user_agent or request_path contains a known attack tool/string
SIGNATURES = ["sqlmap", "nikto", "OR 1=1", "UNION SELECT", "<script>", "' OR '1'='1"]

sig_filter = col("user_agent").contains(SIGNATURES[0])
for sig in SIGNATURES[1:]:
    sig_filter = (
        sig_filter
        | col("user_agent").contains(sig)
        | col("request_path").contains(sig)
    )

signatures = logs \
    .filter(sig_filter) \
    .select(
        col("source_ip").alias("ip_source"),
        col("timestamp").alias("last_seen"),
        lit(95).alias("threat_score"),       # highest score — no false positives
        lit("attack-signature").alias("attack_type"),
    )

q2 = write_to_cassandra(signatures, "signature-stream")
print("Detection 2 started: attack signatures")


# ── DETECTION 3: Volume anomaly ───────────────────────────────────────────
# Rule: same IP transfers more than 10 MB in any 10-second window
volume_anomaly = logs \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("source_ip")
    ) \
    .agg(spark_sum("bytes_transferred").alias("total_bytes")) \
    .filter(col("total_bytes") >= TEN_MB) \
    .select(
        col("source_ip").alias("ip_source"),
        current_timestamp().alias("last_seen"),
        lit(70).alias("threat_score"),
        lit("data-exfiltration").alias("attack_type"),
    )

q3 = write_to_cassandra(volume_anomaly, "volume-stream")
print("Detection 3 started: volume anomaly")

# ── DETECTION 4 (summary): cumulative ip_threat_summary writer ────────────
# Union all 3 alert streams, then upsert into ip_threat_summary each batch.
all_threats = brute_force.union(signatures).union(volume_anomaly)
 
q4 = all_threats.writeStream \
    .outputMode("update") \
    .foreachBatch(upsert_summary) \
    .queryName("ip-summary-stream") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/summary") \
    .start()
 
print("Detection 4 started: cumulative ip_threat_summary")

# ── Wait for all queries to finish ────────────────────────────────────────
print("All 4 detection streams are running. Press Ctrl+C to stop.")
spark.streams.awaitAnyTermination()
