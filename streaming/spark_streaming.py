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
    sum as spark_sum, current_timestamp, lit 
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType
)
from cassandra.cluster import Cluster
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

def upsert_summary(ip, threat_score, attack_type, last_seen):
    """
    Read existing ip_threat_summary row for this IP,
    merge scores/types/counts, write back.
    Import is inside the function to avoid top-level serialization issues.
    """
    from cassandra.cluster import Cluster
 
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)
 
    select_stmt = session.prepare(
        f"SELECT threat_score, attack_types, total_alerts FROM {SUMMARY_TABLE} WHERE ip_source = ?"
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
 
    existing = session.execute(select_stmt, [ip]).one()
 
    if existing:
        merged_score  = max(threat_score, existing.threat_score or 0)
        merged_types  = list(set(existing.attack_types or []) | {attack_type})
        merged_alerts = (existing.total_alerts or 0) + 1
    else:
        merged_score  = threat_score
        merged_types  = [attack_type]
        merged_alerts = 1
 
    session.execute(upsert_stmt, [last_seen, merged_score, merged_types, merged_alerts, ip])
    cluster.shutdown()

# ── Combined foreachBatch: write to both tables ───────────────────────────
def make_batch_writer(query_name: str):
    """
    Returns a foreachBatch function that:
      1. Appends all rows to realtime_threats
      2. Upserts each row's IP into ip_threat_summary
    """
    def save_batch(batch_df, batch_id):
        n = batch_df.count()
        if n == 0:
            return
 
        print(f"[{query_name}] batch {batch_id} — {n} threat(s)")
 
        # Write to realtime_threats
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE) \
            .mode("append") \
            .save()

        # One Cassandra connection per batch, not per row
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)

        select_stmt = session.prepare(
            f"SELECT threat_score, attack_types, total_alerts FROM {SUMMARY_TABLE} WHERE ip_source = ?"
        )
        upsert_stmt = session.prepare(
            f"""UPDATE {SUMMARY_TABLE}
                SET last_seen = ?, threat_score = ?, attack_types = ?, total_alerts = ?
                WHERE ip_source = ?"""
        )

        rows = batch_df.select("ip_source", "threat_score", "attack_type", "last_seen").collect()
        for row in rows:
            try:
                existing = session.execute(select_stmt, [row["ip_source"]]).one()
                if existing:
                    merged_score  = max(row["threat_score"], existing.threat_score or 0)
                    merged_types  = list(set(existing.attack_types or []) | {row["attack_type"]})
                    merged_alerts = (existing.total_alerts or 0) + 1
                else:
                    merged_score, merged_types, merged_alerts = row["threat_score"], [row["attack_type"]], 1

                session.execute(upsert_stmt, [
                    row["last_seen"], merged_score, merged_types, merged_alerts, row["ip_source"]
                ])
            except Exception as e:
                print(f"  [summary] ERROR for {row['ip_source']}: {e}")

        cluster.shutdown()

    return save_batch

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

q1 = brute_force.writeStream \
    .outputMode("update") \
    .foreachBatch(make_batch_writer("brute-force-stream")) \
    .queryName("brute-force-stream") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/brute-force-stream") \
    .start()
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

q2 = signatures.writeStream \
    .outputMode("append") \
    .foreachBatch(make_batch_writer("signature-stream")) \
    .queryName("signature-stream") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/signature-stream") \
    .start()
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

q3 = volume_anomaly.writeStream \
    .outputMode("update") \
    .foreachBatch(make_batch_writer("volume-stream")) \
    .queryName("volume-stream") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/volume-stream") \
    .start()
print("Detection 3 started: volume anomaly")


# ── Wait for all queries to finish ────────────────────────────────────────
print("All 3 detection streams are running. Press Ctrl+C to stop.")
spark.streams.awaitAnyTermination()
