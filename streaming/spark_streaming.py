"""
spark_streaming.py
------------------
Consumes logs from Kafka topic 'cybersecurity-logs' and applies
4 real-time detection rules:

  1. Brute-force    — 5+ blocked requests from same IP in 1 minute
  2. Attack signatures — known tool strings in user_agent / request_path
  3. Volume anomaly — >10 MB transferred by same IP in 10 seconds
  4. Port scan      — Nmap/Masscan-style reconnaissance from same IP

Detected threats are written to Cassandra table: cybersecurity.realtime_threats
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count,
    sum as spark_sum, current_timestamp, lit, lower
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType
)
import time
# ── Config ────────────────────────────────────────────────────────────────
KAFKA_BROKER      = os.getenv("KAFKA_BROKER",   "kafka:29092")
KAFKA_TOPIC       = "cybersecurity-logs"
CASSANDRA_HOST    = os.getenv("CASSANDRA_HOST",  "cassandra")
CASSANDRA_KEYSPACE = "cybersecurity"
CASSANDRA_TABLE   = "realtime_threats"
SUMMARY_TABLE      = "ip_threat_summary"
CORRELATION_TABLE  = "correlated_attacks"
CHECKPOINT_BASE    = os.getenv("CHECKPOINT_BASE", "/tmp/spark-checkpoints-v3")
 
TEN_MB = 10 * 1024 * 1024  # 10 485 760 bytes
MAX_THREATS_PER_BATCH = int(os.getenv("MAX_THREATS_PER_BATCH", "2000"))
CORRELATION_WINDOW_SECONDS = int(os.getenv("CORRELATION_WINDOW_SECONDS", "600"))
CORRELATION_SCORE = 100
RECON_STAGE = "port-scan"
EXPLOIT_STAGES = {"attack-signature", "brute-force", "data-exfiltration"}


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
    .option("startingOffsets", "earliest") \
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

# ── Combined foreachBatch: write to both tables ───────────────────────────
def make_batch_writer(query_name: str):
    """
    Returns a foreachBatch function that:
      1. Appends all rows to realtime_threats (TTL 24h enforced by schema)
      2. Atomically updates ip_threat_summary using SET addition (no read needed)
      3. Detects multi-step attacks for IPs touched by this micro-batch
    """
    def save_batch(batch_df, batch_id):
        from cassandra.cluster import Cluster
        from datetime import datetime, timedelta
        n = batch_df.count()
        if n == 0:
            return
        if n > MAX_THREATS_PER_BATCH:
            print(f"[{query_name}] batch {batch_id} — capping {n} threat(s) to {MAX_THREATS_PER_BATCH}")
            batch_df = batch_df.orderBy(col("last_seen").desc()).limit(MAX_THREATS_PER_BATCH)
            n = batch_df.count()

        print(f"[{query_name}] batch {batch_id} — {n} threat(s)")

        # Write to realtime_threats
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE) \
            .mode("append") \
            .save()

        # One Cassandra connection per batch
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)

        # Atomic SET addition — no read needed, idempotent on retry.
        # threat_score is intentionally omitted: the serving layer derives the
        # max score directly from realtime_threats (bounded by 24h TTL) so there
        # is no risk of a stale low-score overwriting a prior high-score entry.
        update_summary = session.prepare(
            f"""UPDATE {SUMMARY_TABLE}
                SET last_seen    = ?,
                    attack_types = attack_types + ?
                WHERE ip_source  = ?"""
        )
        select_recent = session.prepare(
            f"""SELECT last_seen, attack_type
                FROM {CASSANDRA_TABLE}
                WHERE ip_source = ?
                LIMIT 100"""
        )
        insert_correlated = session.prepare(
            f"""INSERT INTO {CORRELATION_TABLE}
                (ip_source, last_seen, first_seen, stages, threat_score)
                VALUES (?, ?, ?, ?, ?)"""
        )
        insert_realtime = session.prepare(
            f"""INSERT INTO {CASSANDRA_TABLE}
                (ip_source, last_seen, threat_score, attack_type)
                VALUES (?, ?, ?, ?)"""
        )

        rows = batch_df.select("ip_source", "attack_type", "last_seen").collect()
        for row in rows:
            try:
                session.execute(update_summary, [
                    row["last_seen"], {row["attack_type"]}, row["ip_source"]
                ])
            except Exception as e:
                print(f"  [summary] ERROR for {row['ip_source']}: {e}")

        for ip in {row["ip_source"] for row in rows}:
            try:
                recent_rows = list(session.execute(select_recent, [ip]))
                cutoff = datetime.utcnow() - timedelta(seconds=CORRELATION_WINDOW_SECONDS)
                recent_rows = [
                    r for r in recent_rows
                    if r.last_seen and r.last_seen.replace(tzinfo=None) >= cutoff
                ]
                stages = {r.attack_type for r in recent_rows if r.attack_type}
                if RECON_STAGE not in stages or not (stages & EXPLOIT_STAGES):
                    continue
                event_times = [r.last_seen for r in recent_rows if r.last_seen]
                first_seen = min(event_times)
                last_seen = max(event_times)
                correlated_stages = stages - {"multi-step-attack"}
                session.execute(insert_correlated, [
                    ip, last_seen, first_seen, correlated_stages, CORRELATION_SCORE
                ])
                session.execute(insert_realtime, [
                    ip, last_seen, CORRELATION_SCORE, "multi-step-attack"
                ])
                session.execute(update_summary, [
                    last_seen, {"multi-step-attack"}, ip
                ])
                print(f"  [correlation] {ip}: {sorted(correlated_stages)}")
            except Exception as e:
                print(f"  [correlation] ERROR for {ip}: {e}")

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
        current_timestamp().alias("last_seen"),
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


# ── DETECTION 4: Port scan / reconnaissance ──────────────────────────────
# Dataset has no destination port column, so live reconnaissance is inferred
# from scanner user-agents (Nmap/Masscan) and repeated blocked network probes.
scan_filter = (
    lower(col("user_agent")).contains("nmap")
    | lower(col("user_agent")).contains("masscan")
    | (
        (col("action") == "blocked")
        & col("protocol").isin("TCP", "UDP", "ICMP")
    )
)

port_scan = logs \
    .filter(scan_filter) \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("source_ip")
    ) \
    .agg(count("*").alias("probe_count")) \
    .filter(col("probe_count") >= 3) \
    .select(
        col("source_ip").alias("ip_source"),
        current_timestamp().alias("last_seen"),
        lit(75).alias("threat_score"),
        lit("port-scan").alias("attack_type"),
    )

q4 = port_scan.writeStream \
    .outputMode("update") \
    .foreachBatch(make_batch_writer("port-scan-stream")) \
    .queryName("port-scan-stream") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/port-scan-stream") \
    .start()
print("Detection 4 started: port scan")


# ── Wait for all queries to finish ────────────────────────────────────────
print("All 4 detection streams are running. Press Ctrl+C to stop.")
spark.streams.awaitAnyTermination()
