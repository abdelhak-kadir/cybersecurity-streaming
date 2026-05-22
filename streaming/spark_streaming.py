"""
spark_streaming.py
------------------
Consumes logs from Kafka topic 'cybersecurity-logs' and applies
5 real-time detection rules:

  1. Brute-force       — 5+ blocked requests from same IP in 1 minute
  2. Attack signatures — known tool strings in user_agent / request_path
  3. Volume anomaly    — >10 MB transferred by same IP in 10 seconds
  4. Port scan         — Nmap/Masscan-style reconnaissance from same IP
  5. ML classification — Random Forest model applied to every raw event;
                         writes ml-malicious (score 85) or ml-suspicious
                         (score 60) when model predicts non-benign class.
                         Requires the model to be trained first via
                         make ml-train. Starts gracefully if model is absent.

Detected threats are written to Cassandra table: cybersecurity.realtime_threats
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, from_json, window, count,
    sum as spark_sum, current_timestamp, lit, lower,
    log1p, length, hour, dayofweek, when,
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
EXPLOIT_STAGES = {"attack-signature", "brute-force", "data-exfiltration", "ml-malicious"}

ML_MODEL_PATH = os.getenv("ML_MODEL_PATH", "hdfs://namenode:9000/models/threat_classifier")
ML_MALICIOUS_SCORE = 85   # threat_score assigned to ML-detected malicious events
ML_SUSPICIOUS_SCORE = 60  # threat_score assigned to ML-detected suspicious events

# Patterns mirrored from ml/scripts/ml/features.py (inlined to avoid --py-files)
_SQLI_PATTERN  = r"(union|select|insert|drop|sleep|benchmark|--|')"
_XSS_PATTERN   = r"(script|alert|iframe|javascript|eval|onerror)"
_TOOLS_PATTERN = r"(sqlmap|nikto|nmap|dirbuster|masscan)"
_FEATURE_COLS  = [
    "hour_of_day", "day_of_week", "bytes_log", "path_length", "agent_length",
    "is_blocked", "has_sqli", "has_xss", "has_tool_ua",
    "is_tcp", "is_http", "is_ssh", "is_udp",
]


# ── Spark session ─────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("CyberThreatDetection") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark session started. Waiting for messages from Kafka...")

# ── Load ML model (optional — graceful if not yet trained) ───────────────
_ml_model  = None
_ml_labels = None
try:
    from pyspark.ml import PipelineModel
    _ml_model  = PipelineModel.load(ML_MODEL_PATH)
    _ml_labels = list(_ml_model.stages[0].labels)   # StringIndexer learned order
    print(f"[ML] Model loaded from {ML_MODEL_PATH}")
    print(f"[ML] Label order: {_ml_labels}")
except Exception as _e:
    print(f"[ML] Model not available — ML detection disabled. ({_e})")


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
    .withColumn("arrival_time", current_timestamp()) \
    .withWatermark("arrival_time", "30 seconds")


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

# ── Executor-local Cassandra session singleton ────────────────────────────
# A new Cluster object is created only once per Spark executor process,
# avoiding the connection-per-batch overhead of the previous implementation.
_cassandra_cluster = None
_cassandra_session = None


def _get_session():
    global _cassandra_cluster, _cassandra_session
    if _cassandra_session is None:
        from cassandra.cluster import Cluster
        _cassandra_cluster = Cluster([CASSANDRA_HOST])
        _cassandra_session = _cassandra_cluster.connect(CASSANDRA_KEYSPACE)
    return _cassandra_session


# ── Combined foreachBatch: write to both tables ───────────────────────────
def make_batch_writer(query_name: str):
    """
    Returns a foreachBatch function that:
      1. Appends all rows to realtime_threats (TTL 24h enforced by schema)
      2. Atomically updates ip_threat_summary using SET addition (no read needed)
      3. Detects multi-step attacks for IPs touched by this micro-batch using
         concurrent Cassandra queries (one per unique IP, issued in parallel)
    """
    def save_batch(batch_df, batch_id):
        from datetime import datetime, timedelta
        from cassandra.concurrent import execute_concurrent_with_args

        n = batch_df.count()
        if n == 0:
            return
        if n > MAX_THREATS_PER_BATCH:
            print(f"[{query_name}] batch {batch_id} — capping {n} threat(s) to {MAX_THREATS_PER_BATCH}")
            batch_df = batch_df.orderBy(col("last_seen").desc()).limit(MAX_THREATS_PER_BATCH)
            n = MAX_THREATS_PER_BATCH  # avoid a second full scan just for the log line

        print(f"[{query_name}] batch {batch_id} — {n} threat(s)")

        # Write to realtime_threats
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE) \
            .mode("append") \
            .save()

        session = _get_session()

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
        # Time filter pushed into CQL — avoids fetching data outside the
        # correlation window and eliminates the Python-side post-filter.
        select_recent = session.prepare(
            f"""SELECT last_seen, attack_type
                FROM {CASSANDRA_TABLE}
                WHERE ip_source = ? AND last_seen >= ?
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

        # Fetch recent history for all unique IPs concurrently rather than
        # serially — avoids N round-trips blocking the micro-batch.
        cutoff = datetime.utcnow() - timedelta(seconds=CORRELATION_WINDOW_SECONDS)
        unique_ips = list({row["ip_source"] for row in rows})
        concurrent_results = execute_concurrent_with_args(
            session,
            select_recent,
            [(ip, cutoff) for ip in unique_ips],
            concurrency=20,
            raise_on_first_error=False,
        )
        ip_recent: dict = {}
        for i, (success, result) in enumerate(concurrent_results):
            if success:
                ip_recent[unique_ips[i]] = list(result)
            else:
                print(f"  [correlation] SELECT failed for {unique_ips[i]}: {result}")

        for ip, recent_rows in ip_recent.items():
            try:
                stages = {r.attack_type for r in recent_rows if r.attack_type}
                if RECON_STAGE not in stages or not (stages & EXPLOIT_STAGES):
                    continue
                event_times = [r.last_seen for r in recent_rows if r.last_seen]
                if not event_times:
                    continue
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

    return save_batch

# ── DETECTION 1: Brute-force ──────────────────────────────────────────────
# Rule: same IP gets blocked 5+ times within a 1-minute window
brute_force = logs \
    .filter(col("action") == "blocked") \
    .groupBy(
        window(col("arrival_time"), "1 minute"),
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
        window(col("arrival_time"), "10 seconds"),
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
# Trigger only on well-known scanner tool fingerprints in the user-agent.
# The previous broad catch of ALL blocked TCP/UDP/ICMP traffic was flagging
# normal firewall denies as port scans, producing false positives for every IP
# with as few as 3 blocked packets in a minute.
scan_filter = (
    lower(col("user_agent")).contains("nmap")
    | lower(col("user_agent")).contains("masscan")
    | lower(col("user_agent")).contains("zmap")
    | lower(col("user_agent")).contains("zgrab")
)

port_scan = logs \
    .filter(scan_filter) \
    .groupBy(
        window(col("arrival_time"), "1 minute"),
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


# ── DETECTION 5: ML-based classification ─────────────────────────────────
# Applies the trained Random Forest pipeline to every raw event.
# Only non-benign predictions (malicious / suspicious) are written to
# Cassandra so they surface in the live threat feed alongside rule-based
# detections.  The model is loaded once at startup; if it is not present
# (e.g. training has not run yet) this block is skipped silently.
if _ml_model is not None:
    def _build_features(df):
        """Replicate ml/scripts/ml/features.py inline (no --py-files needed)."""
        return (
            df
            .withColumn("hour_of_day",  hour(col("timestamp")))
            .withColumn("day_of_week",  dayofweek(col("timestamp")))
            .withColumn("bytes_log",    log1p(coalesce(col("bytes_transferred"), lit(0)).cast("double")))
            .withColumn("path_length",  length(col("request_path")).cast("double"))
            .withColumn("agent_length", length(col("user_agent")).cast("double"))
            .withColumn("is_blocked",   when(col("action") == "blocked", 1.0).otherwise(0.0))
            .withColumn("has_sqli",     when(lower(col("request_path")).rlike(_SQLI_PATTERN), 1.0).otherwise(0.0))
            .withColumn("has_xss",      when(lower(col("request_path")).rlike(_XSS_PATTERN),  1.0).otherwise(0.0))
            .withColumn("has_tool_ua",  when(lower(col("user_agent")).rlike(_TOOLS_PATTERN),  1.0).otherwise(0.0))
            .withColumn("is_tcp",       when(col("protocol") == "TCP",  1.0).otherwise(0.0))
            .withColumn("is_http",      when(col("protocol") == "HTTP", 1.0).otherwise(0.0))
            .withColumn("is_ssh",       when(col("protocol") == "SSH",  1.0).otherwise(0.0))
            .withColumn("is_udp",       when(col("protocol") == "UDP",  1.0).otherwise(0.0))
            # Dummy label column required by the StringIndexer stage at inference time.
            # handleInvalid="keep" maps unknown values to an extra index that does
            # not affect the RandomForest prediction column.
            .withColumn("threat_label", lit("unknown"))
        )

    # Capture for closure (avoids serialising the whole module namespace)
    _captured_model  = _ml_model
    _captured_labels = _ml_labels

    def _ml_save_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            return

        # Build features and run inference
        featured     = _build_features(batch_df)
        predictions  = _captured_model.transform(featured)
        labels       = _captured_labels

        # Map numeric prediction index → label string using when/otherwise
        # (avoids UDF serialisation issues in foreachBatch)
        label_expr = lit("unknown")
        for idx, lbl in enumerate(labels):
            label_expr = when(col("prediction") == float(idx), lbl).otherwise(label_expr)

        # Score mapping: malicious → 85, suspicious → 60, anything else → skip
        score_expr = (
            when(label_expr == "malicious",  ML_MALICIOUS_SCORE)
           .when(label_expr == "suspicious", ML_SUSPICIOUS_SCORE)
           .otherwise(lit(None))
        )

        # attack_type encodes both the detection method and the ML verdict
        attack_type_expr = (
            when(label_expr == "malicious",  lit("ml-malicious"))
           .when(label_expr == "suspicious", lit("ml-suspicious"))
           .otherwise(lit(None))
        )

        ml_threats = (
            predictions
            .withColumn("ml_label",    label_expr)
            .withColumn("threat_score", score_expr)
            .withColumn("attack_type",  attack_type_expr)
            .filter(col("attack_type").isNotNull())
            .select(
                col("source_ip").alias("ip_source"),
                current_timestamp().alias("last_seen"),
                col("threat_score"),
                col("attack_type"),
            )
        )

        n = ml_threats.count()
        if n == 0:
            return

        if n > MAX_THREATS_PER_BATCH:
            ml_threats = ml_threats.limit(MAX_THREATS_PER_BATCH)
            n = MAX_THREATS_PER_BATCH

        print(f"[ml-stream] batch {batch_id} — {n} ML-detected threat(s)")

        ml_threats.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE) \
            .mode("append") \
            .save()

    q5 = logs.writeStream \
        .outputMode("append") \
        .foreachBatch(_ml_save_batch) \
        .queryName("ml-stream") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/ml-stream") \
        .start()
    print("Detection 5 started: ML classification (Random Forest)")
else:
    print("Detection 5 skipped: run 'make ml-train' first to enable ML detection.")


# ── Wait for all queries to finish ────────────────────────────────────────
active = 5 if _ml_model is not None else 4
print(f"All {active} detection streams are running. Press Ctrl+C to stop.")
spark.streams.awaitAnyTermination()
