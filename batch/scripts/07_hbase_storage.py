import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, greatest, least, lit

# ── 1. Créer la session Spark ──────────────────────────────────────────
spark = SparkSession.builder \
    .appName("HBaseStorage") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(" Spark démarré")

TOP_IPS_PATH = "hdfs://namenode:9000/results/top_ips/"
PORT_SCANS_PATH = "hdfs://namenode:9000/results/port_scans/"
INPUT_LOGS_PATH = "hdfs://namenode:9000/logs/cybersecurity/"
SQLI_XSS_ALERTS_PATH = "hdfs://namenode:9000/alerts/detected_intrusions"
ATTACK_PATTERNS_PATH = "hdfs://namenode:9000/results/attack_patterns/"
THREAT_VOLUME_PATH = "hdfs://namenode:9000/results/threat_volume/"

# ── 2. Connexion à HBase ───────────────────────────────────────────────
connection = happybase.Connection(host="hbase", port=9090)
print(" Connecté à HBase")

# ── 3. Créer les tables HBase ─────────────────────────────────────────
tables_existantes = [t.decode() for t in connection.tables()]

if "ip_reputation" not in tables_existantes:
    connection.create_table("ip_reputation", {"info": dict()})
    print(" Table ip_reputation créée")

if "attack_patterns" not in tables_existantes:
    connection.create_table("attack_patterns", {"info": dict()})
    print(" Table attack_patterns créée")

if "threat_timeline" not in tables_existantes:
    connection.create_table("threat_timeline", {"info": dict()})
    print(" Table threat_timeline créée")

# ── 4. Stocker Top IPs dans ip_reputation ─────────────────────────────
print("\n Stockage des IPs malveillantes...")
top_ips = (
    spark.read.parquet(TOP_IPS_PATH)
    .withColumn(
        "reputation_score",
        least(
            lit(100),
            greatest(
                lit(0),
                col("nb_malicious") * lit(10) + col("nb_suspicious") * lit(5)
            )
        )
    )
)
table_ip = connection.table("ip_reputation")

for row in top_ips.collect():
    table_ip.put(
        row["source_ip"].encode(),
        {
            b"info:nb_attaques": str(row["nb_attaques"]).encode(),
            b"info:nb_malicious": str(row["nb_malicious"]).encode(),
            b"info:nb_suspicious": str(row["nb_suspicious"]).encode(),
            b"info:reputation_score": str(row["reputation_score"]).encode(),
            b"info:source": b"top_ips_batch",
        },
    )
print(f" {top_ips.count()} IPs stockées dans ip_reputation")

# ── 5. Stocker Port Scans dans attack_patterns ────────────────────────
print("\n Stockage des scans de ports...")
port_scans = spark.read.parquet(PORT_SCANS_PATH)
table_attacks = connection.table("attack_patterns")

for row in port_scans.collect():
    row_key = f"PORTSCAN_{row['source_ip']}_{row['window_start']}".encode()
    table_attacks.put(
        row_key,
        {
            b"info:source_ip": row["source_ip"].encode(),
            b"info:window_start": str(row["window_start"]).encode(),
            b"info:window_end": str(row["window_end"]).encode(),
            b"info:distinct_targets": str(row["distinct_targets"]).encode(),
            b"info:nb_connexions": str(row["nb_connexions"]).encode(),
            b"info:scan_basis": str(row["scan_basis"]).encode(),
            b"info:attack_type": b"port_scan",
            b"info:source": b"batch_port_scans",
        },
    )
print(f" {port_scans.count()} scans stockés dans attack_patterns")

# ── 5.bis Stocker les patterns SQLi/XSS ───────────────────────────────
print("\n Stockage des patterns SQLi/XSS...")
attack_patterns = spark.read.parquet(ATTACK_PATTERNS_PATH)

for row in attack_patterns.collect():
    row_key = f"PATTERN_{row['attack_type']}_{row['matched_pattern']}".encode()
    table_attacks.put(
        row_key,
        {
            b"info:attack_type": str(row["attack_type"]).encode(),
            b"info:matched_pattern": str(row["matched_pattern"]).encode(),
            b"info:occurrences": str(row["occurrences"]).encode(),
            b"info:source": b"batch_attack_patterns",
        },
    )
print(f" {attack_patterns.count()} patterns stockés dans attack_patterns")

# ── 5.ter Stocker les alertes détaillées SQLi/XSS ─────────────────────
print("\n Stockage des alertes SQLi/XSS détaillées...")
sqli_xss_alerts = spark.read.parquet(SQLI_XSS_ALERTS_PATH)

for row in sqli_xss_alerts.collect():
    row_key = f"ALERT_{row['source_ip']}_{row['timestamp']}_{row['attack_type']}".encode()
    table_attacks.put(
        row_key,
        {
            b"info:source_ip": str(row["source_ip"]).encode(),
            b"info:timestamp": str(row["timestamp"]).encode(),
            b"info:request": str(row["request_path"]).encode(),
            b"info:attack_type": str(row["attack_type"]).encode(),
            b"info:matched_pattern": str(row["matched_pattern"]).encode(),
            b"info:source": b"batch_detection_sqli_xss",
        },
    )

print(f" {sqli_xss_alerts.count()} alertes SQLi/XSS stockées dans attack_patterns")

# ── 5.quater Stocker le volume par menace ─────────────────────────────
print("\n Stockage du volume des données par type de menace...")

try:
    threat_volume = spark.read.parquet(THREAT_VOLUME_PATH)

    for row in threat_volume.collect():
        row_key = f"VOLUME_{row['threat_label']}".encode()
        table_attacks.put(
            row_key,
            {
                b"info:attack_type": str(row["threat_label"]).encode(),
                b"info:total_bytes": str(row["total_bytes"]).encode(),
                b"info:total_KB": str(row["total_KB"]).encode(),
                b"info:total_MB": str(row["total_MB"]).encode(),
                b"info:source": b"batch_volume_analysis",
            },
        )
    print(f" {threat_volume.count()} types de volumes stockés dans attack_patterns")
except Exception as e:
    print(f" Attention: Pas de données de volume trouvées sur HDFS ({e})")

# ── 6. Stocker threat_timeline ────────────────────────────────────────
print("\n Stockage de la timeline des menaces...")
df = spark.read.parquet(INPUT_LOGS_PATH)

timeline = (
    df.filter(col("threat_label").isin(["suspicious", "malicious"]))
    .groupBy("year", "month", "day", "threat_label")
    .agg(count("*").alias("nb_menaces"))
    .orderBy("year", "month", "day", "threat_label")
)

table_timeline = connection.table("threat_timeline")

for row in timeline.collect():
    row_key = (
        f"{row['year']}-{row['month']:02d}-{row['day']:02d}_{row['threat_label']}"
        .encode()
    )
    table_timeline.put(
        row_key,
        {
            b"info:nb_menaces": str(row["nb_menaces"]).encode(),
            b"info:year": str(row["year"]).encode(),
            b"info:month": str(row["month"]).encode(),
            b"info:day": str(row["day"]).encode(),
            b"info:threat_label": str(row["threat_label"]).encode(),
        },
    )
print(" Timeline stockée dans threat_timeline")

connection.close()
spark.stop()
print("\n Script terminé — tout est dans HBase !")
