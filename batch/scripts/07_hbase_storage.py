import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum as spark_sum, least, lit

# ── 1. Créer la session Spark ──────────────────────────────────────────
spark = SparkSession.builder \
    .appName("HBaseStorage") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(" Spark démarré")

# ── 2. Connexion à HBase ───────────────────────────────────────────────
connection = happybase.Connection('hbase')
print(" Connecté à HBase")

# ── 3. Créer les tables HBase ─────────────────────────────────────────
tables_existantes = [t.decode() for t in connection.tables()]

if 'ip_reputation' not in tables_existantes:
    connection.create_table(
        'ip_reputation',
        {'info': dict()}
    )
    print(" Table ip_reputation créée")

if 'attack_patterns' not in tables_existantes:
    connection.create_table(
        'attack_patterns',
        {'info': dict()}
    )
    print(" Table attack_patterns créée")

if 'threat_timeline' not in tables_existantes:
    connection.create_table(
        'threat_timeline',
        {'info': dict()}
    )
    print(" Table threat_timeline créée")

# ── 4. Stocker Top IPs dans ip_reputation ─────────────────────────────
print("\n Stockage des IPs malveillantes...")
top_ips = spark.read.parquet("hdfs://namenode:9000/results/top_ips/")
table_ip = connection.table('ip_reputation')

for row in top_ips.collect():
    table_ip.put(
        row['source_ip'].encode(),
        {
            b'info:nb_attaques':  str(row['nb_attaques']).encode(),
            b'info:source':       b'top_ips_batch'
        }
    )
print(f" {top_ips.count()} IPs stockées dans ip_reputation")

# ── 5. Stocker Port Scans dans attack_patterns ────────────────────────
print("\n Stockage des scans de ports...")
port_scans = spark.read.parquet("hdfs://namenode:9000/results/port_scans/")
table_attacks = connection.table('attack_patterns')

for row in port_scans.collect():
    row_key = f"{row['source_ip']}_{row['window_start']}".encode()
    table_attacks.put(
        row_key,
        {
            b'info:source_ip':             row['source_ip'].encode(),
            b'info:window_start':          str(row['window_start']).encode(),
            b'info:window_end':            str(row['window_end']).encode(),
            b'info:destinations_uniques':  str(row['destinations_uniques']).encode(),
            b'info:nb_connexions':         str(row['nb_connexions']).encode(),
            b'info:attack_type':           b'port_scan'
        }
    )
print(f" {port_scans.count()} scans stockés dans attack_patterns")

# ── 6. Stocker threat_timeline ────────────────────────────────────────
print("\n Stockage de la timeline des menaces...")
df = spark.read.parquet("hdfs://namenode:9000/logs/cybersecurity/")

timeline = df.filter(
    col("threat_label").isin(["suspicious", "malicious"])
) \
.groupBy("year", "month", "day") \
.agg(count("*").alias("nb_menaces")) \
.orderBy("year", "month", "day")

table_timeline = connection.table('threat_timeline')

for row in timeline.collect():
    row_key = f"{row['year']}-{row['month']:02d}-{row['day']:02d}".encode()
    table_timeline.put(
        row_key,
        {
            b'info:nb_menaces': str(row['nb_menaces']).encode(),
            b'info:year':       str(row['year']).encode(),
            b'info:month':      str(row['month']).encode(),
            b'info:day':        str(row['day']).encode()
        }
    )
print(f" Timeline stockée dans threat_timeline")

connection.close()
spark.stop()
print("\n Script 07 terminé — tout est dans HBase !")