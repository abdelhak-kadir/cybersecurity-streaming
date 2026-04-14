from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, lit, window

# ── 1. Créer la session Spark ──────────────────────────────────────────
spark = SparkSession.builder \
    .appName("PortScanDetection") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(" Spark démarré")

# ── 2. Lire les données depuis HDFS ───────────────────────────────────
HDFS_PATH = "hdfs://namenode:9000/logs/cybersecurity/"
df = spark.read.parquet(HDFS_PATH)
print(f" Données chargées : {df.count()} lignes")

# ── 3. Garder seulement les connexions TCP ────────────────────────────
df_tcp = df.filter(col("protocol") == "TCP")
print(f" Connexions TCP : {df_tcp.count()}")

# ── 4. Détecter les scans de ports ────────────────────────────────────
# Le dataset fourni ne contient pas de colonne de port.
# On utilise donc dest_ip comme proxy de cibles distinctes dans une fenêtre
# glissante de 5 minutes pour conserver une détection batch exploitable.
scan_key = "dest_ip"

port_scans = (
    df_tcp
    .groupBy(
        col("source_ip"),
        window(col("timestamp"), "5 minutes")
    )
    .agg(
        countDistinct(scan_key).alias("distinct_targets"),
        count("*").alias("nb_connexions")
    )
    .filter(col("distinct_targets") > 10)
    .select(
        col("source_ip"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("distinct_targets"),
        col("nb_connexions"),
        lit(scan_key).alias("scan_basis")
    )
    .orderBy(col("distinct_targets").desc(), col("nb_connexions").desc())
)

print(f"\n Scans de ports détectés :")
port_scans.show(20, truncate=False)
print(f" Nombre total de scans détectés : {port_scans.count()}")

# ── 5. Sauvegarder dans HDFS ──────────────────────────────────────────
RESULT_PATH = "hdfs://namenode:9000/results/port_scans/"
port_scans.write.mode("overwrite").parquet(RESULT_PATH)
print(f" Résultats sauvegardés : {RESULT_PATH}")

spark.stop()
print(" Script 03 terminé !")
