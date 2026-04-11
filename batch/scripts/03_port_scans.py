from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, window

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
# Logique : une IP qui contacte beaucoup de destinations
# différentes en moins de 5 minutes = scan probable
port_scans = df_tcp \
    .groupBy(
        col("source_ip"),
        window(col("timestamp"), "5 minutes")
    ) \
    .agg(
        countDistinct("dest_ip").alias("destinations_uniques"),
        count("*").alias("nb_connexions")
    ) \
    .filter(col("destinations_uniques") > 10) \
    .select(
        col("source_ip"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("destinations_uniques"),
        col("nb_connexions")
    ) \
    .orderBy(col("destinations_uniques").desc())

print(f"\n Scans de ports détectés :")
port_scans.show(20, truncate=False)
print(f" Nombre total de scans détectés : {port_scans.count()}")

# ── 5. Sauvegarder dans HDFS ──────────────────────────────────────────
RESULT_PATH = "hdfs://namenode:9000/results/port_scans/"
port_scans.write.mode("overwrite").parquet(RESULT_PATH)
print(f" Résultats sauvegardés : {RESULT_PATH}")

spark.stop()
print(" Script 03 terminé !")