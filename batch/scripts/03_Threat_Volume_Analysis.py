import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, min as spark_min, max as spark_max, round, when, lower

# =====================================================
# 1. Initialisation de Spark
# =====================================================
spark = SparkSession.builder \
    .appName("FullCorrelation_Threat_Volume") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =====================================================
# 2. Chargement des données (Parquet sur HDFS)
# =====================================================
HDFS_PATH = "hdfs://namenode:9000/logs/cybersecurity/"
try:
    df = spark.read.parquet(HDFS_PATH)
except Exception as e:
    print(f"❌ Erreur : {e}")
    spark.stop()
    exit()

# =====================================================
# 3. Corrélation Complète (Pattern Matching)
# =====================================================
SQLI_PATTERN = r"select|union|insert|drop|sleep|benchmark|'|--|#"
XSS_PATTERN = r"script|alert|<|>|javascript|iframe|document\.cookie"

df_correlated = df.withColumn(
    "attack_type",
    when(lower(col("request_path")).rlike(SQLI_PATTERN), "SQL Injection")
    .when(lower(col("request_path")).rlike(XSS_PATTERN), "XSS")
    .otherwise("Benign/Unknown")
)

# =====================================================
# 4. Calcul des Statistiques (Min, Max, Moyenne, Unités)
# =====================================================
# On calcule tout en une seule agrégation pour plus de performance
volume_stats = df_correlated.groupBy("attack_type").agg(
    spark_sum("bytes_transferred").alias("total_b"),
    spark_min("bytes_transferred").alias("min_b"),
    spark_max("bytes_transferred").alias("max_b"),
    avg("bytes_transferred").alias("avg_b")
)

# Conversion en KB, MB, GB pour satisfaire les specs du prof
final_stats = volume_stats.select(
    col("attack_type"),
    round(col("total_b") / 1024, 2).alias("Vol_KB"),
    round(col("total_b") / (1024**2), 2).alias("Vol_MB"),
    round(col("total_b") / (1024**3), 4).alias("Vol_GB"),
    round(col("min_b"), 2).alias("Min_Bytes"),
    round(col("max_b"), 2).alias("Max_Bytes"),
    round(col("avg_b"), 2).alias("Moyenne_Bytes")
)

# =====================================================
# 5. Détection des Anomalies (> 10MB)
# =====================================================
# Le prof veut isoler les transferts volumineux (Exfiltration possible)
anomalies = df_correlated.filter(col("bytes_transferred") > (10 * 1024 * 1024))

# =====================================================
# 6. Affichage et Sauvegarde HDFS
# =====================================================
print("\n" + "=" * 80)
print("📊 RAPPORT DE CORRÉLATION ET ANALYSE DE VOLUME")
print("=" * 80)
final_stats.show(truncate=False)

if anomalies.count() > 0:
    print(f"⚠️ ATTENTION : {anomalies.count()} transferts anormaux (> 10MB) détectés !")
    anomalies.select("source_ip", "request_path", "bytes_transferred").show(5)

# Sauvegarde des statistiques finales sur HDFS
OUTPUT_STATS = "hdfs://namenode:9000/results/correlation_stats/"
final_stats.write.mode("overwrite").parquet(OUTPUT_STATS)

# Sauvegarde spécifique des anomalies pour investigation
OUTPUT_ANOMALIES = "hdfs://namenode:9000/results/detected_anomalies_10MB/"
anomalies.write.mode("overwrite").parquet(OUTPUT_ANOMALIES)

print(f"✅ Statistiques et Anomalies sauvegardées sur HDFS.")

spark.stop()
