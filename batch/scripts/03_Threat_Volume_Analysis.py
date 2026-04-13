import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round, when, lower

# 1. Initialisation de Spark
spark = SparkSession.builder \
    .appName("DetailedThreatVolume") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# 2. Chemin vers tes données sur HDFS (plus rapide et cohérent)
# On utilise les données nettoyées par ton script de chargement
HDFS_PATH = "hdfs://namenode:9000/logs/cybersecurity/"

# 3. Chargement du dataset Parquet
print(f"🚀 Lecture des logs depuis HDFS : {HDFS_PATH}")
df = spark.read.parquet(HDFS_PATH)

# 4. Prétraitement
df_lower = df.withColumn("req_lower", lower(col("request_path")))

# 5. Patterns de détection
sqli_pattern = r"select|union|insert|drop|'|--|#"
xss_pattern = r"script|alert|<|>|javascript"

# 6. Création de la catégorie détaillée
df_categorized = df_lower.withColumn("attack_type", 
    when(col("req_lower").rlike(sqli_pattern), "SQL Injection")
    .when(col("req_lower").rlike(xss_pattern), "XSS")
    .otherwise("Other/Benign")
)

# 7. Calcul des statistiques
# Note: j'utilise spark_sum pour éviter les conflits avec le sum de Python
detailed_stats = df_categorized.groupBy("attack_type").agg(
    spark_sum("bytes_transferred").alias("total_bytes"),
    round(spark_sum("bytes_transferred") / 1024, 2).alias("total_KB")
).orderBy(col("total_bytes").desc())

# 8. Affichage
print("\n" + "="*60)
print("📊 VOLUME DE DONNÉES PAR TYPE D'ATTAQUE (DÉTECTION SALMA)")
print("="*60)
detailed_stats.show(truncate=False)

# 9. Sauvegarde du résultat pour HBase plus tard
# Optionnel : tu peux sauvegarder ce petit tableau sur HDFS
detailed_stats.write.mode("overwrite").parquet("hdfs://namenode:9000/results/attack_volumes/")

spark.stop()
