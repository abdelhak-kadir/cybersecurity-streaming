import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

# 1. Initialisation Spark (Configuré pour ton réseau Docker)
spark = SparkSession.builder \
    .appName("CyberDetection_HDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Chemin HDFS (doit correspondre à la sortie de ton premier script)
HDFS_PATH = "hdfs://namenode:9000/logs/cybersecurity/"

print(f"🚀 Lecture des données partitionnées sur HDFS : {HDFS_PATH}")

try:
    # Lecture du format Parquet (plus rapide que le CSV car déjà traité)
    df = spark.read.parquet(HDFS_PATH)
except Exception as e:
    print(f"❌ Erreur : Impossible de lire les données. Vérifie si le premier script a bien fonctionné. {e}")
    spark.stop()
    exit()

# 3. Patterns de détection (Sécurisés)
# On cherche les mots clés SQL et les balises Script
sqli_pattern = r"select\s+|union\s+|insert\s+|delete\s+|drop\s+|'|--|#|cast\(|convert\("
xss_pattern = r"<script.*?>|alert\(|onload=|onerror=|eval\(|javascript:"

# 4. Analyse et marquage des intrusions
df_analyzed = df.withColumn("is_sqli", 
    lower(col("request_path")).rlike(sqli_pattern) | lower(col("user_agent")).rlike(sqli_pattern)
).withColumn("is_xss", 
    lower(col("request_path")).rlike(xss_pattern) | lower(col("user_agent")).rlike(xss_pattern)
)

# 5. Filtrage : On ne garde que les alertes
alerts = df_analyzed.filter((col("is_sqli") == True) | (col("is_xss") == True))

# 6. Affichage du rapport technique
print("\n" + "="*60)
print("📊 RAPPORT DE DÉTECTION D'INTRUSION (MODE BATCH)")
print(f"Total des logs analysés : {df.count()}")
print(f"Alertes SQL Injection   : {df_analyzed.filter(col('is_sqli') == True).count()}")
print(f"Alertes XSS             : {df_analyzed.filter(col('is_xss') == True).count()}")
print("="*60 + "\n")

if alerts.count() > 0:
    print("🚩 Échantillon des 10 dernières alertes détectées :")
    alerts.select("source_ip", "request_path", "is_sqli", "is_xss").show(10, truncate=False)

    # 7. Sauvegarde des alertes sur HDFS (Dossier spécifique aux incidents)
    ALERTS_OUTPUT = "hdfs://namenode:9000/alerts/detected_intrusions"
    alerts.write.mode("overwrite").parquet(ALERTS_OUTPUT)
    print(f"✅ Alertes persistées sur HDFS : {ALERTS_OUTPUT}")
else:
    print("✅ Analyse terminée : Aucune menace détectée.")

spark.stop()
