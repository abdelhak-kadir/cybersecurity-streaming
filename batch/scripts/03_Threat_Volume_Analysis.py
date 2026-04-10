import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round, when, lower

# 1. Initialisation de Spark
spark = SparkSession.builder.appName("DetailedThreatVolume").getOrCreate()

# 2. Chemins vers ton dataset
script_dir = os.path.dirname(os.path.abspath(__file__))
path = os.path.join(script_dir, "../data/cybersecurity_threat_detection_logs.csv")

# 3. Chargement du dataset (C'est la ligne qui manquait !)
df = spark.read.csv(path, header=True, inferSchema=True)

# 4. On utilise lower() pour éviter les problèmes de MAJUSCULES/minuscules
df_lower = df.withColumn("req_lower", lower(col("request_path")))

# 5. Patterns de détection flexibles
sqli_pattern = r"select|union|insert|drop|'|--|#"
xss_pattern = r"script|alert|<|>|javascript"

# 6. Création de la catégorie détaillée
df_categorized = df_lower.withColumn("attack_type", 
    when(col("req_lower").rlike(sqli_pattern), "SQL Injection")
    .when(col("req_lower").rlike(xss_pattern), "XSS")
    .otherwise("Other/Benign")
)

# 7. Calcul des statistiques par type d'attaque
detailed_stats = df_categorized.groupBy("attack_type").agg(
    sum("bytes_transferred").alias("total_bytes"),
    round(sum("bytes_transferred") / 1024, 2).alias("total_KB")
).orderBy(col("total_bytes").desc())

# 8. Affichage
print("\n" + "="*60)
print("VOLUME DE DONNÉES PAR TYPE D'ATTAQUE (DÉTECTION SALMA)")
print("="*60)
detailed_stats.show(truncate=False)

spark.stop()

