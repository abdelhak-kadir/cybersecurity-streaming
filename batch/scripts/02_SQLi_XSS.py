import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

# Initialisation
spark = SparkSession.builder.appName("SQLi_XSS_Detection").getOrCreate()

# --- CORRECTION ICI ---
# On définit le dossier où se trouve le script
script_dir = os.path.dirname(os.path.abspath(__file__))
# On remonte d'un cran pour aller dans 'data'
path = os.path.join(script_dir, "../data/cybersecurity_threat_detection_logs.csv")
# ----------------------

# Chargement du dataset
df = spark.read.csv(path, header=True, inferSchema=True)

# Patterns de détection
sqli_pattern = r"select\s+|union\s+|insert\s+|delete\s+|drop\s+|'|--|#|cast\(|convert\("
xss_pattern = r"<script.*?>|alert\(|onload=|onerror=|eval\(|javascript:"

# Analyse sur les colonnes cibles
df_analyzed = df.withColumn("is_sqli", 
    lower(col("request_path")).rlike(sqli_pattern) | lower(col("user_agent")).rlike(sqli_pattern)
).withColumn("is_xss", 
    lower(col("request_path")).rlike(xss_pattern) | lower(col("user_agent")).rlike(xss_pattern)
)

# Filtrage des alertes
alerts = df_analyzed.filter((col("is_sqli") == True) | (col("is_xss") == True))

# Affichage du rapport
print("\n" + "="*60)
print("RAPPORT D'ANALYSE CYBER (BATCH)")
print(f"Total des lignes : {df.count()}")
print(f"Alertes détectées : {alerts.count()}")
print("="*60 + "\n")

# Affichage des résultats
alerts.select("source_ip", "request_path", "is_sqli", "is_xss").show(10, truncate=False)

# Sauvegarde
output_path = os.path.join(script_dir, "../data/sqli_xss_results")
alerts.write.mode("overwrite").csv(output_path, header=True)

spark.stop()

