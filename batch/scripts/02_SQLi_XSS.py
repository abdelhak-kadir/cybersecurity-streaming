import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when

# =====================================================
# 1. Initialisation de Spark
# =====================================================
spark = SparkSession.builder \
    .appName("Advanced_Cyber_Detection") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =====================================================
# 2. Lecture des données sur HDFS
# =====================================================
HDFS_PATH = "hdfs://namenode:9000/logs/cybersecurity/"
try:
    df = spark.read.parquet(HDFS_PATH)
except Exception as e:
    print(f"❌ Erreur : {e}")
    spark.stop()
    exit()

# =====================================================
# 3. PATTERNS AVANCÉS (La partie que tu voulais)
# =====================================================

# 1. SQLi Musclé : Ajout de Blind SQLi (sleep, benchmark) et encodages
SQLI_PATTERN = (
    r"select\s+|union\s+|insert\s+|delete\s+|drop\s+|'|--|#|"  # Base
    r"sleep\(|benchmark\(|waitfor\s+delay|"                    # Blind SQLi
    r"group\s+by|having|order\s+by|limit|"                      # Énumération
    r"util_http|ctxsys|dbms_pipe|extractvalue"                  # Out-of-band / XML
)

# 2. XSS Avancé : Ajout d'iframes, manipulation de cookies et événements obscurs
XSS_PATTERN = (
    r"<script.*?>|alert\(|eval\(|javascript:|onerror=|onload=|" # Base
    r"<iframe.*?>|document\.cookie|window\.location|"           # Vol de session
    r"prompt\(|confirm\(|String\.fromCharCode|"                # Obfuscation
    r"onmouseover=|onfocus=|onscroll="                          # Events DOM
)

# 3. Outils d'Attaque : Détection via le User-Agent
TOOLS_PATTERN = r"sqlmap|nikto|dirbuster|gobuster|nmap|hydra|metasploit|burp|nessus"

# =====================================================
# 4. Analyse Multi-Critères
# =====================================================
df_analyzed = df.withColumn(
    "is_sqli",
    lower(col("request_path")).rlike(SQLI_PATTERN) | 
    lower(col("user_agent")).rlike(SQLI_PATTERN)
).withColumn(
    "is_xss",
    lower(col("request_path")).rlike(XSS_PATTERN) | 
    lower(col("user_agent")).rlike(XSS_PATTERN)
).withColumn(
    "is_tool",
    lower(col("user_agent")).rlike(TOOLS_PATTERN)
)

# =====================================================
# 5. Filtrage et Statistiques
# =====================================================
# On crée une colonne globale pour les alertes
alerts = df_analyzed.filter(
    (col("is_sqli") == True) | (col("is_xss") == True) | (col("is_tool") == True)
)

sqli_count = df_analyzed.filter(col("is_sqli") == True).count()
xss_count = df_analyzed.filter(col("is_xss") == True).count()
tools_count = df_analyzed.filter(col("is_tool") == True).count()

print("\n" + "=" * 60)
print("🛡️  RAPPORT DE DÉTECTION AVANCÉ (BATCH)")
print(f"Alertes SQL Injection (Blind incluse) : {sqli_count}")
print(f"Alertes XSS (Advanced DOM/Cookie)    : {xss_count}")
print(f"Outils d'attaque détectés (User-Agent): {tools_count}")
print("=" * 60 + "\n")

# =====================================================
# 6. Sauvegarde des résultats
# =====================================================
if alerts.count() > 0:
    ALERTS_OUTPUT = "hdfs://namenode:9000/alerts/advanced_intrusions"
    alerts.write.mode("overwrite").parquet(ALERTS_OUTPUT)
    
    print("🚩 Top 5 des IPs les plus suspectes :")
    alerts.groupBy("source_ip").count().orderBy(col("count").desc()).show(5)
else:
    print("✅ Aucune menace détectée avec les nouveaux patterns.")

spark.stop()
