from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, lit, sum as spark_sum, when

spark = SparkSession.builder \
    .appName("TopMaliciousIPs") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(" Spark démarré")

HDFS_PATH = "hdfs://namenode:9000/logs/cybersecurity/"
df = spark.read.parquet(HDFS_PATH)
print(f" Données chargées : {df.count()} lignes")

df_threats = df.filter(
    col("threat_label").isin(["suspicious", "malicious"])
)
print(f" Lignes suspectes/malveillantes : {df_threats.count()}")

top_ips = (
    df_threats
    .groupBy("source_ip")
    .agg(
        count("*").alias("nb_attaques"),
        spark_sum(
            when(col("threat_label") == "malicious", lit(1)).otherwise(lit(0))
        ).alias("nb_malicious"),
        spark_sum(
            when(col("threat_label") == "suspicious", lit(1)).otherwise(lit(0))
        ).alias("nb_suspicious")
    )
    .orderBy(
        desc("nb_attaques"),
        desc("nb_malicious"),
        desc("nb_suspicious"),
        desc("source_ip")
    )
    .limit(10)
)
   

print("\n Top 10 IPs malveillantes :")
top_ips.show(truncate=False)

# ── 5. Sauvegarder le résultat dans HDFS ──────────────────────────────
RESULT_PATH = "hdfs://namenode:9000/results/top_ips/"
top_ips.write.mode("overwrite").parquet(RESULT_PATH)
print(f" Résultats sauvegardés : {RESULT_PATH}")

spark.stop()
print(" Script 02 terminé !")
