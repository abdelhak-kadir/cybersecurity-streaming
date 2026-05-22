from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, round as spark_round,
    count, avg, max as spark_max, min as spark_min
)

spark = SparkSession.builder \
    .appName("ThreatVolumeAnalysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(" Spark démarré")


HDFS_PATH = "hdfs://namenode:9000/logs/cybersecurity/"
df = spark.read.parquet(HDFS_PATH)
print(f" Données chargées : {df.count()} lignes")


stats_by_threat = df.groupBy("threat_label") \
    .agg(
        count("*").alias("nb_events"),
        spark_sum("bytes_transferred").alias("total_bytes"),
        spark_round(avg("bytes_transferred"), 2).alias("moyenne_bytes"),
        spark_max("bytes_transferred").alias("max_bytes"),
        spark_min("bytes_transferred").alias("min_bytes"),
        spark_round(spark_sum("bytes_transferred") / 1024, 2).alias("total_KB"),
        spark_round(spark_sum("bytes_transferred") / (1024*1024), 2).alias("total_MB"),
        spark_round(spark_sum("bytes_transferred") / (1024*1024*1024), 4).alias("total_GB")
    ) \
    .orderBy(col("total_bytes").desc())

stats_by_protocol = df.groupBy("protocol", "threat_label") \
    .agg(
        count("*").alias("nb_events"),
        spark_round(avg("bytes_transferred"), 2).alias("moyenne_bytes"),
        spark_sum("bytes_transferred").alias("total_bytes")
    ) \
    .orderBy(col("total_bytes").desc())


SEUIL_BYTES = 10 * 1024 * 1024  

transferts_anormaux = df \
    .filter(col("bytes_transferred") > SEUIL_BYTES) \
    .groupBy("source_ip", "threat_label") \
    .agg(
        count("*").alias("nb_transferts_anormaux"),
        spark_sum("bytes_transferred").alias("total_bytes"),
        spark_round(
            spark_sum("bytes_transferred") / (1024*1024), 2
        ).alias("total_MB")
    ) \
    .orderBy(col("total_bytes").desc())

print("\n" + "="*60)
print("   CORRÉLATION bytes_transferred ↔ threat_label")
print("="*60)
stats_by_threat.show(truncate=False)

print("\n" + "="*60)
print("   VOLUME PAR PROTOCOLE + TYPE DE MENACE")
print("="*60)
stats_by_protocol.show(20, truncate=False)

print("\n" + "="*60)
print("   TRANSFERTS ANORMAUX (> 10MB)")
print("="*60)
transferts_anormaux.show(20, truncate=False)

stats_by_threat.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/results/threat_volume/")
print(" Stats par threat_label sauvegardées")

stats_by_protocol.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/results/attacks_by_protocol/")
print(" Stats par protocole sauvegardées")

transferts_anormaux.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/results/abnormal_transfers/")
print(" Transferts anormaux sauvegardés")

spark.stop()
print(" Script Threat Volume terminé !")