import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, date_format, to_timestamp, when


spark = SparkSession.builder \
    .appName("Full_Evolution_Analytics") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


HDFS_INPUT = "hdfs://namenode:9000/logs/cybersecurity/"
try:
    df = spark.read.parquet(HDFS_INPUT)
except Exception as e:
    print(f" Erreur : {e}")
    spark.stop()
    exit()


SQLI_PATTERN = r"select|union|insert|drop|sleep|'|--|#"
XSS_PATTERN = r"script|alert|<|>|javascript|iframe"
TOOLS_PATTERN = r"sqlmap|nikto|dirbuster|nmap"

df_enriched = df.withColumn(
    "event_time", to_timestamp(col("timestamp"))
).withColumn(
    "attack_category",
    when(lower(col("request_path")).rlike(SQLI_PATTERN), "SQLi")
    .when(lower(col("request_path")).rlike(XSS_PATTERN), "XSS")
    .when(lower(col("user_agent")).rlike(TOOLS_PATTERN), "Tool_Scanner")
    .otherwise(col("threat_label"))
)


df_timed = df_enriched.withColumn("Month", date_format(col("event_time"), "yyyy-MM")) \
                      .withColumn("Day", date_format(col("event_time"), "yyyy-MM-dd")) \
                      .withColumn("Hour", date_format(col("event_time"), "yyyy-MM-dd HH:00"))


evo_hour = df_timed.groupBy("Hour", "attack_category").count().orderBy("Hour")


evo_day = df_timed.groupBy("Day", "attack_category").count().orderBy("Day")


evo_month = df_timed.groupBy("Month", "attack_category").count().orderBy("Month")


print("\n" + "=" * 60)
print(" ANALYSE TEMPORELLE MULTI-DIMENSIONS")
print("=" * 60)

print("\n--- Top 10 par Heure ---")
evo_hour.show(10, truncate=False)

print("\n--- Top 5 par Jour ---")
evo_day.show(5, truncate=False)


BASE_OUT = "hdfs://namenode:9000/results/evolution/"

evo_hour.write.mode("overwrite").parquet(BASE_OUT + "hourly")
evo_day.write.mode("overwrite").parquet(BASE_OUT + "daily")
evo_month.write.mode("overwrite").parquet(BASE_OUT + "monthly")

print(f"\n Toutes les statistiques sont sauvegardées dans : {BASE_OUT}")

spark.stop()