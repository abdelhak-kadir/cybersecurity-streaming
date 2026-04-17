from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, lower, regexp_extract, when

spark = SparkSession.builder \
    .appName("BatchSQLiXSSPatterns") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(" Spark démarré")

HDFS_INPUT = "hdfs://namenode:9000/logs/cybersecurity/"
ALERTS_OUTPUT = "hdfs://namenode:9000/alerts/detected_intrusions"
PATTERNS_OUTPUT = "hdfs://namenode:9000/results/attack_patterns/"

df = spark.read.parquet(HDFS_INPUT)
print(f" Données chargées : {df.count()} lignes")

request_path = lower(col("request_path"))

SQLI_REGEX = (
    r"(union\s+select|or\s+['\"]?1['\"]?\s*=\s*['\"]?1|drop\s+table|"
    r"insert\s+into|delete\s+from|sleep\s*\(|benchmark\s*\(|select\s+.+\s+from|--|#)"
)
XSS_REGEX = (
    r"(<script.*?>|javascript:|alert\s*\(|onerror=|onload=|<iframe.*?>|"
    r"document\.cookie|window\.location|eval\s*\()"
)

df_tagged = (
    df.withColumn("normalized_request_path", request_path)
    .withColumn("sqli_pattern", regexp_extract(request_path, SQLI_REGEX, 1))
    .withColumn("xss_pattern", regexp_extract(request_path, XSS_REGEX, 1))
    .withColumn(
        "attack_type",
        when(col("sqli_pattern") != "", lit("SQLi"))
        .when(col("xss_pattern") != "", lit("XSS"))
    )
    .withColumn(
        "matched_pattern",
        when(col("attack_type") == "SQLi", col("sqli_pattern"))
        .when(col("attack_type") == "XSS", col("xss_pattern"))
    )
)

alerts = (
    df_tagged
    .filter(col("attack_type").isNotNull())
    .select(
        "timestamp",
        "source_ip",
        "dest_ip",
        "protocol",
        "action",
        "threat_label",
        "request_path",
        "attack_type",
        "matched_pattern"
    )
)

pattern_summary = (
    alerts
    .groupBy("attack_type", "matched_pattern")
    .agg(count("*").alias("occurrences"))
    .orderBy(col("occurrences").desc(), col("attack_type"), col("matched_pattern"))
)

print("\n" + "=" * 60)
print(" RAPPORT BATCH SQLi / XSS (request_path uniquement)")
print("=" * 60)
print(f" Alertes SQLi/XSS : {alerts.count()}")

print("\n Top patterns fréquents :")
pattern_summary.show(20, truncate=False)

alerts.write.mode("overwrite").parquet(ALERTS_OUTPUT)
print(f" Alertes détaillées sauvegardées : {ALERTS_OUTPUT}")

pattern_summary.write.mode("overwrite").parquet(PATTERNS_OUTPUT)
print(f" Patterns fréquents sauvegardés : {PATTERNS_OUTPUT}")

spark.stop()
print(" Script 06 terminé !")
