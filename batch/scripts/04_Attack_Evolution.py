import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, date_format, to_timestamp

# 1. Initialisation Spark
spark = SparkSession.builder \
    .appName("AttackEvolution_HDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# 2. Lecture des données nettoyées sur HDFS
HDFS_INPUT = "hdfs://namenode:9000/logs/cybersecurity/"
df = spark.read.parquet(HDFS_INPUT)

# 3. Préparation et Détection
sqli_pattern = r"select|union|insert|drop|'|--|#"

# On s'assure que le timestamp est bien au format temps
df_time = df.withColumn("event_time", to_timestamp(col("timestamp"))) \
            .withColumn("is_sqli", lower(col("request_path")).rlike(sqli_pattern))

# 4. Groupement par heure pour voir l'évolution
evolution = df_time.filter(col("is_sqli") == True) \
                   .withColumn("hour", date_format(col("event_time"), "yyyy-MM-dd HH:00")) \
                   .groupBy("hour") \
                   .count() \
                   .orderBy("hour")

# 5. Affichage
print("\n" + "="*60)
print("📈 ÉVOLUTION TEMPORELLE DES ATTAQUES SQLi")
print("="*60)
evolution.show(truncate=False)

# 6. Sauvegarde du résultat sur HDFS pour le script HBase
HDFS_OUTPUT = "hdfs://namenode:9000/results/attack_evolution/"
evolution.write.mode("overwrite").parquet(HDFS_OUTPUT)

print(f"✅ Statistiques temporelles sauvegardées sur HDFS : {HDFS_OUTPUT}")
spark.stop()
