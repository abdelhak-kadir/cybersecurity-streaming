import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, date_format, to_timestamp

spark = SparkSession.builder.appName("AttackEvolution").getOrCreate()

script_dir = os.path.dirname(os.path.abspath(__file__))
path = os.path.join(script_dir, "../data/cybersecurity_threat_detection_logs.csv")

df = spark.read.csv(path, header=True, inferSchema=True)

# 1. Préparation du temps et détection
# On convertit le timestamp et on crée une colonne pour identifier les attaques
sqli_pattern = r"select|union|insert|drop|'|--|#"

df_time = df.withColumn("event_time", to_timestamp(col("timestamp"))) \
            .withColumn("is_sqli", lower(col("request_path")).rlike(sqli_pattern))

# 2. On groupe par heure (ou par jour si tu préfères)
# 'yyyy-MM-dd HH' pour l'évolution par heure
evolution = df_time.filter(col("is_sqli") == True) \
                   .withColumn("hour", date_format(col("event_time"), "yyyy-MM-dd HH:00")) \
                   .groupBy("hour") \
                   .count() \
                   .orderBy("hour")

# 3. Affichage
print("\n" + "="*60)
print("ÉVOLUTION TEMPORELLE DES ATTAQUES SQLi")
print("="*60)
evolution.show(truncate=False)

# Sauvegarde pour faire un graphique plus tard
output_path = os.path.join(script_dir, "../data/attack_evolution_stats")
evolution.write.mode("overwrite").csv(output_path, header=True)

spark.stop()
