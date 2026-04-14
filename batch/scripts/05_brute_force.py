from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, window, when, lit
)

spark = SparkSession.builder \
    .appName("BruteForceDetection") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(" Spark démarré")


HDFS_PATH = "hdfs://namenode:9000/logs/cybersecurity/"
df = spark.read.parquet(HDFS_PATH)
print(" Données chargées ")

df_blocked = df.filter(col("action") == "blocked")
print(" Connexions bloquées :")


brute_force_1min = df_blocked \
    .groupBy(
        col("source_ip"),
        window(col("timestamp"), "1 minute")
    ) \
    .agg(
        count("*").alias("nb_tentatives"),
        count(col("dest_ip")).alias("nb_cibles")
    ) \
    .filter(col("nb_tentatives") >= 5) \
    .select(
        col("source_ip"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("nb_tentatives"),
        col("nb_cibles"),
        when(col("nb_tentatives") >= 20, lit("CRITIQUE"))
        .when(col("nb_tentatives") >= 10, lit("ÉLEVÉ"))
        .otherwise(lit("MOYEN")).alias("severite")
    ) \
    .orderBy(col("nb_tentatives").desc())


brute_force_ssh = df_blocked \
    .filter(col("protocol") == "SSH") \
    .groupBy(
        col("source_ip"),
        window(col("timestamp"), "5 minutes")
    ) \
    .agg(count("*").alias("nb_tentatives_ssh")) \
    .filter(col("nb_tentatives_ssh") >= 3) \
    .select(
        col("source_ip"),
        col("window.start").alias("window_start"),
        col("nb_tentatives_ssh")
    ) \
    .orderBy(col("nb_tentatives_ssh").desc())

top_brute_force_ips = brute_force_1min \
    .groupBy("source_ip") \
    .agg(
        count("*").alias("nb_episodes"),
        col("severite")
    ) \
    .orderBy(col("nb_episodes").desc()) \
    .limit(20)

total_bf = brute_force_1min.count()
critique = brute_force_1min.filter(col("severite") == "CRITIQUE").count()
eleve = brute_force_1min.filter(col("severite") == "ÉLEVÉ").count()
moyen = brute_force_1min.filter(col("severite") == "MOYEN").count()
ssh_bf = brute_force_ssh.count()

print("\n" + "="*60)
print("       RAPPORT DÉTECTION BRUTE FORCE")
print("="*60)
print(f" Total épisodes brute force : {total_bf}")
print(f" Sévérité CRITIQUE (20+)    : {critique}")
print(f"  Sévérité ÉLEVÉE  (10-19)  : {eleve}")
print(f" Sévérité MOYENNE  (5-9)   : {moyen}")
print(f" Brute force SSH           : {ssh_bf}")
print("="*60)

print("\n Top épisodes brute force :")
brute_force_1min.show(20, truncate=False)

print("\n Brute force SSH :")
brute_force_ssh.show(10, truncate=False)

print("\n Top IPs brute force :")
top_brute_force_ips.show(20, truncate=False)

brute_force_1min.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/results/brute_force/")
print(" Résultats brute force sauvegardés")

brute_force_ssh.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/results/brute_force_ssh/")
print(" Résultats brute force SSH sauvegardés")

spark.stop()
print(" Script Brute Force terminé !")