from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, col
import subprocess

spark = SparkSession.builder \
    .appName("LoadHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(" Spark démarré")

CSV_PATH = "file:///data/cybersecurity_threat_detection_logs.csv"

df = spark.read.csv(
    CSV_PATH,
    header=True,
    inferSchema=True
)

print(f" CSV chargé : {df.count()} lignes")
print(" Colonnes disponibles :")
df.printSchema()

df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

df = df.withColumn("year",  year(col("timestamp"))) \
       .withColumn("month", month(col("timestamp"))) \
       .withColumn("day",   dayofmonth(col("timestamp")))

df_clean = df.filter(col("timestamp").isNotNull())
print(f" Lignes après nettoyage : {df_clean.count()}")


HDFS_PATH = "hdfs://namenode:9000/logs/cybersecurity/"

df_clean.write \
    .partitionBy("year", "month", "day") \
    .mode("overwrite") \
    .parquet(HDFS_PATH)

print(f" Données sauvegardées dans HDFS : {HDFS_PATH}")


print("\n Vérification depuis Spark :")
spark.read.parquet("hdfs://namenode:9000/logs/cybersecurity/").printSchema()


df_verify = spark.read.parquet(HDFS_PATH)
print(f" Vérification : {df_verify.count()} lignes dans HDFS")
print(" Exemple des données :")
df_verify.show(5)

spark.stop()
print(" Script terminé avec succès !")