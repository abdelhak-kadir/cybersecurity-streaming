"""
09_ml_predict.py
----------------
Batch inference — loads the saved model from HDFS and runs predictions
on the current log dataset.  Does NOT retrain.

Workflow:
  1. Load the RF PipelineModel saved by 08_ml_threat_classification.py
  2. Read raw logs from HDFS
  3. Apply identical feature engineering (ml/features.py)
  4. Run model.transform() — no labels required
  5. If threat_label is present in the data, compute accuracy metrics
  6. Save predictions as Parquet to ml_predictions_latest/
  7. Write run summary to HBase ml_predictions table

Usage:
  spark-submit --master spark://spark-master:7077 /ml/09_ml_predict.py

Outputs:
  HDFS  → hdfs://namenode:9000/results/ml_predictions_latest/
  HBase → table ml_predictions, row key RUN_<timestamp>
"""

from datetime import datetime

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import ml

# ══════════════════════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════════════════════
HDFS_PATH  = "hdfs://namenode:9000/logs/cybersecurity/"
MODEL_PATH = "hdfs://namenode:9000/models/threat_classifier"
PREDS_PATH = "hdfs://namenode:9000/results/ml_predictions_latest/"
HBASE_HOST = "hbase"

# ══════════════════════════════════════════════════════════════════════════════
# Spark session
# ══════════════════════════════════════════════════════════════════════════════
spark = SparkSession.builder \
    .appName("MLThreatPrediction") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

print("=" * 65)
print("  SPARK MLLIB — PRÉDICTION BATCH DE MENACES")
print("=" * 65)
print(f"  Run timestamp : {run_ts}")
print(f"  Modèle        : {MODEL_PATH}")
print(f"  Sortie        : {PREDS_PATH}")

# ══════════════════════════════════════════════════════════════════════════════
# 1. Load trained model
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n Chargement du modèle depuis HDFS...")
model = PipelineModel.load(MODEL_PATH)
print(" Modèle chargé.")

# ══════════════════════════════════════════════════════════════════════════════
# 2. Load data + feature engineering
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n Chargement des données depuis : {HDFS_PATH}")
raw_df = spark.read.parquet(HDFS_PATH)
total  = raw_df.count()
print(f" Données chargées : {total:,} lignes")

df = ml.build_features(raw_df)
# Drop rows with null features only — label column not required for inference
df = df.dropna(subset=ml.FEATURE_COLS)
clean_total = df.count()
dropped = total - clean_total
if dropped:
    print(f" Lignes ignorées (features nulles) : {dropped:,}")
print(f" Lignes prêtes pour l'inférence    : {clean_total:,}")

# ══════════════════════════════════════════════════════════════════════════════
# 3. Predict
# ══════════════════════════════════════════════════════════════════════════════
print("\n Application du modèle...")
preds = model.transform(df)
preds = ml.add_predicted_labels(preds, model.stages[0])

print("\n Distribution des prédictions :")
preds.groupBy("predicted_label").count() \
    .orderBy("count", ascending=False).show()

# ══════════════════════════════════════════════════════════════════════════════
# 4. Evaluate — only when ground-truth labels are present
# ══════════════════════════════════════════════════════════════════════════════
has_labels = (
    "threat_label" in raw_df.columns
    and raw_df.filter(col("threat_label").isNotNull()).limit(1).count() > 0
)

metrics = None
if has_labels:
    # Re-apply StringIndexer labels so 'label' column exists for evaluation
    labeled = preds.filter(col("threat_label").isNotNull())
    if labeled.count() > 0:
        metrics = ml.compute_metrics(labeled)
        print("\n Métriques sur les lignes avec labels :")
        print(f"  Accuracy  : {metrics['accuracy'] * 100:.2f}%")
        print(f"  F1-Score  : {metrics['f1']:.4f}")
        print(f"  Precision : {metrics['precision']:.4f}")

        wrong = labeled \
            .filter(col("threat_label") != col("predicted_label")) \
            .select("source_ip", "timestamp", "threat_label", "predicted_label") \
            .limit(20)
        print(f"\n Échantillon de prédictions incorrectes :")
        wrong.show(truncate=False)
else:
    print("\n (Données sans labels — évaluation ignorée)")

# ══════════════════════════════════════════════════════════════════════════════
# 5. Save predictions to HDFS
# ══════════════════════════════════════════════════════════════════════════════
ml.save_predictions(preds, PREDS_PATH)

# ══════════════════════════════════════════════════════════════════════════════
# 6. Save run summary to HBase
# ══════════════════════════════════════════════════════════════════════════════
ml.save_prediction_run_to_hbase(
    preds_df=preds,
    total_rows=clean_total,
    run_ts=run_ts,
    model_path=MODEL_PATH,
    metrics=metrics,
    hbase_host=HBASE_HOST,
)

# ══════════════════════════════════════════════════════════════════════════════
# 7. Final report
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 65)
print("  RAPPORT — PRÉDICTION BATCH")
print("=" * 65)
print(f"  Lignes traitées   : {clean_total:,}")
print(f"  Modèle utilisé    : {MODEL_PATH}")
print(f"  Prédictions HDFS  : {PREDS_PATH}")
if metrics:
    print(f"  Accuracy          : {metrics['accuracy'] * 100:.2f}%")
    print(f"  F1-Score          : {metrics['f1']:.4f}")
print("=" * 65)

spark.stop()
print("\n Script 09 terminé avec succès !")
