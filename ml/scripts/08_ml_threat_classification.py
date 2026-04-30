"""
08_ml_threat_classification.py
-------------------------------
Orchestrateur principal — entraîne, évalue et sauvegarde le modèle.

La logique métier est dans le package ml/ :
  ml/features.py   → feature engineering
  ml/models.py     → pipelines MLlib + cross-validation
  ml/evaluation.py → métriques, matrice de confusion, rapports
  ml/storage.py    → HDFS (modèle + prédictions) et HBase

Usage :
  spark-submit --master spark://spark-master:7077 /ml/08_ml_threat_classification.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import ml

# ══════════════════════════════════════════════════════════════════════════════
# Config
# ══════════════════════════════════════════════════════════════════════════════
HDFS_PATH  = "hdfs://namenode:9000/logs/cybersecurity/"
MODEL_PATH = "hdfs://namenode:9000/models/threat_classifier"
PREDS_PATH = "hdfs://namenode:9000/results/ml_predictions/"

# ══════════════════════════════════════════════════════════════════════════════
# Spark session
# ══════════════════════════════════════════════════════════════════════════════
spark = SparkSession.builder \
    .appName("MLThreatClassification") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("=" * 65)
print("  SPARK MLLIB — CLASSIFICATION DES MENACES CYBERSÉCURITÉ")
print("=" * 65)

# ══════════════════════════════════════════════════════════════════════════════
# 1. Chargement + feature engineering
# ══════════════════════════════════════════════════════════════════════════════
raw_df = spark.read.parquet(HDFS_PATH)
total  = raw_df.count()
print(f"\n Données chargées : {total:,} lignes")

print("\n Distribution des classes :")
raw_df.groupBy("threat_label").count().orderBy("count", ascending=False).show()

df = ml.clean(ml.build_features(raw_df))
print(f" Lignes après nettoyage : {df.count():,}")

# ══════════════════════════════════════════════════════════════════════════════
# 2. Split train / test
# ══════════════════════════════════════════════════════════════════════════════
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f"\n Split : {train_df.count():,} train / {test_df.count():,} test")

# ══════════════════════════════════════════════════════════════════════════════
# 3. Entraînement
# ══════════════════════════════════════════════════════════════════════════════
print("\n Entraînement Random Forest...")
rf_model = ml.train(ml.build_random_forest(), train_df)

print(" Entraînement Logistic Regression...")
lr_model = ml.train(ml.build_logistic_regression(), train_df)

# ══════════════════════════════════════════════════════════════════════════════
# 4. Évaluation
# ══════════════════════════════════════════════════════════════════════════════
preds_rf = rf_model.transform(test_df)
preds_lr = lr_model.transform(test_df)

metrics_rf = ml.compute_metrics(preds_rf)
metrics_lr = ml.compute_metrics(preds_lr)

ml.print_comparison_table(metrics_rf, metrics_lr)
ml.print_confusion_matrix(preds_rf, "Random Forest")
importances = ml.print_feature_importances(rf_model.stages[-1])

# ══════════════════════════════════════════════════════════════════════════════
# 5. Cross-validation
# ══════════════════════════════════════════════════════════════════════════════
print("\n Cross-Validation (2 folds, sous-ensemble 2%)...")
best_f1, best_params, _ = ml.cross_validate(train_df)
print(f" Meilleur F1 (CV) : {best_f1:.4f}")
print(f" Meilleurs hyperparamètres : {best_params}")

# ══════════════════════════════════════════════════════════════════════════════
# 6. Prédictions sur le dataset complet
# ══════════════════════════════════════════════════════════════════════════════
print("\n Prédictions sur le dataset complet...")
preds_full   = rf_model.transform(df)
preds_labeled = ml.add_predicted_labels(preds_full, rf_model.stages[0])

print(" Résumé des prédictions :")
preds_labeled.groupBy("predicted_label").count() \
    .orderBy("count", ascending=False).show()

# ══════════════════════════════════════════════════════════════════════════════
# 7. Sauvegarde
# ══════════════════════════════════════════════════════════════════════════════
ml.save_predictions(preds_labeled, PREDS_PATH)
ml.save_model(rf_model, MODEL_PATH)

wrong_preds = preds_labeled \
    .filter(col("threat_label") != col("predicted_label")) \
    .select("source_ip", "timestamp", "threat_label", "predicted_label") \
    .limit(50)

ml.save_to_hbase(metrics_rf, metrics_lr, importances, wrong_preds, best_f1, MODEL_PATH)

# ══════════════════════════════════════════════════════════════════════════════
# 8. Rapport final
# ══════════════════════════════════════════════════════════════════════════════
ml.print_final_report(total, metrics_rf, importances, MODEL_PATH, PREDS_PATH)

spark.stop()
print("\n Script 08 terminé avec succès !")
