"""
ml/storage.py
-------------
Persistence : sauvegarde du modèle dans HDFS
et des métriques / prédictions dans HBase.
"""

import happybase
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def save_predictions(df: DataFrame, path: str):
    """Sauvegarde le DataFrame de prédictions en parquet sur HDFS."""
    to_save = df.select(
        "timestamp", "source_ip", "dest_ip", "protocol",
        "threat_label", "predicted_label",
        col("probability").cast("string").alias("probabilities"),
        "prediction"
    )
    to_save.write.mode("overwrite").parquet(path)
    print(f" Prédictions sauvegardées : {path}")


def save_model(pipeline_model, path: str):
    """Sauvegarde le PipelineModel entraîné dans HDFS."""
    pipeline_model.write().overwrite().save(path)
    print(f" Modèle sauvegardé : {path}")
    print(f"  → Recharger avec : PipelineModel.load('{path}')")


def save_to_hbase(metrics_rf: dict, metrics_lr: dict,
                  importances: list, wrong_preds: DataFrame,
                  best_f1: float, model_path: str,
                  hbase_host: str = "hbase"):
    """
    Stocke dans HBase (table ml_predictions) :
      - métriques des deux modèles
      - importance des features
      - échantillon de prédictions incorrectes
    """
    try:
        connection = happybase.Connection(host=hbase_host, port=9090)
        tables = [t.decode() for t in connection.tables()]

        if "ml_predictions" not in tables:
            connection.create_table("ml_predictions", {
                "metrics":  dict(),
                "features": dict(),
                "sample":   dict(),
            })
            print(" Table ml_predictions créée dans HBase")

        table = connection.table("ml_predictions")

        # ── Métriques Random Forest ────────────────────────────────
        table.put(b"MODEL_RF", {
            b"metrics:accuracy":   f"{metrics_rf['accuracy']:.6f}".encode(),
            b"metrics:f1_score":   f"{metrics_rf['f1']:.6f}".encode(),
            b"metrics:precision":  f"{metrics_rf['precision']:.6f}".encode(),
            b"metrics:cv_best_f1": f"{best_f1:.6f}".encode(),
            b"metrics:model_type": b"RandomForestClassifier",
            b"metrics:num_trees":  b"100",
            b"metrics:max_depth":  b"10",
            b"metrics:model_path": model_path.encode(),
        })

        # ── Métriques Logistic Regression ─────────────────────────
        table.put(b"MODEL_LR", {
            b"metrics:accuracy":   f"{metrics_lr['accuracy']:.6f}".encode(),
            b"metrics:f1_score":   f"{metrics_lr['f1']:.6f}".encode(),
            b"metrics:precision":  f"{metrics_lr['precision']:.6f}".encode(),
            b"metrics:model_type": b"LogisticRegression",
            b"metrics:max_iter":   b"100",
            b"metrics:reg_param":  b"0.01",
        })

        # ── Importance des features ────────────────────────────────
        for feat, imp in importances:
            table.put(
                f"FEATURE_{feat}".encode(),
                {
                    b"features:name":       feat.encode(),
                    b"features:importance": f"{imp:.6f}".encode(),
                }
            )

        # ── Échantillon de faux positifs / négatifs ────────────────
        for i, row in enumerate(wrong_preds.collect()):
            rk = f"WRONG_{i:04d}_{row['source_ip']}".encode()
            table.put(rk, {
                b"sample:source_ip":       str(row["source_ip"]).encode(),
                b"sample:timestamp":       str(row["timestamp"]).encode(),
                b"sample:true_label":      str(row["threat_label"]).encode(),
                b"sample:predicted_label": str(row["predicted_label"]).encode(),
            })

        connection.close()
        print(" HBase mis à jour : table ml_predictions")

    except Exception as e:
        print(f" Attention: HBase inaccessible ({e})")
        print("  → Les prédictions restent disponibles dans HDFS")