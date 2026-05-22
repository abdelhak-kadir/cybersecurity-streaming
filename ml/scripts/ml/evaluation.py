"""
ml/evaluation.py
----------------
Évaluation des modèles : métriques, matrice de confusion,
importance des features, rapport final.
"""

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import IndexToString
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from .features import FEATURE_COLS



def compute_metrics(predictions: DataFrame) -> dict:
    """Retourne accuracy, f1, precision pour un DataFrame de prédictions."""
    evaluator_acc  = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    )
    evaluator_f1   = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1"
    )
    evaluator_prec = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedPrecision"
    )
    return {
        "accuracy":  evaluator_acc.evaluate(predictions),
        "f1":        evaluator_f1.evaluate(predictions),
        "precision": evaluator_prec.evaluate(predictions),
    }


def print_confusion_matrix(predictions: DataFrame, model_name: str = ""):
    """Affiche la matrice de confusion groupée par label/prediction."""
    header = f" Matrice de confusion{' — ' + model_name if model_name else ''}"
    print("\n" + header)
    predictions.groupBy("label", "prediction").count() \
        .orderBy("label", "prediction").show()


def print_feature_importances(rf_model) -> list:
    """
    Affiche et retourne l'importance des features triée par score décroissant.
    rf_model doit être le dernier stage d'un PipelineModel Random Forest.
    Retourne une liste de tuples (feature_name, importance).
    """
    importances = sorted(
        zip(FEATURE_COLS, rf_model.featureImportances.toArray()),
        key=lambda x: x[1],
        reverse=True
    )
    print("\n Importance des features (Random Forest) :")
    print(f"  {'Feature':<25} {'Importance':>10}")
    print("  " + "-" * 37)
    for feat, imp in importances:
        bar = "█" * int(imp * 50)
        print(f"  {feat:<25} {imp:>10.4f}  {bar}")
    return importances


def add_predicted_labels(predictions: DataFrame, label_model) -> DataFrame:
    """
    Convertit la colonne numérique 'prediction' en label lisible.
    label_model = StringIndexerModel (stages[0] du PipelineModel).
    """
    index_to_str = IndexToString(
        inputCol="prediction",
        outputCol="predicted_label",
        labels=label_model.labels
    )
    return index_to_str.transform(predictions)


def print_comparison_table(metrics_rf: dict, metrics_lr: dict):
    """Affiche le tableau comparatif RF vs LR."""
    print("\n" + "=" * 65)
    print("   RÉSULTATS D'ÉVALUATION (jeu de test 20%)")
    print("=" * 65)
    print(f"{'Modèle':<30} {'Accuracy':>10} {'F1-Score':>10} {'Precision':>10}")
    print("-" * 65)
    print(f"{'Random Forest (30 arbres)':<30} "
          f"{metrics_rf['accuracy']:>10.4f} "
          f"{metrics_rf['f1']:>10.4f} "
          f"{metrics_rf['precision']:>10.4f}")
    print(f"{'Logistic Regression':<30} "
          f"{metrics_lr['accuracy']:>10.4f} "
          f"{metrics_lr['f1']:>10.4f} "
          f"{metrics_lr['precision']:>10.4f}")
    print("=" * 65)


def print_final_report(total: int, metrics: dict, importances: list,
                       model_path: str, preds_path: str):
    """Affiche le rapport final récapitulatif."""
    print("\n" + "=" * 65)
    print("   RAPPORT FINAL — ML THREAT CLASSIFICATION")
    print("=" * 65)
    print(f" Dataset                  : {total:,} événements")
    print(f" Features utilisées       : {len(FEATURE_COLS)}")
    print(f" Modèle choisi            : Random Forest (meilleur F1)")
    print(f" Accuracy                 : {metrics['accuracy']*100:.2f}%")
    print(f" F1-Score (weighted)      : {metrics['f1']:.4f}")
    print(f" Precision (weighted)     : {metrics['precision']:.4f}")
    print(f" Meilleure feature        : {importances[0][0]} ({importances[0][1]:.4f})")
    print(f" Modèle HDFS              : {model_path}")
    print(f" Prédictions HDFS         : {preds_path}")
    print("=" * 65)