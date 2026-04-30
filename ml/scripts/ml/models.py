"""
ml/models.py
------------
Construction des pipelines MLlib et entraînement des modèles.
"""

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from .features import FEATURE_COLS


def _base_stages(label_col: str = "threat_label"):
    """Retourne les stages partagés : indexer → assembler → scaler."""
    label_indexer = StringIndexer(
        inputCol=label_col,
        outputCol="label",
        handleInvalid="keep"
    )
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_raw")
    scaler    = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )
    return label_indexer, assembler, scaler


def build_random_forest():
    """Pipeline Random Forest multiclasse (modèle principal)."""
    label_indexer, assembler, scaler = _base_stages()
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=30,
        maxDepth=8,
        seed=42,
        featureSubsetStrategy="sqrt"
    )
    return Pipeline(stages=[label_indexer, assembler, scaler, rf])


def build_logistic_regression():
    """Pipeline Logistic Regression multinomiale (modèle de comparaison)."""
    label_indexer, assembler, scaler = _base_stages()
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=40,
        regParam=0.01,
        elasticNetParam=0.0,
        family="multinomial"
    )
    return Pipeline(stages=[label_indexer, assembler, scaler, lr])


def train(pipeline, train_df):
    """Entraîne un pipeline sur train_df et retourne le modèle."""
    return pipeline.fit(train_df)


def cross_validate(train_df, n_folds: int = 2, sample_frac: float = 0.02):
    """
    Cross-validation sur un sous-ensemble (pour la vitesse en local Docker).
    Retourne (best_f1, best_params, cv_model).
    """
    label_indexer, assembler, scaler = _base_stages()
    rf_cv = RandomForestClassifier(
        featuresCol="features", labelCol="label", seed=42
    )
    pipeline_cv = Pipeline(stages=[label_indexer, assembler, scaler, rf_cv])

    param_grid = (
        ParamGridBuilder()
        .addGrid(rf_cv.numTrees, [50])
        .addGrid(rf_cv.maxDepth, [5])
        .build()
    )
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1"
    )
    cv = CrossValidator(
        estimator=pipeline_cv,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=n_folds,
        seed=42
    )
    sample = train_df.sample(fraction=sample_frac, seed=42)
    cv_model   = cv.fit(sample)
    best_f1    = max(cv_model.avgMetrics)
    best_params = param_grid[cv_model.avgMetrics.index(best_f1)]
    return best_f1, best_params, cv_model
