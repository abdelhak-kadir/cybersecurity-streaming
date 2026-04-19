# ML Layer — Cybersecurity Threat Classification

Classification automatique des menaces avec Spark MLlib.

## Pré-requis

- La chaîne batch doit avoir été exécutée au moins une fois (`make batch-load`)
- Les données HDFS doivent être présentes dans `hdfs://namenode:9000/logs/cybersecurity/`

## Scripts

| Script | Rôle |
|--------|------|
| `08_ml_threat_classification.py` | Entraîne le modèle, évalue, sauvegarde dans HDFS + HBase |
| `09_ml_predict.py` | Recharge le modèle entraîné et génère des prédictions |

## Commandes

Entraîner le modèle :

```bash
make ml-train
```

Générer des prédictions avec le modèle sauvegardé :

```bash
make ml-predict
```

Exécuter toute la chaîne ML (train + predict) :

```bash
make ml-all
```

## Sorties produites

| Destination | Contenu |
|-------------|---------|
| `hdfs://namenode:9000/models/threat_classifier` | Modèle PipelineModel (Random Forest) |
| `hdfs://namenode:9000/results/ml_predictions/` | Prédictions du run d'entraînement |
| `hdfs://namenode:9000/results/ml_predictions_latest/` | Prédictions du dernier run de `09_` |
| HBase `ml_predictions` | Métriques, importance features, faux positifs/négatifs |

## Features utilisées (13)

- **Temporelles** : `hour_of_day`, `day_of_week`
- **Volumétriques** : `bytes_log` (log-normalisé), `path_length`, `agent_length`
- **Comportementales** : `is_blocked`, `has_sqli`, `has_xss`, `has_tool_ua`
- **Protocole** : `is_tcp`, `is_http`, `is_ssh`, `is_udp`

## Modèles entraînés

- **Random Forest** (principal) : 100 arbres, profondeur max 10, validation croisée 3-folds
- **Logistic Regression** (comparaison) : multinomiale, régularisation L2

## Rechargement du modèle

```python
from pyspark.ml import PipelineModel
model = PipelineModel.load("hdfs://namenode:9000/models/threat_classifier")
predictions = model.transform(new_data_df)
```