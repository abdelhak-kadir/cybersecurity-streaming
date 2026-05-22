# ML Layer — Cybersecurity Threat Classification

Automatic threat classification using Spark MLlib, trained on 6 million network log events.

---

## How It Works

### The Goal

Every network log event in HDFS has a `threat_label`: `benign`, `suspicious`, or `malicious`. The ML layer trains a classifier to **predict that label automatically** from the raw log fields — so that future, unlabeled traffic can be scored in real time without relying on hard-coded rules.

### Pipeline Overview

```
HDFS parquet logs
       ↓
  Feature Engineering        (ml/features.py)
  13 numeric columns derived from raw fields
       ↓
  StringIndexer              label "benign/suspicious/malicious" → 0.0/1.0/2.0
  VectorAssembler            13 features → single vector column
  StandardScaler             zero mean, unit variance
       ↓
  RandomForestClassifier     30 trees, max depth 8
       ↓
  Predictions + Metrics
       ↓
  HDFS (model + predictions) + HBase (metrics + feature importances)
```

### The 13 Features Explained

The raw log fields (IP addresses, timestamps, strings) cannot be fed directly into a ML model. `features.py` derives 13 numeric columns from them:

| Feature | Derived from | What it captures |
|---|---|---|
| `hour_of_day` | `timestamp` | Attacks peak at unusual hours |
| `day_of_week` | `timestamp` | Weekend traffic patterns differ |
| `bytes_log` | `bytes_transferred` | Log-normalised transfer size (reduces outlier skew) |
| `path_length` | `request_path` | Malicious paths tend to be long (SQLi payloads, encoded URLs) |
| `agent_length` | `user_agent` | Attack tool agents differ in length from browser agents |
| `is_blocked` | `action` | 1.0 if the firewall/IDS blocked the request |
| `has_sqli` | `request_path` | 1.0 if path matches SQL injection patterns (`union`, `select`, `drop`…) |
| `has_xss` | `request_path` | 1.0 if path matches XSS patterns (`<script>`, `alert`, `iframe`…) |
| `has_tool_ua` | `user_agent` | 1.0 if user-agent matches known attack tools (`sqlmap`, `nikto`, `nmap`…) |
| `is_tcp` | `protocol` | 1.0 if TCP |
| `is_http` | `protocol` | 1.0 if HTTP |
| `is_ssh` | `protocol` | 1.0 if SSH |
| `is_udp` | `protocol` | 1.0 if UDP |

### What the Model Learned (Actual Results)

Training on 4.8M events, evaluated on 1.2M:

| Model | Accuracy | F1-Score | Precision |
|---|---|---|---|
| **Random Forest** | **97.11%** | **0.9684** | **0.9693** |
| Logistic Regression | 95.57% | 0.9477 | 0.9545 |

**Feature importances (Random Forest):**

| Rank | Feature | Importance | Note |
|---|---|---|---|
| 1 | `path_length` | 76.5% | Dominant signal — long paths = attack payloads |
| 2 | `has_sqli` | 10.6% | SQLi pattern match |
| 3 | `is_tcp` | 6.0% | Protocol type |
| 4 | `is_udp` | 4.1% | Protocol type |
| 5 | `is_ssh` | 1.9% | SSH connections |
| 6–13 | others | ~0% | Unused by the forest on this dataset |

**Class distribution and imbalance:**

The dataset is heavily imbalanced — 92% benign, 6% suspicious, 2% malicious. The model classifies benign traffic nearly perfectly, but misses ~38% of actual malicious events (they get predicted as benign or suspicious). This is the main known limitation.

---

## Scripts

| Script | Role |
|---|---|
| `08_ml_threat_classification.py` | Orchestrator: loads data, engineers features, trains RF + LR, evaluates, cross-validates, saves model + predictions to HDFS and metrics to HBase |
| `09_ml_predict.py` | Loads the saved PipelineModel from HDFS and scores new data |
| `ml/features.py` | All feature engineering logic — `build_features()` and `clean()` |
| `ml/models.py` | MLlib pipeline construction for RF and LR, plus cross-validation |
| `ml/evaluation.py` | Metrics, confusion matrix, feature importance display |
| `ml/storage.py` | Saves model to HDFS, predictions to parquet, metrics to HBase |

---

## Setup

### Prerequisites

1. The full stack must be running with the batch profile active:

```bash
make batch
```

2. Historical data must be loaded into HDFS (runs `01_load_hdfs.py`):

```bash
make batch-load
```

3. Verify the data is present before training:

```bash
docker compose exec spark-batch hdfs dfs -ls hdfs://namenode:9000/logs/cybersecurity/
```

You should see partitioned parquet directories (`year=2024/month=1/...`).

### Train the model

```bash
make ml-train
```

This runs `08_ml_threat_classification.py` via `spark-submit` inside the `spark-batch` container. Expected runtime: 10–20 minutes on a 2-worker local cluster with 6M rows.

What it produces:

| Output | Location |
|---|---|
| Trained PipelineModel | `hdfs://namenode:9000/models/threat_classifier` |
| Predictions (full dataset) | `hdfs://namenode:9000/results/ml_predictions/` |
| Metrics + feature importances | HBase table `ml_predictions` |

### Run predictions on new data

```bash
make ml-predict
```

This runs `09_ml_predict.py`, which reloads the saved model and scores data, writing results to:

```
hdfs://namenode:9000/results/ml_predictions_latest/
```

### Run the full ML chain

```bash
make ml-all
```

Equivalent to `make ml-train` followed by `make ml-predict`.

---

## Reload the Model in Your Own Script

```python
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Load the saved pipeline (includes StringIndexer + Scaler + RF)
model = PipelineModel.load("hdfs://namenode:9000/models/threat_classifier")

# Score any DataFrame that has the same raw columns as the training data
predictions = model.transform(new_data_df)
predictions.select("source_ip", "predicted_label", "probability").show()
```

The pipeline is self-contained — it includes the StringIndexer, VectorAssembler, StandardScaler, and RandomForestClassifier. You only need to pass a DataFrame with the original raw columns (`timestamp`, `bytes_transferred`, `request_path`, `user_agent`, `action`, `protocol`).

---

## Known Limitations

- **Class imbalance**: 92% of events are benign. The model misses ~38% of malicious events. To improve minority class recall, add inverse-frequency class weights to the `RandomForestClassifier` via a `weightCol`.
- **`path_length` dominance**: 76.5% of the model's decisions rely on a single feature. On real-world traffic with more varied path lengths, accuracy may degrade. Consider adding more behavioural features.
- **No temporal generalisation**: The model is trained and tested on data from the same time period. It has not been validated on future traffic distributions.
