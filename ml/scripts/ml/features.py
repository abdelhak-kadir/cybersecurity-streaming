"""
ml/features.py
--------------
Feature engineering : transforme les colonnes brutes du dataset
en features numériques exploitables par MLlib.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, length, lower, when, log1p, hour, dayofweek
)

# ── Patterns de détection ──────────────────────────────────────────────────
SQLI_PATTERN  = r"(union|select|insert|drop|sleep|benchmark|--|')"
XSS_PATTERN   = r"(script|alert|iframe|javascript|eval|onerror)"
TOOLS_PATTERN = r"(sqlmap|nikto|nmap|dirbuster|masscan)"

# ── Liste des colonnes features (ordre important pour VectorAssembler) ────
FEATURE_COLS = [
    "hour_of_day",
    "day_of_week",
    "bytes_log",
    "path_length",
    "agent_length",
    "is_blocked",
    "has_sqli",
    "has_xss",
    "has_tool_ua",
    "is_tcp",
    "is_http",
    "is_ssh",
    "is_udp",
]


def build_features(df: DataFrame) -> DataFrame:
    """
    Ajoute toutes les colonnes features au DataFrame.
    Entrée  : DataFrame brut depuis HDFS (parquet)
    Sortie  : DataFrame enrichi avec les colonnes de FEATURE_COLS
    """
    return (
        df
        # ── Temporelles ───────────────────────────────────────────────
        .withColumn("hour_of_day",  hour(col("timestamp")))
        .withColumn("day_of_week",  dayofweek(col("timestamp")))

        # ── Volumétriques ─────────────────────────────────────────────
        .withColumn("bytes_log",    log1p(col("bytes_transferred").cast("double")))
        .withColumn("path_length",  length(col("request_path")).cast("double"))
        .withColumn("agent_length", length(col("user_agent")).cast("double"))

        # ── Comportementales ──────────────────────────────────────────
        .withColumn("is_blocked",
            when(col("action") == "blocked", 1.0).otherwise(0.0))
        .withColumn("has_sqli",
            when(lower(col("request_path")).rlike(SQLI_PATTERN), 1.0).otherwise(0.0))
        .withColumn("has_xss",
            when(lower(col("request_path")).rlike(XSS_PATTERN), 1.0).otherwise(0.0))
        .withColumn("has_tool_ua",
            when(lower(col("user_agent")).rlike(TOOLS_PATTERN), 1.0).otherwise(0.0))

        # ── Protocole ─────────────────────────────────────────────────
        .withColumn("is_tcp",  when(col("protocol") == "TCP",  1.0).otherwise(0.0))
        .withColumn("is_http", when(col("protocol") == "HTTP", 1.0).otherwise(0.0))
        .withColumn("is_ssh",  when(col("protocol") == "SSH",  1.0).otherwise(0.0))
        .withColumn("is_udp",  when(col("protocol") == "UDP",  1.0).otherwise(0.0))
    )


def clean(df: DataFrame) -> DataFrame:
    """Supprime les lignes inutilisables (label ou features nuls)."""
    return df.dropna(subset=FEATURE_COLS + ["threat_label"])