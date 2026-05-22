from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class HealthResponse(BaseModel):
    cassandra: bool
    hbase: bool
    status: str


class RecentEvent(BaseModel):
    last_seen: Optional[datetime]
    threat_score: Optional[int]
    attack_type: Optional[str]


class BlockRecommendation(BaseModel):
    action: str
    reason: str


class IPReputationResponse(BaseModel):
    ip: str
    merged_reputation_score: float
    attack_types: list[str]
    total_realtime_alerts: int
    nb_batch_attacks: int
    last_seen: Optional[str]
    recommendation: BlockRecommendation
    recent_events: list[RecentEvent]
    batch_data: Optional[dict]


class LiveThreat(BaseModel):
    ip_source: str
    last_seen: Optional[datetime]
    threat_score: Optional[int]
    attack_type: Optional[str]


class LiveThreatsResponse(BaseModel):
    threats: list[LiveThreat]
    count: int


class CorrelatedAttack(BaseModel):
    ip_source: str
    first_seen: Optional[datetime]
    last_seen: Optional[datetime]
    stages: list[str]
    threat_score: int


class AdaptiveScore(BaseModel):
    ip_source: str
    base_score: int
    adaptive_score: int
    score_delta: int
    reasons: list[str]
    attack_types: list[str]
    alert_count: int
    last_seen: Optional[datetime]


class TopIP(BaseModel):
    ip: str
    reputation_score: float
    nb_malicious: int
    nb_suspicious: int


class GeoThreat(BaseModel):
    ip: str
    lat: float
    lon: float
    country: str
    city: str
    reputation_score: float
    nb_malicious: int


class AttackPattern(BaseModel):
    key: str
    data: dict


class ThreatTimelinePoint(BaseModel):
    date: str
    threat_label: str
    count: int


class ThreatVolumePoint(BaseModel):
    threat_label: str
    total_bytes: float


class AttackByProtocol(BaseModel):
    protocol: str
    threat_label: str
    nb_events: int
    total_bytes: float


class MLMetricPoint(BaseModel):
    model: str
    accuracy: float
    f1_score: float
    precision: float


class MLPredictionCount(BaseModel):
    predicted_label: str
    count: int


class MLFeatureImportance(BaseModel):
    feature: str
    importance: float


class MLSummary(BaseModel):
    model_config = {"protected_namespaces": ()}

    status: str
    trained_at: Optional[str] = None
    dataset_rows: int = 0
    train_rows: int = 0
    test_rows: int = 0
    model_path: Optional[str] = None
    predictions_path: Optional[str] = None
    cv_best_f1: Optional[float] = None
    metrics: list[MLMetricPoint] = Field(default_factory=list)
    prediction_counts: list[MLPredictionCount] = Field(default_factory=list)
    feature_importance: list[MLFeatureImportance] = Field(default_factory=list)
