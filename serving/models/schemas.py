from pydantic import BaseModel
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


class IPReputationResponse(BaseModel):
    ip: str
    merged_reputation_score: float
    attack_types: list[str]
    total_realtime_alerts: int
    nb_batch_attacks: int
    last_seen: Optional[str]
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
