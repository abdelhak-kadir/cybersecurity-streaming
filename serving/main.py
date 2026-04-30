from contextlib import asynccontextmanager
from datetime import datetime
import json
import os
import time as _time
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
import requests as _requests

from db.cassandra_client import CassandraClient
from db.hbase_client import HBaseClient
from models.schemas import (
    AdaptiveScore,
    AttackPattern,
    CorrelatedAttack,
    GeoThreat,
    HealthResponse,
    IPReputationResponse,
    LiveThreat,
    LiveThreatsResponse,
    MLFeatureImportance,
    MLMetricPoint,
    MLPredictionCount,
    MLSummary,
    RecentEvent,
    ThreatTimelinePoint,
    ThreatVolumePoint,
    TopIP,
)

cassandra: Optional[CassandraClient] = None
hbase: Optional[HBaseClient] = None
_geo_cache: dict = {}
_geo_cache_time: float = 0.0
_GEO_TTL = 3600
ML_SUMMARY_PATH = os.getenv("ML_SUMMARY_PATH", "/data/ml_summary.json")


def load_ml_summary() -> MLSummary:
    if not os.path.exists(ML_SUMMARY_PATH):
        return MLSummary(status="not_trained")
    try:
        with open(ML_SUMMARY_PATH, encoding="utf-8") as fh:
            return MLSummary(**json.load(fh))
    except Exception as exc:
        print(f"[ml] summary unavailable: {exc}")
        return MLSummary(status="unavailable")


def geolocate_batch(ips: list) -> dict:
    global _geo_cache_time
    now = _time.time()
    uncached = [ip for ip in ips if ip not in _geo_cache]
    if uncached and (now - _geo_cache_time > _GEO_TTL):
        try:
            response = _requests.post(
                "http://ip-api.com/batch",
                json=[
                    {"query": ip, "fields": "query,lat,lon,country,city,status"}
                    for ip in uncached
                ],
                timeout=5,
            )
            response.raise_for_status()
            for entry in response.json():
                if entry.get("status") == "success":
                    _geo_cache[entry["query"]] = entry
            _geo_cache_time = now
        except Exception as exc:
            print(f"[geo] lookup failed: {exc}")
    return {ip: _geo_cache.get(ip, {}) for ip in ips}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global cassandra, hbase
    try:
        cassandra = CassandraClient()
    except Exception as exc:
        print(f"[startup] Cassandra unavailable: {exc} — /health will report degraded")
        cassandra = None
    try:
        hbase = HBaseClient()
    except Exception as exc:
        print(f"[startup] HBase unavailable: {exc} — /health will report degraded")
        hbase = None
    yield
    if cassandra:
        cassandra.close()
    if hbase:
        hbase.close()


app = FastAPI(title="CyberSecurity Serving Layer", version="1.0.0", lifespan=lifespan)


@app.get("/health", response_model=HealthResponse)
def health():
    cass_ok = cassandra.ping() if cassandra else False
    hbase_ok = hbase.ping() if hbase else False
    return HealthResponse(
        cassandra=cass_ok,
        hbase=hbase_ok,
        status="ok" if cass_ok and hbase_ok else "degraded",
    )


@app.get("/api/ip/{ip}", response_model=IPReputationResponse)
def get_ip_reputation(ip: str):
    # Batch layer (HBase)
    batch = hbase.get_ip_reputation(ip) if hbase else None
    batch_score = float(batch.get("data:reputation_score", 0)) if batch else 0.0
    batch_attacks = int(batch.get("data:nb_malicious", 0)) if batch else 0
    batch_types: set[str] = set()
    if batch and batch.get("data:attack_type"):
        batch_types = {t.strip() for t in batch["data:attack_type"].split(",") if t.strip()}

    # Real-time layer (Cassandra)
    summary = cassandra.get_ip_summary(ip) if cassandra else None
    realtime_types: set[str] = set(summary.attack_types or set()) if summary else set()
    # Derive score and count from realtime_threats (bounded by 24h TTL, idempotent).
    # This avoids the score-regression problem that arises when overwriting a summary
    # row with a lower-severity event that arrives out of order.
    recent_rows = cassandra.get_ip_recent_events(ip, limit=100) if cassandra else []
    realtime_score = max((r.threat_score or 0) for r in recent_rows) if recent_rows else 0
    alert_count = cassandra.get_ip_alert_count(ip) if cassandra else 0

    recent_events = [
        RecentEvent(
            last_seen=r.last_seen,
            threat_score=r.threat_score,
            attack_type=r.attack_type,
        )
        for r in recent_rows[:10]
    ]

    return IPReputationResponse(
        ip=ip,
        merged_reputation_score=max(batch_score, realtime_score or 0),
        attack_types=sorted(batch_types | realtime_types),
        total_realtime_alerts=alert_count,
        nb_batch_attacks=batch_attacks,
        last_seen=str(summary.last_seen) if summary and summary.last_seen else None,
        recent_events=recent_events,
        batch_data=batch,
    )


@app.get("/api/threats/live", response_model=LiveThreatsResponse)
def get_live_threats(
    minutes: int = Query(60, ge=1, le=1440),
    limit: int = Query(100, ge=1, le=1000),
    attack_type: Optional[str] = None,
    min_score: int = Query(0, ge=0, le=100),
):
    if not cassandra:
        raise HTTPException(status_code=503, detail="Cassandra unavailable")
    rows = cassandra.get_live_threats(
        minutes=minutes, limit=limit, attack_type=attack_type, min_score=min_score
    )
    threats = [
        LiveThreat(
            ip_source=r.ip_source,
            last_seen=r.last_seen,
            threat_score=r.threat_score,
            attack_type=r.attack_type,
        )
        for r in rows
    ]
    if not threats and hbase and attack_type is None:
        now = datetime.utcnow()
        threats = [
            LiveThreat(
                ip_source=row["ip"],
                last_seen=now,
                threat_score=int(row["reputation_score"]),
                attack_type="historical-malicious",
            )
            for row in hbase.get_top_ips(limit=min(limit, 100))
            if int(row["reputation_score"]) >= min_score
        ]
    return LiveThreatsResponse(threats=threats, count=len(threats))


@app.get("/api/threats/correlated", response_model=list[CorrelatedAttack])
def get_correlated_attacks(
    minutes: int = Query(60, ge=1, le=1440),
    limit: int = Query(100, ge=1, le=1000),
):
    if not cassandra:
        raise HTTPException(status_code=503, detail="Cassandra unavailable")
    rows = cassandra.get_correlated_attacks(minutes=minutes, limit=limit)
    return [
        CorrelatedAttack(
            ip_source=r.ip_source,
            first_seen=r.first_seen,
            last_seen=r.last_seen,
            stages=sorted(r.stages or []),
            threat_score=r.threat_score,
        )
        for r in rows
    ]


def _int_value(data: Optional[dict], key: str) -> int:
    if not data:
        return 0
    try:
        return int(float(data.get(key, 0) or 0))
    except (TypeError, ValueError):
        return 0


@app.get("/api/scoring/adaptive", response_model=list[AdaptiveScore])
def get_adaptive_scores(
    minutes: int = Query(60, ge=1, le=1440),
    limit: int = Query(50, ge=1, le=200),
):
    if not cassandra:
        raise HTTPException(status_code=503, detail="Cassandra unavailable")

    query_limit = min(max(limit * 100, 1000), 10000)
    rows = cassandra.get_recent_threats(minutes=minutes, limit=query_limit)
    grouped: dict[str, list] = {}
    for row in rows:
        grouped.setdefault(row.ip_source, []).append(row)

    scores = []
    for ip, events in grouped.items():
        base_score = max((event.threat_score or 0) for event in events)
        attack_types = sorted({event.attack_type for event in events if event.attack_type})
        alert_count = len(events)
        last_seen = max(
            (event.last_seen for event in events if event.last_seen),
            default=None,
        )

        boost = 0
        reasons = []
        if alert_count >= 5:
            boost += 10
            reasons.append("5+ recent alerts")
        if len(attack_types) >= 3:
            boost += 10
            reasons.append("3+ attack types")

        batch = hbase.get_ip_reputation(ip) if hbase else None
        batch_score = _int_value(batch, "data:reputation_score")
        batch_malicious = _int_value(batch, "data:nb_malicious")
        if batch_score >= 80 or batch_malicious > 0:
            boost += 15
            reasons.append("known malicious in batch")

        adaptive_score = min(100, base_score + boost)
        scores.append(
            AdaptiveScore(
                ip_source=ip,
                base_score=base_score,
                adaptive_score=adaptive_score,
                score_delta=adaptive_score - base_score,
                reasons=reasons,
                attack_types=attack_types,
                alert_count=alert_count,
                last_seen=last_seen,
            )
        )

    scores.sort(
        key=lambda item: (
            item.adaptive_score,
            item.score_delta,
            item.last_seen or datetime.min,
        ),
        reverse=True,
    )
    return scores[:limit]


@app.get("/api/stats/top-ips", response_model=list[TopIP])
def get_top_ips(limit: int = Query(10, ge=1, le=50)):
    if not hbase:
        raise HTTPException(status_code=503, detail="HBase unavailable")
    rows = hbase.get_top_ips(limit=limit)
    return [
        TopIP(
            ip=r["ip"],
            reputation_score=r["reputation_score"],
            nb_malicious=r["nb_malicious"],
            nb_suspicious=r["nb_suspicious"],
        )
        for r in rows
    ]


@app.get("/api/stats/geo-threats", response_model=list[GeoThreat])
def get_geo_threats(limit: int = Query(20, ge=1, le=50)):
    if not hbase:
        raise HTTPException(status_code=503, detail="HBase unavailable")
    top_ips = hbase.get_top_ips(limit=limit)
    geo = geolocate_batch([r["ip"] for r in top_ips])
    result = []
    for row in top_ips:
        geo_row = geo.get(row["ip"], {})
        if geo_row.get("lat") is not None and geo_row.get("lon") is not None:
            result.append(
                GeoThreat(
                    ip=row["ip"],
                    lat=geo_row["lat"],
                    lon=geo_row["lon"],
                    country=geo_row.get("country", ""),
                    city=geo_row.get("city", ""),
                    reputation_score=row["reputation_score"],
                    nb_malicious=row["nb_malicious"],
                )
            )
    return result


@app.get("/api/stats/attack-patterns", response_model=list[AttackPattern])
def get_attack_patterns(
    attack_type: Optional[str] = Query(None, description="SQLi | XSS | port_scan"),
    limit: int = Query(50, ge=1, le=200),
):
    if not hbase:
        raise HTTPException(status_code=503, detail="HBase unavailable")
    rows = hbase.get_attack_patterns(attack_type=attack_type, limit=limit)
    return [AttackPattern(key=r["key"], data=r["data"]) for r in rows]


@app.get("/api/stats/threat-timeline", response_model=list[ThreatTimelinePoint])
def get_threat_timeline(
    days: int = Query(30, ge=1, le=365),
    threat_label: Optional[str] = Query(None, description="malicious | suspicious"),
):
    if not hbase:
        raise HTTPException(status_code=503, detail="HBase unavailable")
    rows = hbase.get_threat_timeline(days=days, threat_label=threat_label)
    return [
        ThreatTimelinePoint(date=r["date"], threat_label=r["threat_label"], count=r["count"])
        for r in rows
    ]


@app.get("/api/stats/threat-volume", response_model=list[ThreatVolumePoint])
def get_threat_volume(limit: int = Query(50, ge=1, le=200)):
    if not hbase:
        raise HTTPException(status_code=503, detail="HBase unavailable")
    rows = hbase.get_threat_volume(limit=limit)
    return [ThreatVolumePoint(threat_label=r["threat_label"], total_bytes=r["total_bytes"]) for r in rows]


@app.get("/api/ml/summary", response_model=MLSummary)
def get_ml_summary():
    return load_ml_summary()


@app.get("/api/ml/metrics", response_model=list[MLMetricPoint])
def get_ml_metrics():
    return load_ml_summary().metrics


@app.get("/api/ml/prediction-counts", response_model=list[MLPredictionCount])
def get_ml_prediction_counts():
    return load_ml_summary().prediction_counts


@app.get("/api/ml/feature-importance", response_model=list[MLFeatureImportance])
def get_ml_feature_importance(limit: int = Query(13, ge=1, le=50)):
    return load_ml_summary().feature_importance[:limit]
