"""
Unit tests for the FastAPI serving layer.
All Cassandra and HBase I/O is mocked — no live infrastructure needed.
"""
import types
from contextlib import contextmanager
from datetime import datetime
import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ── Helpers ───────────────────────────────────────────────────────────────

def row(**kwargs):
    """Tiny stand-in for a cassandra-driver Row namedtuple."""
    return types.SimpleNamespace(**kwargs)


def make_clients(
    cass_ping=True,
    hbase_ping=True,
    ip_summary=None,
    alert_count=0,
    recent_events=None,
    live_threats=None,
    correlated_attacks=None,
    ip_reputation=None,
    top_ips=None,
    attack_patterns=None,
    threat_timeline=None,
    threat_volume=None,
):
    mock_cass = MagicMock()
    mock_cass.ping.return_value = cass_ping
    mock_cass.get_ip_summary.return_value = ip_summary
    mock_cass.get_ip_alert_count.return_value = alert_count
    mock_cass.get_ip_recent_events.return_value = recent_events or []
    mock_cass.get_live_threats.return_value = live_threats or []
    mock_cass.get_correlated_attacks.return_value = correlated_attacks or []

    mock_hbase = MagicMock()
    mock_hbase.ping.return_value = hbase_ping
    mock_hbase.get_ip_reputation.return_value = ip_reputation
    mock_hbase.get_top_ips.return_value = top_ips or []
    mock_hbase.get_attack_patterns.return_value = attack_patterns or []
    mock_hbase.get_threat_timeline.return_value = threat_timeline or []
    mock_hbase.get_threat_volume.return_value = threat_volume or []

    return mock_cass, mock_hbase


@pytest.fixture()
def client_factory():
    """
    Returns a context-manager factory.  Usage in tests:

        with client_factory(cass_ping=False) as c:
            r = c.get("/health")

    The `with TestClient(app) as c:` pattern is required so FastAPI's lifespan
    runs startup (which sets the cassandra/hbase module globals to the mocks).
    """
    @contextmanager
    def _make(**kwargs):
        mock_cass, mock_hbase = make_clients(**kwargs)
        import main as app_module
        with (
            patch("main.CassandraClient", return_value=mock_cass),
            patch("main.HBaseClient", return_value=mock_hbase),
        ):
            with TestClient(app_module.app, raise_server_exceptions=True) as c:
                c.mock_cass = mock_cass
                c.mock_hbase = mock_hbase
                yield c

    return _make


# ── /health ───────────────────────────────────────────────────────────────

def test_health_both_ok(client_factory):
    with client_factory(cass_ping=True, hbase_ping=True) as c:
        r = c.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["cassandra"] is True
        assert body["hbase"] is True
        assert body["status"] == "ok"


def test_health_cassandra_down(client_factory):
    with client_factory(cass_ping=False, hbase_ping=True) as c:
        r = c.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["cassandra"] is False
        assert body["status"] == "degraded"


def test_health_hbase_down(client_factory):
    with client_factory(cass_ping=True, hbase_ping=False) as c:
        r = c.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "degraded"


def test_health_degraded_when_cassandra_constructor_raises():
    """If CassandraClient() raises at startup, /health must still return 200 with degraded status."""
    import main as app_module
    with patch("main.CassandraClient", side_effect=Exception("connection refused")), \
         patch("main.HBaseClient") as mock_hbase_cls:
        mock_hbase = MagicMock()
        mock_hbase.ping.return_value = True
        mock_hbase_cls.return_value = mock_hbase
        with TestClient(app_module.app, raise_server_exceptions=True) as c:
            r = c.get("/health")
            assert r.status_code == 200
            body = r.json()
            assert body["cassandra"] is False
            assert body["status"] == "degraded"


def test_health_degraded_when_hbase_constructor_raises():
    """If HBaseClient() raises at startup, /health must still return 200 with degraded status."""
    import main as app_module
    with patch("main.HBaseClient", side_effect=Exception("thrift timeout")), \
         patch("main.CassandraClient") as mock_cass_cls:
        mock_cass = MagicMock()
        mock_cass.ping.return_value = True
        mock_cass_cls.return_value = mock_cass
        with TestClient(app_module.app, raise_server_exceptions=True) as c:
            r = c.get("/health")
            assert r.status_code == 200
            body = r.json()
            assert body["hbase"] is False
            assert body["status"] == "degraded"


# ── /api/ip/{ip} ──────────────────────────────────────────────────────────

def test_ip_reputation_both_layers(client_factory):
    now = datetime(2026, 4, 29, 12, 0, 0)
    events = [
        row(last_seen=now, threat_score=80, attack_type="brute-force"),
        row(last_seen=now, threat_score=95, attack_type="attack-signature"),
    ]
    summary = row(ip_source="1.2.3.4", last_seen=now, attack_types={"brute-force", "attack-signature"})
    batch = {"data:reputation_score": "60", "data:nb_malicious": "3", "data:attack_type": "port-scan"}

    with client_factory(
        ip_summary=summary,
        alert_count=5,
        recent_events=events,
        ip_reputation=batch,
    ) as c:
        r = c.get("/api/ip/1.2.3.4")
        assert r.status_code == 200
        body = r.json()
        assert body["ip"] == "1.2.3.4"
        # merged score = max(batch=60, realtime=95)
        assert body["merged_reputation_score"] == 95
        assert body["total_realtime_alerts"] == 5
        assert body["nb_batch_attacks"] == 3
        assert "attack-signature" in body["attack_types"]
        assert "port-scan" in body["attack_types"]


def test_ip_reputation_realtime_score_is_max_not_last(client_factory):
    """Score must be the max over all recent events, not whichever arrived last."""
    now = datetime(2026, 4, 29, 12, 0, 0)
    # High-score event followed by a low-score event (out-of-order delivery scenario)
    events = [
        row(last_seen=now, threat_score=95, attack_type="attack-signature"),
        row(last_seen=now, threat_score=20, attack_type="brute-force"),
    ]
    with client_factory(recent_events=events, ip_reputation=None) as c:
        r = c.get("/api/ip/10.0.0.1")
        assert r.status_code == 200
        assert r.json()["merged_reputation_score"] == 95


def test_ip_reputation_no_data(client_factory):
    """IP with no data in either layer should return zeros, not 500."""
    with client_factory(ip_summary=None, alert_count=0, recent_events=[], ip_reputation=None) as c:
        r = c.get("/api/ip/192.168.1.99")
        assert r.status_code == 200
        body = r.json()
        assert body["merged_reputation_score"] == 0
        assert body["total_realtime_alerts"] == 0
        assert body["recent_events"] == []


def test_ip_reputation_realtime_only(client_factory):
    now = datetime(2026, 4, 29, 12, 0, 0)
    events = [row(last_seen=now, threat_score=70, attack_type="brute-force")]
    with client_factory(recent_events=events, ip_reputation=None) as c:
        r = c.get("/api/ip/10.0.0.2")
        assert r.status_code == 200
        body = r.json()
        assert body["merged_reputation_score"] == 70
        assert body["batch_data"] is None


def test_ip_reputation_batch_score_wins(client_factory):
    """When batch score > realtime max, merged score must use the batch value."""
    now = datetime(2026, 4, 29, 12, 0, 0)
    events = [row(last_seen=now, threat_score=30, attack_type="brute-force")]
    batch = {"data:reputation_score": "90", "data:nb_malicious": "5", "data:attack_type": ""}
    with client_factory(recent_events=events, ip_reputation=batch) as c:
        r = c.get("/api/ip/10.0.0.3")
        assert r.status_code == 200
        assert r.json()["merged_reputation_score"] == 90


# ── /api/threats/live ─────────────────────────────────────────────────────

def test_live_threats_returns_list(client_factory):
    now = datetime(2026, 4, 29, 12, 0, 0)
    threats = [
        row(ip_source="1.1.1.1", last_seen=now, threat_score=80, attack_type="brute-force"),
        row(ip_source="2.2.2.2", last_seen=now, threat_score=90, attack_type="attack-signature"),
    ]
    with client_factory(live_threats=threats) as c:
        r = c.get("/api/threats/live")
        assert r.status_code == 200
        body = r.json()
        assert body["count"] == 2
        assert len(body["threats"]) == 2


def test_live_threats_passes_params_to_client(client_factory):
    with client_factory(live_threats=[]) as c:
        c.get("/api/threats/live?minutes=30&limit=50&attack_type=brute-force&min_score=70")
        c.mock_cass.get_live_threats.assert_called_once_with(
            minutes=30, limit=50, attack_type="brute-force", min_score=70
        )


def test_live_threats_filtered_empty_does_not_fallback_to_hbase(client_factory):
    top_ips = [{"ip": "10.0.0.9", "reputation_score": 100.0, "nb_malicious": 9, "nb_suspicious": 0}]
    with client_factory(live_threats=[], top_ips=top_ips) as c:
        r = c.get("/api/threats/live?attack_type=port-scan")
        assert r.status_code == 200
        assert r.json() == {"threats": [], "count": 0}
        c.mock_hbase.get_top_ips.assert_not_called()


def test_live_threats_invalid_minutes(client_factory):
    with client_factory() as c:
        r = c.get("/api/threats/live?minutes=0")
        assert r.status_code == 422


def test_live_threats_minutes_too_large(client_factory):
    with client_factory() as c:
        r = c.get("/api/threats/live?minutes=9999")
        assert r.status_code == 422


# ── /api/threats/correlated ───────────────────────────────────────────────

def test_correlated_attacks_returns_list(client_factory):
    now = datetime(2026, 4, 30, 12, 0, 0)
    attacks = [
        row(
            ip_source="10.0.0.9",
            first_seen=now,
            last_seen=now,
            stages={"port-scan", "attack-signature"},
            threat_score=100,
        )
    ]
    with client_factory(correlated_attacks=attacks) as c:
        r = c.get("/api/threats/correlated")
        assert r.status_code == 200
        body = r.json()
        assert body[0]["ip_source"] == "10.0.0.9"
        assert body[0]["stages"] == ["attack-signature", "port-scan"]
        assert body[0]["threat_score"] == 100


def test_correlated_attacks_passes_params_to_client(client_factory):
    with client_factory(correlated_attacks=[]) as c:
        c.get("/api/threats/correlated?minutes=30&limit=25")
        c.mock_cass.get_correlated_attacks.assert_called_once_with(minutes=30, limit=25)


def test_correlated_attacks_invalid_params(client_factory):
    with client_factory() as c:
        assert c.get("/api/threats/correlated?minutes=0").status_code == 422
        assert c.get("/api/threats/correlated?limit=0").status_code == 422


# ── /api/stats/top-ips ────────────────────────────────────────────────────

def test_top_ips_default_limit(client_factory):
    ips = [{"ip": f"10.0.0.{i}", "reputation_score": float(i * 10), "nb_malicious": i, "nb_suspicious": 0}
           for i in range(5)]
    with client_factory(top_ips=ips) as c:
        r = c.get("/api/stats/top-ips")
        assert r.status_code == 200
        assert len(r.json()) == 5
        c.mock_hbase.get_top_ips.assert_called_once_with(limit=10)


def test_top_ips_custom_limit(client_factory):
    with client_factory(top_ips=[]) as c:
        c.get("/api/stats/top-ips?limit=5")
        c.mock_hbase.get_top_ips.assert_called_once_with(limit=5)


def test_top_ips_limit_out_of_range(client_factory):
    with client_factory() as c:
        assert c.get("/api/stats/top-ips?limit=0").status_code == 422
        assert c.get("/api/stats/top-ips?limit=51").status_code == 422


# ── /api/stats/attack-patterns ────────────────────────────────────────────

def test_attack_patterns_no_filter(client_factory):
    patterns = [{"key": "SQLi_192.168.1.1_ts", "data": {"data:pattern": "DROP TABLE"}}]
    with client_factory(attack_patterns=patterns) as c:
        r = c.get("/api/stats/attack-patterns")
        assert r.status_code == 200
        assert r.json()[0]["key"] == "SQLi_192.168.1.1_ts"


def test_attack_patterns_with_type_filter(client_factory):
    with client_factory(attack_patterns=[]) as c:
        c.get("/api/stats/attack-patterns?attack_type=SQLi&limit=20")
        c.mock_hbase.get_attack_patterns.assert_called_once_with(attack_type="SQLi", limit=20)


# ── /api/stats/threat-timeline ────────────────────────────────────────────

def test_threat_timeline(client_factory):
    timeline = [{"date": "2026-04-28", "threat_label": "malicious", "count": 42}]
    with client_factory(threat_timeline=timeline) as c:
        r = c.get("/api/stats/threat-timeline")
        assert r.status_code == 200
        assert r.json()[0]["count"] == 42


def test_threat_timeline_params_forwarded(client_factory):
    with client_factory(threat_timeline=[]) as c:
        c.get("/api/stats/threat-timeline?days=7&threat_label=malicious")
        c.mock_hbase.get_threat_timeline.assert_called_once_with(days=7, threat_label="malicious")


# ── /api/stats/threat-volume ──────────────────────────────────────────────

def test_threat_volume(client_factory):
    volume = [{"threat_label": "malicious", "total_bytes": 1024.0}]
    with client_factory(threat_volume=volume) as c:
        r = c.get("/api/stats/threat-volume")
        assert r.status_code == 200
        assert r.json()[0]["total_bytes"] == 1024.0


# ── /api/ml/* ─────────────────────────────────────────────────────────────

def test_ml_summary_missing_returns_not_trained(client_factory, tmp_path):
    import main as app_module
    with patch.object(app_module, "ML_SUMMARY_PATH", str(tmp_path / "missing.json")):
        with client_factory() as c:
            r = c.get("/api/ml/summary")
            assert r.status_code == 200
            assert r.json()["status"] == "not_trained"


def test_ml_endpoints_return_summary_sections(client_factory, tmp_path):
    import main as app_module
    summary_path = tmp_path / "ml_summary.json"
    summary_path.write_text(json.dumps({
        "status": "trained",
        "trained_at": "2026-04-30T17:47:00Z",
        "dataset_rows": 6000000,
        "train_rows": 4798637,
        "test_rows": 1201363,
        "model_path": "hdfs://namenode:9000/models/threat_classifier",
        "predictions_path": "hdfs://namenode:9000/results/ml_predictions/",
        "cv_best_f1": 0.9475,
        "metrics": [
            {"model": "Random Forest", "accuracy": 0.9695, "f1_score": 0.9664, "precision": 0.9674}
        ],
        "prediction_counts": [
            {"predicted_label": "malicious", "count": 75817}
        ],
        "feature_importance": [
            {"feature": "path_length", "importance": 0.7546},
            {"feature": "has_sqli", "importance": 0.1093},
        ],
    }))

    with patch.object(app_module, "ML_SUMMARY_PATH", str(summary_path)):
        with client_factory() as c:
            assert c.get("/api/ml/summary").json()["dataset_rows"] == 6000000
            assert c.get("/api/ml/metrics").json()[0]["f1_score"] == 0.9664
            assert c.get("/api/ml/prediction-counts").json()[0]["predicted_label"] == "malicious"
            features = c.get("/api/ml/feature-importance?limit=1").json()
            assert features == [{"feature": "path_length", "importance": 0.7546}]
