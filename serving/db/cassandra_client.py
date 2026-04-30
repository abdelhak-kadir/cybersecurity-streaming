import os
from datetime import datetime, timedelta
from typing import Optional

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", 9042))
KEYSPACE = "cybersecurity"


class CassandraClient:
    def __init__(self):
        from cassandra.cluster import Cluster  # lazy — not imported when class is mocked in tests
        self.cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        self.session = self.cluster.connect(KEYSPACE)

    def close(self):
        self.cluster.shutdown()

    def ping(self) -> bool:
        try:
            self.session.execute("SELECT now() FROM system.local")
            return True
        except Exception:
            return False

    def get_ip_summary(self, ip: str):
        row = self.session.execute(
            "SELECT ip_source, last_seen, attack_types "
            "FROM ip_threat_summary WHERE ip_source = %s",
            [ip],
        ).one()
        return row

    def get_ip_alert_count(self, ip: str) -> int:
        """Count alerts from realtime_threats — idempotent and bounded by 24h TTL."""
        row = self.session.execute(
            "SELECT COUNT(*) FROM realtime_threats WHERE ip_source = %s",
            [ip],
        ).one()
        return int(row.count) if row else 0

    def get_ip_recent_events(self, ip: str, limit: int = 10) -> list:
        rows = self.session.execute(
            "SELECT ip_source, last_seen, threat_score, attack_type "
            "FROM realtime_threats WHERE ip_source = %s LIMIT %s",
            [ip, limit],
        )
        return list(rows)

    def get_live_threats(
        self,
        minutes: int = 60,
        limit: int = 100,
        attack_type: Optional[str] = None,
        min_score: int = 0,
    ) -> list:
        since = datetime.utcnow() - timedelta(minutes=minutes)
        # ALLOW FILTERING is acceptable at project scale (data is bounded by 24h TTL)
        query = (
            "SELECT ip_source, last_seen, threat_score, attack_type "
            "FROM realtime_threats WHERE last_seen >= %s ALLOW FILTERING"
        )
        rows = list(self.session.execute(query, [since]))

        if attack_type:
            rows = [r for r in rows if r.attack_type == attack_type]
        if min_score:
            rows = [r for r in rows if (r.threat_score or 0) >= min_score]

        rows.sort(key=lambda r: r.last_seen or datetime.min, reverse=True)
        return rows[:limit]
