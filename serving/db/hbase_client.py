import os
from datetime import datetime, timedelta
from typing import Optional

HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
HBASE_PORT = int(os.getenv("HBASE_PORT", 9090))


class HBaseClient:
    def __init__(self):
        import happybase  # lazy — not imported when class is mocked in tests
        # ConnectionPool amortises Thrift connection overhead across requests.
        # Opens `size` connections eagerly — failure here is caught by the lifespan
        # try/except so the serving layer degrades gracefully rather than crashing.
        self._pool = happybase.ConnectionPool(
            size=3, host=HBASE_HOST, port=HBASE_PORT,
            timeout=10000,       # 10s socket timeout (ms) — prevents indefinite hangs
            transport="buffered",
        )

    def close(self):
        pass  # ConnectionPool manages connection lifecycle

    def ping(self) -> bool:
        try:
            with self._pool.connection() as conn:
                conn.tables()
            return True
        except Exception:
            return False

    def _decode_row(self, row: dict) -> dict:
        return {k.decode(): v.decode() for k, v in row.items()}

    def _value(self, row: dict, name: str, default="0"):
        return row.get(f"data:{name}", row.get(f"info:{name}", default))

    def get_ip_reputation(self, ip: str) -> Optional[dict]:
        try:
            with self._pool.connection() as conn:
                table = conn.table("ip_reputation")
                row = table.row(ip.encode())
        except Exception as exc:
            print(f"[hbase] ip_reputation unavailable: {exc}")
            return None
        if not row:
            return None
        decoded = self._decode_row(row)
        return {
            **decoded,
            "data:reputation_score": self._value(decoded, "reputation_score"),
            "data:nb_malicious": self._value(decoded, "nb_malicious"),
            "data:nb_suspicious": self._value(decoded, "nb_suspicious"),
            "data:attack_type": self._value(decoded, "attack_type", ""),
        }

    def get_ip_reputations_batch(self, ips: list) -> dict:
        """Single HBase multi-get for a list of IPs — avoids N separate connections."""
        if not ips:
            return {}
        try:
            with self._pool.connection() as conn:
                table = conn.table("ip_reputation")
                rows = table.rows([ip.encode() for ip in ips])
        except Exception as exc:
            print(f"[hbase] batch ip_reputation unavailable: {exc}")
            return {}
        result = {}
        for key, data in rows:
            decoded = self._decode_row(data)
            ip = key.decode()
            result[ip] = {
                **decoded,
                "data:reputation_score": self._value(decoded, "reputation_score"),
                "data:nb_malicious": self._value(decoded, "nb_malicious"),
                "data:nb_suspicious": self._value(decoded, "nb_suspicious"),
                "data:attack_type": self._value(decoded, "attack_type", ""),
            }
        return result

    def get_top_ips(self, limit: int = 10) -> list:
        rows = []
        try:
            with self._pool.connection() as conn:
                table = conn.table("ip_reputation")
                for key, data in table.scan(limit=limit * 5):  # over-fetch then sort
                    decoded = self._decode_row(data)
                    rows.append({
                        "ip": key.decode(),
                        "reputation_score": float(self._value(decoded, "reputation_score")),
                        "nb_malicious": int(self._value(decoded, "nb_malicious")),
                        "nb_suspicious": int(self._value(decoded, "nb_suspicious")),
                    })
        except Exception as exc:
            print(f"[hbase] top IPs unavailable: {exc}")
            return []
        rows.sort(key=lambda x: x["reputation_score"], reverse=True)
        return rows[:limit]

    def get_attack_patterns(self, attack_type: Optional[str] = None, limit: int = 50) -> list:
        # Rows are keyed ALERT_<ip>_<date>_<type> (e.g. ALERT_1.2.3.4_2024-01-01_SQLi)
        rows = []
        try:
            with self._pool.connection() as conn:
                table = conn.table("attack_patterns")
                # Over-fetch to account for type filtering; cap at 10× limit
                scan_limit = limit if not attack_type else limit * 10
                for key, data in table.scan(row_prefix=b"ALERT_", limit=scan_limit):
                    key_str = key.decode()
                    if attack_type and not key_str.endswith(f"_{attack_type}"):
                        continue
                    rows.append({"key": key_str, "data": self._decode_row(data)})
                    if len(rows) >= limit:
                        break
        except Exception as exc:
            print(f"[hbase] attack patterns unavailable: {exc}")
            return []
        return rows

    def get_threat_timeline(self, days: int = 30, threat_label: Optional[str] = None) -> list:
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = []

        def _read(conn, scan_kwargs: dict) -> list:
            result = []
            table = conn.table("threat_timeline")
            for key, data in table.scan(**scan_kwargs):
                decoded = self._decode_row(data)
                key_str = key.decode()
                parts = key_str.split("_", 1)
                label = parts[1] if len(parts) == 2 else ""
                if threat_label and label != threat_label:
                    continue
                result.append({
                    "date": parts[0],
                    "threat_label": label,
                    "count": int(self._value(decoded, "count", self._value(decoded, "nb_menaces"))),
                })
            return result

        try:
            with self._pool.connection() as conn:
                rows = _read(conn, {"row_start": start_date.encode()})
                if not rows:
                    rows = _read(conn, {})
        except Exception as exc:
            print(f"[hbase] threat timeline unavailable: {exc}")
            return []
        return rows

    def get_attacks_by_protocol(self) -> list:
        rows = []
        try:
            with self._pool.connection() as conn:
                table = conn.table("attack_patterns")
                for key, data in table.scan(row_prefix=b"PROTO_"):
                    decoded = self._decode_row(data)
                    rows.append({
                        "protocol": self._value(decoded, "protocol", ""),
                        "threat_label": self._value(decoded, "threat_label", ""),
                        "nb_events": int(self._value(decoded, "nb_events")),
                        "total_bytes": float(self._value(decoded, "total_bytes", "0")),
                    })
        except Exception as exc:
            print(f"[hbase] attacks by protocol unavailable: {exc}")
            return []
        return rows

    def get_threat_volume(self, limit: int = 50) -> list:
        rows = []
        try:
            with self._pool.connection() as conn:
                table = conn.table("attack_patterns")
                for key, data in table.scan(row_prefix=b"VOLUME_", limit=limit):
                    decoded = self._decode_row(data)
                    rows.append({
                        "threat_label": key.decode().removeprefix("VOLUME_"),
                        "total_bytes": float(self._value(decoded, "total_bytes")),
                    })
        except Exception as exc:
            print(f"[hbase] threat volume unavailable: {exc}")
            return []
        return rows
