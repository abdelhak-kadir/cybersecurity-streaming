import os
from datetime import datetime, timedelta
from typing import Optional

HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
HBASE_PORT = int(os.getenv("HBASE_PORT", 9090))


class HBaseClient:
    def __init__(self):
        import happybase  # lazy — not imported when class is mocked in tests
        self._happybase = happybase

    def close(self):
        pass

    def _connection(self):
        return self._happybase.Connection(HBASE_HOST, port=HBASE_PORT)

    def ping(self) -> bool:
        connection = None
        try:
            connection = self._connection()
            connection.tables()
            return True
        except Exception:
            return False
        finally:
            if connection:
                connection.close()

    def _decode_row(self, row: dict) -> dict:
        return {k.decode(): v.decode() for k, v in row.items()}

    def _value(self, row: dict, name: str, default="0"):
        return row.get(f"data:{name}", row.get(f"info:{name}", default))

    def get_ip_reputation(self, ip: str) -> Optional[dict]:
        connection = None
        try:
            connection = self._connection()
            table = connection.table("ip_reputation")
            row = table.row(ip.encode())
        except Exception as exc:
            print(f"[hbase] ip_reputation unavailable: {exc}")
            return None
        finally:
            if connection:
                connection.close()
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

    def get_top_ips(self, limit: int = 10) -> list:
        rows = []
        connection = None
        try:
            connection = self._connection()
            table = connection.table("ip_reputation")
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
        finally:
            if connection:
                connection.close()
        rows.sort(key=lambda x: x["reputation_score"], reverse=True)
        return rows[:limit]

    def get_attack_patterns(self, attack_type: Optional[str] = None, limit: int = 50) -> list:
        prefix_map = {"SQLi": b"SQLi_", "XSS": b"XSS_", "port_scan": b"PORTSCAN_"}
        prefix = prefix_map.get(attack_type) if attack_type else None

        rows = []
        scan_kwargs = {"limit": limit}
        if prefix:
            scan_kwargs["row_prefix"] = prefix
        connection = None
        try:
            connection = self._connection()
            table = connection.table("attack_patterns")
            for key, data in table.scan(**scan_kwargs):
                rows.append({"key": key.decode(), "data": self._decode_row(data)})
        except Exception as exc:
            print(f"[hbase] attack patterns unavailable: {exc}")
            return []
        finally:
            if connection:
                connection.close()
        return rows

    def get_threat_timeline(self, days: int = 30, threat_label: Optional[str] = None) -> list:
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = []

        def read_rows(connection, scan_kwargs: dict) -> list:
            result = []
            table = connection.table("threat_timeline")
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

        connection = None
        try:
            connection = self._connection()
            rows = read_rows(connection, {"row_start": start_date.encode()})
            if not rows:
                rows = read_rows(connection, {})
        except Exception as exc:
            print(f"[hbase] threat timeline unavailable: {exc}")
            return []
        finally:
            if connection:
                connection.close()
        return rows

    def get_threat_volume(self, limit: int = 50) -> list[dict]:
        rows = []
        connection = None
        try:
            connection = self._connection()
            table = connection.table("attack_patterns")
            for key, data in table.scan(row_prefix=b"VOLUME_", limit=limit):
                decoded = self._decode_row(data)
                rows.append({
                    "threat_label": key.decode().removeprefix("VOLUME_"),
                    "total_bytes": float(self._value(decoded, "total_bytes")),
                })
        except Exception as exc:
            print(f"[hbase] threat volume unavailable: {exc}")
            return []
        finally:
            if connection:
                connection.close()
        return rows
