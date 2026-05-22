"""
cassandra_setup.py
------------------
Run this once to create the keyspace and tables in Cassandra.
In Docker, this is handled automatically by cassandra-init.cql —
but you can also run this manually if needed.
"""

import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


_cluster = None


def wait_for_cassandra(host="cassandra", retries=10, delay=10):
    global _cluster
    for i in range(retries):
        try:
            _cluster = Cluster([host])
            session = _cluster.connect()
            print(f"Connected to Cassandra at {host}")
            return session
        except Exception as e:
            if _cluster:
                _cluster.shutdown()
                _cluster = None
            print(f"Cassandra not ready ({e}), retrying in {delay}s... ({i+1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Cassandra.")


def main():
    session = wait_for_cassandra()

    # replication_factor=1 is acceptable for single-node dev/demo.
    # For production, use NetworkTopologyStrategy with RF=3.
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS cybersecurity
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    print("Keyspace created: cybersecurity")

    session.execute("USE cybersecurity")

    session.execute("""
        CREATE TABLE IF NOT EXISTS realtime_threats (
            ip_source    TEXT,
            last_seen    TIMESTAMP,
            threat_score INT,
            attack_type  TEXT,
            PRIMARY KEY (ip_source, last_seen)
        ) WITH CLUSTERING ORDER BY (last_seen DESC)
          AND default_time_to_live = 86400
    """)
    print("Table created: realtime_threats")

    session.execute("""
        CREATE TABLE IF NOT EXISTS ip_threat_summary (
            ip_source    TEXT PRIMARY KEY,
            last_seen    TIMESTAMP,
            attack_types SET<TEXT>
        )
    """)
    print("Table created: ip_threat_summary")

    session.execute("""
        CREATE TABLE IF NOT EXISTS correlated_attacks (
            ip_source    TEXT,
            last_seen    TIMESTAMP,
            first_seen   TIMESTAMP,
            stages       SET<TEXT>,
            threat_score INT,
            PRIMARY KEY (ip_source, last_seen)
        ) WITH CLUSTERING ORDER BY (last_seen DESC)
          AND default_time_to_live = 86400
    """)
    print("Table created: correlated_attacks")
    print("Cassandra setup complete.")


if __name__ == "__main__":
    main()
