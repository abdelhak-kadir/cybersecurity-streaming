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


def wait_for_cassandra(host="cassandra", retries=10, delay=10):
    for i in range(retries):
        try:
            cluster = Cluster([host])
            session = cluster.connect()
            print(f"Connected to Cassandra at {host}")
            return session
        except Exception as e:
            print(f"Cassandra not ready ({e}), retrying in {delay}s... ({i+1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Cassandra.")


def main():
    session = wait_for_cassandra()

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
    """)
    print("Table created: realtime_threats")

    session.execute("""
        CREATE TABLE IF NOT EXISTS ip_threat_summary (
            ip_source    TEXT PRIMARY KEY,
            last_seen    TIMESTAMP,
            threat_score INT,
            attack_types LIST<TEXT>,
            total_alerts INT
        )
    """)
    print("Table created: ip_threat_summary")
    print("Cassandra setup complete.")


if __name__ == "__main__":
    main()
