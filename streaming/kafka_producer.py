"""
kafka_producer.py
-----------------
Reads cybersecurity_logs.csv row by row and sends each log
as a JSON message to the Kafka topic 'cybersecurity-logs'.

Simulates a live network log stream at ~10 events/second.
"""

import csv
import json
import time
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config ────────────────────────────────────────────────────────────────
KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC   = "cybersecurity-logs"
CSV_FILE      = os.getenv("CSV_FILE", "/data/cybersecurity_logs.csv")
DELAY_SECONDS = float(os.getenv("SEND_DELAY", "0.1"))  # 100ms = 10 events/sec


def wait_for_kafka(broker, retries=10, delay=5):
    """Retry connecting to Kafka on startup."""
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
            print(f"Connected to Kafka at {broker}")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready, retrying in {delay}s... ({i+1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


def row_to_message(row: dict) -> dict:
    """Convert a CSV row dict into a clean JSON-serialisable message."""
    return {
        "timestamp":         row.get("timestamp", ""),
        "source_ip":         row.get("source_ip", ""),
        "dest_ip":           row.get("dest_ip", ""),
        "protocol":          row.get("protocol", ""),
        "action":            row.get("action", ""),
        "threat_label":      row.get("threat_label", ""),
        "bytes_transferred": int(row.get("bytes_transferred", 0) or 0),
        "user_agent":        row.get("user_agent", ""),
        "request_path":      row.get("request_path", ""),
    }


def main():
    print(f"Starting Kafka producer — topic: {KAFKA_TOPIC}")
    producer = wait_for_kafka(KAFKA_BROKER)

    if not os.path.exists(CSV_FILE):
        print(f"CSV file not found: {CSV_FILE}")
        print("Place your dataset at /data/cybersecurity_logs.csv")
        print("Running in demo mode with synthetic data...")
        run_demo_mode(producer)
        return

    sent = 0
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            message = row_to_message(row)
            producer.send(
                topic=KAFKA_TOPIC,
                key=message["source_ip"],
                value=message,
            )
            sent += 1
            if sent % 100 == 0:
                print(f"Sent {sent} messages...")
            time.sleep(DELAY_SECONDS)

    producer.flush()
    producer.close()
    print(f"Done. Total messages sent: {sent}")


def run_demo_mode(producer):
    """Send synthetic log data when no CSV is available."""
    import random
    import datetime

    ips = ["192.168.1.10", "10.0.0.5", "172.16.0.3", "192.168.2.99", "10.10.0.1"]
    actions = ["blocked", "allowed", "blocked", "blocked"]
    protocols = ["TCP", "HTTP", "SSH", "UDP"]
    agents = ["Mozilla/5.0", "sqlmap/1.7.2", "nikto/2.1", "curl/7.8", "python-requests"]
    paths = ["/login", "/admin", "/?id=1' OR '1'='1", "/wp-admin", "/index.php"]
    labels = ["benign", "suspicious", "malicious"]

    sent = 0
    print("Sending synthetic demo data (Ctrl+C to stop)...")
    while True:
        msg = {
            "timestamp":         datetime.datetime.now().isoformat(),
            "source_ip":         random.choice(ips),
            "dest_ip":           "10.0.0.100",
            "protocol":          random.choice(protocols),
            "action":            random.choice(actions),
            "threat_label":      random.choice(labels),
            "bytes_transferred": random.randint(100, 20_000_000),
            "user_agent":        random.choice(agents),
            "request_path":      random.choice(paths),
        }
        producer.send(KAFKA_TOPIC, key=msg["source_ip"], value=msg)
        sent += 1
        if sent % 50 == 0:
            print(f"Demo: sent {sent} synthetic messages")
        time.sleep(DELAY_SECONDS)


if __name__ == "__main__":
    main()
