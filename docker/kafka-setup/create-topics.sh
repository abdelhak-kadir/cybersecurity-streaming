#!/bin/bash

echo "Waiting for Kafka to be ready..."
sleep 10

echo "Creating topic: cybersecurity-logs"
kafka-topics --create \
  --bootstrap-server kafka:29092 \
  --topic cybersecurity-logs \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "Topic list:"
kafka-topics --list --bootstrap-server kafka:29092

echo "Topic setup complete."
