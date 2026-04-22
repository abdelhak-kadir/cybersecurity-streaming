.PHONY: batch stream all down-batch down-stream down clean \
        status logs shell-kafka shell-cassandra shell-spark shell-batch \
        batch-load batch-analytics batch-hbase batch-all \
        ml-train ml-predict ml-all \
        verify verify-all watch-kafka restart-producer restart-streaming

SPARK_BATCH_EXEC=docker compose exec spark-batch spark-submit --master spark://spark-master:7077

# ══════════════════════════════════════════════════════════════════
#  Start / Stop
# ══════════════════════════════════════════════════════════════════

## Start only the batch stack (HDFS + HBase + Spark workers)
batch:
	docker compose --profile batch up -d --build
	@echo ""
	@echo "Batch stack starting — HDFS, HBase, Spark master+workers."
	@echo "Run 'make batch-all' once services are up."

## Start only the streaming stack (Kafka + Cassandra + Spark streaming)
stream:
	docker compose --profile stream up -d --build
	@echo ""
	@echo "Stream stack starting — Kafka, Cassandra, Spark streaming."
	@echo "Wait ~60 s for Cassandra, then check: make status"

## Start everything (batch + stream)
all:
	docker compose --profile batch --profile stream up -d --build
	@echo ""
	@echo "Full stack starting. Wait ~60 s for Cassandra."

## Stop batch containers only
down-batch:
	docker compose --profile batch down

## Stop stream containers only
down-stream:
	docker compose --profile stream down

## Stop all containers
down:
	docker compose --profile batch --profile stream down

## Full reset — remove containers, volumes, orphans
clean:
	docker compose --profile batch --profile stream down -v --remove-orphans
	@echo "All containers and volumes removed."

# ══════════════════════════════════════════════════════════════════
#  Observability
# ══════════════════════════════════════════════════════════════════

status:
	docker compose ps

logs:
	docker compose logs -f $(s)

logs-all:
	docker compose --profile batch --profile stream logs -f

# ══════════════════════════════════════════════════════════════════
#  Shells
# ══════════════════════════════════════════════════════════════════

shell-cassandra:
	docker exec -it cassandra cqlsh

shell-kafka:
	docker exec -it kafka bash

shell-spark:
	docker exec -it spark-streaming bash

shell-batch:
	docker exec -it spark-batch bash

# ══════════════════════════════════════════════════════════════════
#  Streaming helpers
# ══════════════════════════════════════════════════════════════════

verify:
	docker exec -it cassandra cqlsh -e \
	  "USE cybersecurity; SELECT * FROM realtime_threats LIMIT 20;"

verify-all:
	docker exec -it cassandra cqlsh -e \
	  "USE cybersecurity; SELECT * FROM realtime_threats;"

watch-kafka:
	docker exec -it kafka kafka-console-consumer \
	  --bootstrap-server localhost:9092 \
	  --topic cybersecurity-logs \
	  --from-beginning

restart-producer:
	docker compose restart producer

restart-streaming:
	docker compose restart spark-streaming

# ══════════════════════════════════════════════════════════════════
#  Batch jobs
# ══════════════════════════════════════════════════════════════════

batch-load:
	$(SPARK_BATCH_EXEC) /app/01_load_hdfs.py

batch-analytics:
	$(SPARK_BATCH_EXEC) /app/02_top_ips.py
	$(SPARK_BATCH_EXEC) /app/03_port_scans.py
	$(SPARK_BATCH_EXEC) /app/03_Threat_Volume_Analysis.py
	$(SPARK_BATCH_EXEC) /app/06_SQLi_XSS.py

batch-hbase:
	$(SPARK_BATCH_EXEC) /app/07_hbase_storage.py

batch-all: batch-load batch-analytics batch-hbase

# ══════════════════════════════════════════════════════════════════
#  ML layer
# ══════════════════════════════════════════════════════════════════

ml-train:
	docker compose exec spark-batch spark-submit \
	  --master spark://spark-master:7077 \
	  /ml/08_ml_threat_classification.py
	@echo "Model      → hdfs://namenode:9000/models/threat_classifier"
	@echo "Predictions→ hdfs://namenode:9000/results/ml_predictions/"
	@echo "HBase table→ ml_predictions"

ml-predict:
	docker compose exec spark-batch spark-submit \
	  --master spark://spark-master:7077 \
	  /ml/09_ml_predict.py
	@echo "Predictions→ hdfs://namenode:9000/results/ml_predictions_latest/"

ml-all: ml-train ml-predict

# ── Serving layer additions for Makefile ─────────────────────────────────────
# Add these targets to your existing Makefile

# ── Start serving layer only ──────────────────────────────────────────────────
serving-up:
	docker compose up -d --build serving
	@echo "Serving layer starting at http://localhost:8000"
	@echo "API docs at: http://localhost:8000/docs"

# ── Serving layer logs ────────────────────────────────────────────────────────
serving-logs:
	docker compose logs -f serving

# ── Open API docs in browser (macOS) ─────────────────────────────────────────
serving-docs:
	open http://localhost:8000/docs

# ── Quick API smoke-tests (requires curl) ─────────────────────────────────────
serving-test:
	@echo "\n── Health ──────────────────────────────────────"
	curl -s http://localhost:8000/health | python3 -m json.tool
	@echo "\n── Live threats (last 60 min) ───────────────────"
	curl -s "http://localhost:8000/api/threats/live?limit=5" | python3 -m json.tool
	@echo "\n── Top IPs ──────────────────────────────────────"
	curl -s "http://localhost:8000/api/stats/top-ips?limit=5" | python3 -m json.tool
	@echo "\n── Attack patterns (SQLi) ───────────────────────"
	curl -s "http://localhost:8000/api/stats/attack-patterns?attack_type=SQLi&limit=5" | python3 -m json.tool
	@echo "\n── Threat timeline (last 7 days) ────────────────"
	curl -s "http://localhost:8000/api/stats/threat-timeline?days=7" | python3 -m json.tool
	@echo "\n── Threat volume ────────────────────────────────"
	curl -s http://localhost:8000/api/stats/threat-volume | python3 -m json.tool

# ── Test IP reputation merge for a specific IP ────────────────────────────────
# Usage: make serving-ip IP=192.168.1.10
serving-ip:
	curl -s "http://localhost:8000/api/ip/$(IP)" | python3 -m json.tool