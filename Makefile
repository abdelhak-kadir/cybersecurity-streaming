.PHONY: up down logs status shell-kafka shell-cassandra shell-spark shell-batch verify clean batch-load batch-analytics batch-hbase batch-all

SPARK_BATCH_EXEC=docker compose exec spark-batch spark-submit --master spark://spark-master:7077

# ── Start everything ──────────────────────────────────────────────────────
up:
	docker compose up -d --build
	@echo ""
	@echo "Services starting... wait ~60 seconds for Cassandra to be ready."
	@echo "Check status with: make status"

# ── Stop everything ───────────────────────────────────────────────────────
down:
	docker compose down

# ── Stop and remove volumes (full reset) ─────────────────────────────────
clean:
	docker compose down -v --remove-orphans
	@echo "All containers and volumes removed."

# ── Show running containers ───────────────────────────────────────────────
status:
	docker compose ps

# ── Follow logs of a service (usage: make logs s=spark-streaming) ─────────
logs:
	docker compose logs -f $(s)

# ── Follow all logs ───────────────────────────────────────────────────────
logs-all:
	docker compose logs -f

# ── Open Cassandra shell ──────────────────────────────────────────────────
shell-cassandra:
	docker exec -it cassandra cqlsh

# ── Open Kafka shell ──────────────────────────────────────────────────────
shell-kafka:
	docker exec -it kafka bash

# ── Open shell in Spark container ────────────────────────────────────────
shell-spark:
	docker exec -it spark-streaming bash

# ── Open shell in Batch Spark container ────────────────────────────────
shell-batch:
	docker exec -it spark-batch bash

# ── Verify threats are saved in Cassandra ────────────────────────────────
verify:
	docker exec -it cassandra cqlsh -e \
	  "USE cybersecurity; SELECT * FROM realtime_threats LIMIT 20;"

# ── Verify all threats are saved in Cassandra ────────────────────────────────
verify-all:
	docker exec -it cassandra cqlsh -e \
	  "USE cybersecurity; SELECT * FROM realtime_threats;"

# ── Watch Kafka messages live ─────────────────────────────────────────────
watch-kafka:
	docker exec -it kafka kafka-console-consumer \
	  --bootstrap-server localhost:9092 \
	  --topic cybersecurity-logs \
	  --from-beginning

# ── Restart just the producer ─────────────────────────────────────────────
restart-producer:
	docker compose restart producer

# ── Restart just the streaming job ───────────────────────────────────────
restart-streaming:
	docker compose restart spark-streaming

# ── Batch: load historical data into HDFS ──────────────────────────────
batch-load:
	$(SPARK_BATCH_EXEC) /app/01_load_hdfs.py

# ── Batch: run all historical analyses required by the PDF ─────────────
batch-analytics:
	$(SPARK_BATCH_EXEC) /app/02_top_ips.py
	$(SPARK_BATCH_EXEC) /app/03_port_scans.py
	$(SPARK_BATCH_EXEC) /app/03_Threat_Volume_Analysis.py
	$(SPARK_BATCH_EXEC) /app/06_SQLi_XSS.py

# ── Batch: persist batch views into HBase ──────────────────────────────
batch-hbase:
	$(SPARK_BATCH_EXEC) /app/07_hbase_storage.py

# ── Batch: full end-to-end execution chain ─────────────────────────────
batch-all: batch-load batch-analytics batch-hbase

# ═══════════════════════════════════════════════════════════════
#  ML Layer
# ═══════════════════════════════════════════════════════════════

ml-train:
	docker compose exec spark-batch spark-submit \
	  --master spark://spark-master:7077 \
	  /ml/08_ml_threat_classification.py
	@echo "Model saved  → hdfs://namenode:9000/models/threat_classifier"
	@echo "Predictions  → hdfs://namenode:9000/results/ml_predictions/"
	@echo "HBase table  → ml_predictions"

ml-predict:
	docker compose exec spark-batch spark-submit \
	  --master spark://spark-master:7077 \
	  /ml/09_ml_predict.py
	@echo "Predictions  → hdfs://namenode:9000/results/ml_predictions_latest/"

ml-all: ml-train ml-predict