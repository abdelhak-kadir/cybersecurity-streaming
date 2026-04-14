.PHONY: up down logs status shell-kafka shell-cassandra shell-spark verify clean

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

# ── Verify threats are saved in Cassandra ────────────────────────────────
verify:
	docker exec -it cassandra cqlsh -e \
	  "USE cybersecurity; SELECT * FROM realtime_threats LIMIT 20;"

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
