# Load .env and export every variable to child processes.
# This means DIGITALOCEAN_TOKEN, TF_VAR_*, etc. are available to
# terraform, ansible, and all shell commands without touching .zshrc.
ifneq (,$(wildcard .env))
  include .env
  export
endif

.PHONY: batch stream all down-batch down-stream down clean \
        status logs shell-kafka shell-cassandra shell-spark shell-batch \
        batch-load batch-analytics batch-hbase batch-all \
        ml-train ml-predict ml-all \
        verify verify-all watch-kafka restart-producer restart-streaming \
        test reset-schema serving-up serving-logs serving-docs serving-test serving-ip \
        grafana-up grafana-open \
        tf-init tf-apply tf-destroy tf-ips \
        gen-inventory \
        ansible-install-docker ansible-deploy-infra ansible-deploy-compute \
        ansible-deploy-serve ansible-verify \
        deploy-all sync-ml-summary ansible-deploy-batch

SPARK_BATCH_EXEC=docker compose exec spark-batch spark-submit --master spark://spark-master:7077

# ══════════════════════════════════════════════════════════════════
#  Start / Stop
# ══════════════════════════════════════════════════════════════════

## DESTRUCTIVE: drop and recreate Cassandra tables (requires running stack)
## Useful after a schema change; normal restarts do NOT need this.
reset-schema:
	@echo "WARNING: this will drop all Cassandra threat data. Ctrl-C to abort."
	@sleep 3
	docker exec -i cassandra cqlsh < docker/cassandra-reset.cql
	@echo "Schema reset complete."

## Run unit tests inside Docker (no live Cassandra/HBase needed)
test:
	docker compose --profile test run --rm test

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
	docker compose --profile serve up -d --build serving
	@echo "Serving layer starting at http://localhost:8000"
	@echo "API docs at: http://localhost:8000/docs"

grafana-up:
	docker compose --profile serve up -d --build grafana
	@echo "Grafana starting at http://localhost:3000 (admin / admin)"

grafana-open:
	open http://localhost:3000

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

# ══════════════════════════════════════════════════════════════════
#  Digital Ocean — Terraform + Ansible deployment
#  Prerequisites:
#    brew install terraform ansible
#    ssh-keygen -t ed25519 -f ~/.ssh/do_cyber
#    export DIGITALOCEAN_TOKEN="your_token"
# ══════════════════════════════════════════════════════════════════

# ── Terraform ─────────────────────────────────────────────────────
tf-init:
	cd terraform && terraform init

tf-apply:
	cd terraform && terraform apply

tf-destroy:
	cd terraform && terraform destroy

tf-ips:
	cd terraform && terraform output

# ── Ansible ───────────────────────────────────────────────────────

## Generate ansible/inventory.ini from terraform outputs (run after tf-apply)
gen-inventory:
	bash ansible/generate_inventory.sh

## Install Docker CE on all 3 droplets
ansible-install-docker:
	ansible-playbook -i ansible/inventory.ini ansible/playbooks/install_docker.yml

## Deploy Kafka + Cassandra to Droplet 1
ansible-deploy-infra:
	ansible-playbook -i ansible/inventory.ini ansible/playbooks/deploy_infra.yml

## Deploy Spark + HBase + HDFS + streaming to Droplet 2
ansible-deploy-compute:
	ansible-playbook -i ansible/inventory.ini ansible/playbooks/deploy_compute.yml

## Deploy FastAPI + Grafana to Droplet 3
ansible-deploy-serve:
	ansible-playbook -i ansible/inventory.ini ansible/playbooks/deploy_serve.yml

## Run health checks across all droplets
ansible-verify:
	ansible-playbook -i ansible/inventory.ini ansible/playbooks/verify.yml

## Run the full batch pipeline on compute (loads CSV→HDFS, analytics, writes to HBase)
ansible-deploy-batch:
	ansible-playbook -i ansible/inventory.ini ansible/playbooks/deploy_batch.yml

## Copy updated ml_summary.json from compute to serve after a batch/ML run
sync-ml-summary:
	ansible-playbook -i ansible/inventory.ini ansible/playbooks/sync_ml_summary.yml

## Full deployment from scratch — provisions infra, deploys all layers,
## runs the batch pipeline, and verifies the cluster.
deploy-all: tf-apply gen-inventory ansible-install-docker \
            ansible-deploy-infra ansible-deploy-compute ansible-deploy-serve \
            ansible-deploy-batch ansible-verify
	@echo ""
	@echo "Cluster deployed successfully."
	@echo "Serve IP:  $$(cd terraform && terraform output -raw serve_ip)"
	@echo "Grafana:   https://$(GRAFANA_SUBDOMAIN)"
	@echo "FastAPI:   https://$(API_SUBDOMAIN)  (only if EXPOSE_API=true)"
	@echo ""
	@echo "TLS: Caddy handles certs automatically via Let's Encrypt."
