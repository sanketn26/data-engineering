.PHONY: help setup serve build validate lab-kafka lab-spark lab-flink lab-clickhouse clean

.DEFAULT_GOAL := help

BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-16s$(NC) %s\n", $$1, $$2}'

setup: ## venv + documentation dependencies
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt
	@echo "$(GREEN)ok.  source .venv/bin/activate$(NC)"

serve: ## preview the academy at http://127.0.0.1:8000
	python3 -m mkdocs serve

build: ## build the static site into site/
	python3 -m mkdocs build --strict

validate: ## build docs and statically validate lab assets
	python3 -m mkdocs build --strict
	python3 scripts/validate_course.py

lab-kafka: ## start Kafka and print the next lab commands
	docker compose -f labs/kafka/docker-compose.yml up -d
	@echo "cd labs/kafka && pip install -r requirements.txt && python produce_events.py"

lab-spark: ## run the local Spark uniform-shuffle lab
	cd labs/spark && python run_lab.py --mode uniform

lab-flink: ## run deterministic watermark/idleness model
	python3 labs/flink/stalled_watermark.py

lab-clickhouse: ## start ClickHouse; then load schema and data
	docker compose -f labs/clickhouse/docker-compose.yml up -d
	docker exec -i dea-clickhouse clickhouse-client --user academy --password academy --multiquery < labs/clickhouse/schema.sql
	python3 labs/clickhouse/load_events.py

clean: ## caches and build output
	rm -rf site .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
