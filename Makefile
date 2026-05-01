# Clickstream Pipeline local management Makefile

.PHONY: build up down restart logs ps clean test simulator run-enrichment run-aggregations run-collector

build:
	docker-compose -f docker/docker-compose.yml build

up:
	docker-compose -f docker/docker-compose.yml up -d
	@echo "Waiting for services to become healthy..."
	sleep 10

down:
	docker-compose -f docker/docker-compose.yml down

restart: down up

logs:
	docker-compose -f docker/docker-compose.yml logs -f

ps:
	docker-compose -f docker/docker-compose.yml ps

clean:
	docker-compose -f docker/docker-compose.yml down -v
	rm -rf /tmp/spark_checkpoints/*

test:
	pytest tests/

run-collector:
	uvicorn collector.app.main:app --host 0.0.0.0 --port 8000 --reload

run-api:
	uvicorn api.app.main:app --host 0.0.0.0 --port 8082 --reload

simulator:
	python simulator/main.py

run-enrichment:
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 jobs/enrichment_job.py

run-aggregations:
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 jobs/aggregation_job.py
