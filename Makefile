.PHONY: up-all up-web-server up-kafka up-flink up-clickhouse up-grafana down-web-server down-kafka down-flink down-clickhouse down-grafana down-all logs

up-all:
	@echo "Deploying all services in order..."
	bash ./deploy.sh

up-web-server:
	@echo "Starting web server..."
	docker compose -f web-server/docker-compose.yml up -d

up-kafka:
	@echo "Starting Kafka..."
	docker compose -f kafka/docker-compose.yml up -d

up-flink:
	@echo "Starting Flink..."
	docker compose -f flink/docker-compose.yml up -d

up-clickhouse:
	@echo "Starting ClickHouse..."
	docker compose -f clickhouse/docker-compose.yml up -d

up-grafana:
	@echo "Starting Grafana..."
	docker compose -f grafana/docker-compose.yml up -d

down-web-server:
	docker compose -f web-server/docker-compose.yml down -v

down-kafka:
	docker compose -f kafka-server/docker-compose.yml down -v

down-flink:
	docker compose -f flink-consumer/docker-compose.yml down -v

down-clickhouse:
	docker compose -f clickhouse/docker-compose.yml down -v

down-grafana:
	docker compose -f grafana/docker-compose.yml down -v


down-all:
	@echo "Stopping all services..."
	docker compose -f kafka/docker-compose.yml down -v
	docker compose -f flink/docker-compose.yml down -v
	docker compose -f clickhouse/docker-compose.yml down -v
	docker compose -f grafana/docker-compose.yml down -v
	docker compose -f web-server/docker-compose.yml down -v

logs:
	docker compose -f web-server/docker-compose.yml logs -f
	docker compose -f kafka/docker-compose.yml logs -f
	docker compose -f flink/docker-compose.yml logs -f
