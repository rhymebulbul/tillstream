.PHONY: help up down up-infra up-apps test test-go test-python e2e load-test benchmark

help: ## Show this help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Spin up the entire TillStream infrastructure and applications
	docker-compose up -d --build
	@echo "TillStream is running! Kafka Broker: 29092 | Schema Registry: 8081 | Trino: 8090"

down: ## Tear down all infrastructure and clear volumes
	docker-compose down -v
	@echo "TillStream infrastructure torn down."

up-infra: ## Spin up ONLY the core infrastructure (Kafka, Schema Registry, MinIO)
	docker-compose up -d broker schema-registry minio minio-create-bucket trino

up-apps: ## Spin up ONLY the apps (Producer, Consumer)
	docker-compose up -d --build producer consumer

test: test-go test-python ## Run all unit and integration tests across Go and Python

test-go: ## Run Golang unit tests
	docker run --rm -v $$(pwd)/producers:/app -w /app golang:1.22 go test ./...

test-python: ## Run Python unit and integration tests
	cd agents && pytest test_dlq_resolver.py
	cd consumers && pytest test_integration.py

e2e: ## Run the End-to-End smoke test
	bash scripts/e2e_test.sh

load-test: ## Run the K6 load test benchmark
	k6 run load-test/k6-script.js

benchmark: ## Run the standalone metric calculation benchmarks
	docker run --rm -v $$(pwd)/producers:/app -w /app golang:1.22 go run cmd/benchmark/main.go
	docker run --rm -v $$(pwd):/app -w /app python:3.10 bash -c "pip install requests google-generativeai > /dev/null 2>&1 && python benchmark/llm_cache_benchmark.py"
