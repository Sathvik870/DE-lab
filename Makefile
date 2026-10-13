# ==============================================================================
# Pipeline as Code: Task Automation & Infrastructure Management
# ==============================================================================

SHELL := /bin/bash
PYTHON := .venv/bin/python
PIP := .venv/bin/pip
PYTEST := .venv/bin/pytest
FLAKE8 := .venv/bin/flake8

.PHONY: help setup-env install test test-unit test-mock lint ci-local docker-build docker-test up down lakehouse-up lakehouse-down stream-producer stream-processor benchmark api dashboard clean

help:
	@echo "Earbuds Data Engineering Lakehouse Pipeline as Code Runner"
	@echo "------------------------------------------------------------"
	@echo "Available commands:"
	@echo "  make setup-env        : Create python virtual environment (.venv)"
	@echo "  make install          : Install production and dev requirements"
	@echo "  make test             : Run full test suite with coverage report"
	@echo "  make test-unit        : Run transformation and validation unit tests"
	@echo "  make test-mock        : Run mock tests for DB extract, load & DAG"
	@echo "  make lint             : Run flake8 code quality analysis"
	@echo "  make ci-local         : Execute standalone local CI pipeline runner"
	@echo "  make lakehouse-up     : Start Lakehouse cluster (Kafka, MinIO, Iceberg, Trino, Airflow)"
	@echo "  make lakehouse-down   : Stop Lakehouse cluster"
	@echo "  make stream-producer  : Run Kafka real-time stream event producer"
	@echo "  make stream-processor : Run PySpark streaming processor into Iceberg"
	@echo "  make benchmark        : Execute Trino performance benchmark suite"
	@echo "  make api              : Launch FastAPI analytics backend server"
	@echo "  make dashboard        : Launch Vite/React lakehouse BI dashboard"
	@echo "  make clean            : Remove caches, test outputs, and temporary files"

setup-env:
	python3 -m venv .venv
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements-dev.txt

install:
	$(PIP) install -r requirements-dev.txt

test:
	$(PYTEST) tests/ -v --cov=etl --cov-report=term-missing

test-unit:
	$(PYTEST) tests/test_transform.py tests/test_validator.py -v

test-mock:
	$(PYTEST) tests/test_extract_mock.py tests/test_loader_mock.py tests/test_pipeline_mock.py tests/test_dag_integrity.py -v

lint:
	$(FLAKE8) etl/ tests/ --max-line-length=120 --exclude=__pycache__,.venv,venv

ci-local:
	@bash scripts/run_local_ci.sh

docker-build:
	docker build -t earbuds-etl-pipeline:test .

docker-test:
	docker run --rm earbuds-etl-pipeline:test

up:
	docker compose -f docker-compose.yml up -d
	docker compose -f airflow-compose.yml up -d

down:
	docker compose -f airflow-compose.yml down
	docker compose -f docker-compose.yml down

lakehouse-up:
	docker compose -f docker-compose.lakehouse.yml up -d

lakehouse-down:
	docker compose -f docker-compose.lakehouse.yml down

stream-producer:
	$(PYTHON) scripts/kafka_stream_producer.py --continuous --rate 50 --inject-anomalies

stream-processor:
	$(PYTHON) spark_streaming/stream_processor.py

benchmark:
	$(PYTHON) benchmarks/trino_benchmark.py --save docs/benchmarks_results.md

api:
	$(PYTHON) -m uvicorn analytics.main:app --host 0.0.0.0 --port 8000 --reload

dashboard:
	cd dashboard && npm run dev

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	rm -rf .coverage htmlcov/

