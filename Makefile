# Every target runs offline: synthetic API responses, a local DuckDB file, no credentials.
PYTHON ?= python3
VENV ?= .venv
BIN := $(abspath $(VENV))/bin
PROD_VENV ?= .venv-prod
DUCKDB_PATH ?= $(CURDIR)/local.duckdb
DBT := cd transform && DUCKDB_PATH=$(DUCKDB_PATH) DBT_TARGET=local $(BIN)/dbt

.DEFAULT_GOAL := help
.PHONY: help install lint pytest dbt test demo bigquery-check lock clean

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  make %-15s %s\n", $$1, $$2}'

$(VENV)/.installed: requirements/dev.txt pyproject.toml
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --quiet --upgrade pip
	$(BIN)/pip install --quiet -r requirements/dev.txt
	$(BIN)/pip install --quiet --no-deps -e .
	touch $@

install: $(VENV)/.installed ## Create the virtualenv from the pinned requirements

lint: install ## ruff lint and format check
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .

pytest: install ## Unit tests, pipeline tests and dbt tests (with sabotage cases)
	$(BIN)/pytest

dbt: install ## Replay the synthetic reports into DuckDB, then dbt build and freshness
	rm -f $(DUCKDB_PATH)
	$(BIN)/amazon-ads-pipeline --replay tests/fixtures/amazon_ads --warehouse duckdb \
		--duckdb-path $(DUCKDB_PATH) --region NA --region EU \
		--start 2026-09-01 --end 2026-09-14
	$(DBT) build --profiles-dir .
	$(DBT) source freshness --profiles-dir .

test: lint pytest dbt ## Everything: lint, pytest, then the dbt build on DuckDB

demo: dbt ## Build locally and print the marts
	$(BIN)/python scripts/show_marts.py $(DUCKDB_PATH)

$(PROD_VENV)/.installed: requirements/prod.txt pyproject.toml
	$(PYTHON) -m venv $(PROD_VENV)
	$(PROD_VENV)/bin/pip install --quiet --upgrade pip
	$(PROD_VENV)/bin/pip install --quiet -r requirements/prod.txt
	touch $@

bigquery-check: $(PROD_VENV)/.installed ## Compile dbt for BigQuery and syntax-check the SQL (offline)
	$(PROD_VENV)/bin/python scripts/check_bigquery_sql.py

lock: ## Re-pin requirements/*.txt from pyproject.toml (needs uv)
	uv pip compile pyproject.toml --extra dev --universal --python-version 3.10 -o requirements/dev.txt
	uv pip compile pyproject.toml --extra prod --universal --python-version 3.10 -o requirements/prod.txt

clean: ## Remove the virtualenv and build output
	rm -rf $(VENV) $(PROD_VENV) transform/target transform/logs local.duckdb .pytest_cache .ruff_cache
