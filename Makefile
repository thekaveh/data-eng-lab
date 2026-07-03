.DEFAULT_GOAL := help
SHELL := /bin/bash
.PHONY: help setup up down datasets verify test preflight lint fmt new-scenario build-apps

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n",$$1,$$2}'

setup: ## Initialize the Atlas submodule
	git submodule update --init --recursive infra

up: ## Launch the Atlas data-eng track + bootstrap
	./scripts/start-all.sh

down: ## Tear down (add COLD=1 to wipe volumes)
	./scripts/stop-all.sh $(if $(COLD),--cold,)

datasets: ## Download datasets into MinIO landing bucket (override tier with SCALE=tiny|small|medium)
	uv run python scripts/download_datasets.py --scale $(if $(SCALE),$(SCALE),small)

verify: ## Run the repo verifier
	uv run python scripts/verify_repo.py --root .

test: ## offline: no live stack, no network
	uv run pytest -m "not infra and not network" -q

preflight: ## Infra preflight (layer 1 existence + layer 2 integration) against a live stack
	uv run python tests/infra/preflight.py
	uv run python tests/infra/layer2.py

lint: ## Lint (ruff; shell/yaml lint if installed)
	uv run ruff check .
	@if command -v shellcheck >/dev/null; then shellcheck scripts/*.sh; else echo "shellcheck not installed — skipping"; fi

fmt: ## Auto-format Python
	uv run ruff format .
	uv run ruff check --fix .

new-scenario: ## Scaffold a scenario folder: make new-scenario NAME=pattern-dataset-engine-format
	uv run python scripts/new_scenario.py $(NAME) --root .

build-apps: ## Build (test + shade) the Maven Spark apps
	for pom in spark-apps/*/pom.xml; do \
		echo "$$pom"; mvn -q -B -f "$$pom" package; \
	done
