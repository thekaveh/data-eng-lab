.DEFAULT_GOAL := help
SHELL := /bin/bash

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n",$$1,$$2}'

setup: ## Initialize the Atlas submodule
	git submodule update --init --recursive infra

up: ## Launch the Atlas data-eng track + bootstrap
	./scripts/start-all.sh

down: ## Tear down (add COLD=1 to wipe volumes)
	./scripts/stop-all.sh $(if $(COLD),--cold,)

datasets: ## Download datasets into MinIO (Phase 1)
	@echo "datasets: implemented in Phase 1"

verify: ## Run the repo verifier
	uv run python scripts/verify_repo.py --root .

test: ## Static + unit tests (no live stack)
	uv run pytest -m "not infra" -q

preflight: ## Infra preflight against a live stack
	uv run python tests/infra/preflight.py

lint: ## Lint (ruff; shell/yaml lint if installed)
	uv run ruff check .
	@command -v shellcheck >/dev/null && shellcheck scripts/*.sh || echo "shellcheck not installed — skipping"

fmt: ## Auto-format Python
	uv run ruff format .
	uv run ruff check --fix .
