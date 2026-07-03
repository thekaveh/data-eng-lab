# Contributing to data-eng-lab

## Workflow
1. Branch off `main`.
2. Make the change (follow existing patterns; keep files focused).
3. `make verify` (repo structure/oracle) must pass.
4. `make lint` and `make test` (static + unit) must pass.
5. Against a live stack: `make preflight` and relevant `make test-int` must pass.
6. Open a PR — one concern per PR.

## Adding a scenario
See [`docs/scenarios.md`](docs/scenarios.md) for the step-by-step recipe.

## Conventions
- Never edit `infra/` (the Atlas submodule).
- Scenario folders: `[pattern]-[dataset]-[engine]-[format]`, flat under `scenarios/`.
- Every scenario ships both a Zeppelin Scala and a Jupyter PySpark notebook + a 6-section README.
- Bootstrap scripts are idempotent and read secrets from `infra/.env` (never hardcode).
