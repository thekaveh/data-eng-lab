#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck disable=SC1091  # lib.sh path is dynamic (resolved at runtime via $HERE)
source "$HERE/lib.sh"

INFRA_DIR="${INFRA_DIR:-$ROOT/infra}"
DRY_RUN=0
for a in "$@"; do [ "$a" = "--dry-run" ] && DRY_RUN=1; done

# shellcheck disable=SC2294  # run() intentionally accepts pre-built command strings via eval
run() { if [ "$DRY_RUN" = 1 ]; then echo "+ $*"; else eval "$@"; fi; }

log "1/5 wiring overlay + .env (setup-overlay)"
run "\"$HERE/setup-overlay.sh\""

log "2/5 launching Atlas data-eng track"
# Non-interactive; backgrounded to avoid the start.py 'logs -f' block (see Atlas reuse notes).
ATLAS_START="cd \"$INFRA_DIR\" && ./start.sh --track data-eng --no-tui \
  --spark-source container --zeppelin-source container --airflow-source container \
  --minio-source container --jupyterhub-source container"
# shellcheck disable=SC2294  # intentional backgrounded eval of a pre-built command string
if [ "$DRY_RUN" = 1 ]; then echo "+ $ATLAS_START &"; else eval "$ATLAS_START" & fi

log "3/5 waiting for core services to be healthy"
PROJECT_NAME="$(resolve_project_name "$INFRA_DIR/.env")"
export PROJECT_NAME
run "wait_healthy minio spark-master spark-connect airflow-webserver zeppelin jupyterhub"

log "4/5 creating buckets"
run "\"$HERE/create_buckets.sh\""

log "5/5 preflight (stack doctor)"
run "uv run python \"$ROOT/tests/infra/preflight.py\""

log "data-eng-lab is up. Consoles: see 'cd infra && ./start.sh --list-tracks' / infra/.env for ports."
