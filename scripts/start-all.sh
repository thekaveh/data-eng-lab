#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
source "$HERE/lib.sh"

INFRA_DIR="${INFRA_DIR:-$ROOT/infra}"
DRY_RUN=0
[ "${1:-}" = "--dry-run" ] && DRY_RUN=1

run() { if [ "$DRY_RUN" = 1 ]; then echo "+ $*"; else eval "$@"; fi; }

log "1/5 wiring overlay + .env (setup-overlay)"
run "\"$HERE/setup-overlay.sh\""

log "2/5 launching Atlas data-eng track"
# Non-interactive; backgrounded to avoid the start.py 'logs -f' block (see Atlas reuse notes).
ATLAS_START="cd \"$INFRA_DIR\" && ./start.sh --track data-eng --no-tui \
  --spark-source container --zeppelin-source container --airflow-source container \
  --minio-source container --jupyterhub-source container"
if [ "$DRY_RUN" = 1 ]; then echo "+ $ATLAS_START &"; else eval "$ATLAS_START" & fi

log "3/5 waiting for core services to be healthy"
export PROJECT_NAME="$(resolve_project_name "$INFRA_DIR/.env")"
run "wait_healthy minio spark-master spark-connect airflow-webserver zeppelin jupyterhub"

log "4/5 creating buckets"
run "\"$HERE/create_buckets.sh\""

log "5/5 preflight (stack doctor)"
run "uv run python \"$ROOT/tests/infra/preflight.py\""

log "data-eng-lab is up. Consoles: see 'cd infra && ./start.sh --list-tracks' / infra/.env for ports."
