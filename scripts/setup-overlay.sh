#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
source "$HERE/lib.sh"

INFRA_DIR="${INFRA_DIR:-$ROOT/infra}"
ENV_FILE="$INFRA_DIR/.env"
SLOT_DIR="$INFRA_DIR/services/_user/data-eng-lab"

log "linking overlay into Atlas user slot: $SLOT_DIR"
mkdir -p "$SLOT_DIR"
# Relative link so it resolves the same on any checkout: slot -> repo compose file.
ln -sf "$ROOT/compose/data-eng-lab.yml" "$SLOT_DIR/compose.yml"

log "injecting config into $ENV_FILE"
set_env         PROJECT_NAME     data-eng-lab                     "$ENV_FILE"
set_env         ICEBERG_REST_URI http://iceberg-rest:8181         "$ENV_FILE"
set_env_default BRAND_NAME       "data-eng-lab"                   "$ENV_FILE"

log "setup-overlay complete"
