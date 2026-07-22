#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
# shellcheck disable=SC1091  # lib.sh path is dynamic (resolved at runtime via $HERE)
source "$HERE/lib.sh"

INFRA_DIR="${INFRA_DIR:-$ROOT/infra}"
MANIFEST="$ROOT/atlas.consumer.yml"
DRY_RUN=0
for a in "$@"; do [ "$a" = "--dry-run" ] && DRY_RUN=1; done

# shellcheck disable=SC2294  # run() intentionally accepts pre-built command strings via eval
run() { if [ "$DRY_RUN" = 1 ]; then echo "+ $*"; else eval "$@"; fi; }

log "1/6 removing stale legacy overlay symlink (pre-manifest layout; _user/ now auto-launches)"
run "rm -f \"$INFRA_DIR/services/_user/data-eng-lab/compose.yml\""

log "2/6 backfilling new Atlas .env keys (additive)"
run "(cd \"$INFRA_DIR\" && ./start.sh env backfill)"

log "3/6 consumer doctor (manifest + compose + env lints)"
run "(cd \"$INFRA_DIR\" && ./start.sh --consumer \"$MANIFEST\" doctor --format json)"

log "4/6 launching Atlas data-eng track (detached; Atlas waits on health gates)"
run "(cd \"$INFRA_DIR\" && ./start.sh --consumer \"$MANIFEST\" --track data-eng --no-tui --detach)"

log "5/6 registering Iceberg medallion namespaces"
run "uv run python \"$ROOT/scripts/register_iceberg.py\""

log "6/6 preflight (stack doctor: layer 1 + layer 2)"
run "uv run python \"$ROOT/tests/infra/preflight.py\""
run "uv run python \"$ROOT/tests/infra/layer2.py\""

log "data-eng-lab is up. Ports: infra/.env; endpoints: (cd infra && ./start.sh endpoints export --format env)."
