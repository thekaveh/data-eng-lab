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

log "1/8 removing stale legacy overlay symlink (pre-manifest layout; _user/ now auto-launches)"
run "rm -f \"$INFRA_DIR/services/_user/data-eng-lab/compose.yml\""

log "2/8 backfilling new Atlas .env keys (additive)"
run "(cd \"$INFRA_DIR\" && ./start.sh env backfill)"

log "3/8 consumer compose validation"
run "(cd \"$INFRA_DIR\" && ./start.sh --consumer \"$MANIFEST\" compose validate)"

log "4/8 consumer doctor"
run "(cd \"$INFRA_DIR\" && ./start.sh --consumer \"$MANIFEST\" doctor --format json)"

log "5/8 launching Atlas data-eng track (detached; Atlas waits on health gates)"
run "(cd \"$INFRA_DIR\" && ./start.sh --consumer \"$MANIFEST\" --track data-eng --no-tui --detach)"

log "6/8 exporting and asserting the supported endpoint contract"
run "(cd \"$INFRA_DIR\" && ./start.sh --consumer \"$MANIFEST\" endpoints export --format env --output \"$ROOT/atlas-consumer.env\")"
run "(cd \"$INFRA_DIR\" && ./start.sh --consumer \"$MANIFEST\" endpoints assert --require ATLAS_MINIO_HOST_ENDPOINT)"

log "7/8 registering Iceberg medallion namespaces"
run "uv run python \"$ROOT/scripts/register_iceberg.py\""

log "8/8 preflight (stack doctor: layer 1 + layer 2)"
run "uv run python \"$ROOT/tests/infra/preflight.py\""
run "uv run python \"$ROOT/tests/infra/layer2.py\""

log "data-eng-lab is up. Endpoint export: atlas-consumer.env."
