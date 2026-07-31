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

print_command() {
  printf '+ '
  printf '%q ' "$@"
  printf '\n'
}

run() {
  if [ "$DRY_RUN" = 1 ]; then
    print_command "$@"
  else
    "$@"
  fi
}

run_atlas() {
  if [ "$DRY_RUN" = 1 ]; then
    printf '+ (cd %q && ' "$INFRA_DIR"
    printf '%q ' ./start.sh "$@"
    printf ')\n'
  else
    (
      cd "$INFRA_DIR"
      ./start.sh "$@"
    )
  fi
}

log "1/8 removing stale legacy overlay symlink (pre-manifest layout; _user/ now auto-launches)"
run rm -f "$INFRA_DIR/services/_user/data-eng-lab/compose.yml"

log "2/8 backfilling new Atlas .env keys (additive)"
run_atlas --consumer "$MANIFEST" env backfill

log "3/8 consumer compose validation"
run_atlas --consumer "$MANIFEST" compose validate

log "4/8 consumer doctor"
run_atlas --consumer "$MANIFEST" doctor --format json

log "5/8 launching Atlas data-eng track (detached; Atlas waits on health gates)"
run_atlas --consumer "$MANIFEST" --track data-eng --no-tui --detach

log "6/8 exporting and asserting the supported endpoint contract"
run_atlas --consumer "$MANIFEST" endpoints export --format env --output "$ROOT/atlas-consumer.env"
run_atlas --consumer "$MANIFEST" endpoints assert --require ATLAS_MINIO_HOST_ENDPOINT

log "7/8 registering Iceberg medallion namespaces"
run uv run python "$ROOT/scripts/register_iceberg.py"

log "8/8 preflight (stack doctor: layer 1 + layer 2)"
run uv run python "$ROOT/tests/infra/preflight.py"
run uv run python "$ROOT/tests/infra/layer2.py"

log "data-eng-lab is up. Endpoint export: atlas-consumer.env."
