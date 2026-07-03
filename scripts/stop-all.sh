#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
source "$HERE/lib.sh"
INFRA_DIR="${INFRA_DIR:-$ROOT/infra}"

DRY_RUN=0
COLD=""
for a in "$@"; do
  [ "$a" = "--dry-run" ] && DRY_RUN=1
  [ "$a" = "--cold" ] && COLD="--cold"
done

CMD="cd \"$INFRA_DIR\" && ./stop.sh $COLD"
if [ "$DRY_RUN" = 1 ]; then echo "+ $CMD"; else eval "$CMD"; fi
log "data-eng-lab stopped ${COLD:+(volumes wiped)}"
