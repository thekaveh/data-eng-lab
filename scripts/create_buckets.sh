#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
source "$HERE/lib.sh"

INFRA_DIR="${INFRA_DIR:-$ROOT/infra}"
ENV_FILE="$INFRA_DIR/.env"
PROJECT_NAME="${PROJECT_NAME:-$(envval PROJECT_NAME "$ENV_FILE")}"
PROJECT_NAME="${PROJECT_NAME:-data-eng-lab}"

USER="$(envval MINIO_ROOT_USER "$ENV_FILE")"
PASS="$(envval MINIO_ROOT_PASSWORD "$ENV_FILE")"
BUCKETS=(landing lakehouse jars checkpoints lakehouse-test)

# Default: a transient mc container on the Atlas network, aliased to the in-network MinIO.
# Overridable via MC_CMD (used by tests and by callers that already have mc configured).
MC_CMD="${MC_CMD:-docker run --rm --network ${PROJECT_NAME}-network \
  -e MC_HOST_local=http://${USER}:${PASS}@minio:9000 minio/mc}"

log "creating buckets: ${BUCKETS[*]}"
for b in "${BUCKETS[@]}"; do
  # 'local/' alias when using the default docker mc; stub ignores the alias prefix.
  $MC_CMD mb --ignore-existing "local/${b}"
done
log "buckets ready"
