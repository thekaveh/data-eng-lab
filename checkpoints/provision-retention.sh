#!/bin/sh
set -eu
set +x

: "${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"
: "${MINIO_RETENTION_ACCESS_KEY:?MINIO_RETENTION_ACCESS_KEY is required}"
: "${MINIO_RETENTION_SECRET_KEY:?MINIO_RETENTION_SECRET_KEY is required}"

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null

if mc admin policy info local checkpoint-retention >/dev/null 2>&1; then
    if ! mc admin policy create local checkpoint-retention /config/retention-policy.json >/dev/null 2>&1; then
        mc admin policy remove local checkpoint-retention >/dev/null
        mc admin policy create local checkpoint-retention /config/retention-policy.json >/dev/null
    fi
else
    mc admin policy create local checkpoint-retention /config/retention-policy.json >/dev/null
fi

if mc admin user svcacct info local "$MINIO_RETENTION_ACCESS_KEY" >/dev/null 2>&1; then
    mc admin user svcacct edit local "$MINIO_RETENTION_ACCESS_KEY" \
        --secret-key "$MINIO_RETENTION_SECRET_KEY" \
        --policy /config/retention-policy.json >/dev/null
else
    mc admin user svcacct add local "$MINIO_ROOT_USER" \
        --access-key "$MINIO_RETENTION_ACCESS_KEY" \
        --secret-key "$MINIO_RETENTION_SECRET_KEY" \
        --policy /config/retention-policy.json >/dev/null
fi

printf '%s\n' "checkpoint-retention-init: scoped identity ready"
