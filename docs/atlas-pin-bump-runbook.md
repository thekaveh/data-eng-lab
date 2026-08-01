# 8.7. Atlas Pin-Bump Runbook

How to move the `infra` submodule to a newer Atlas commit using only Atlas's
official headless commands (adapted from tableau's runbook; validated on the
`85ff46b2 → 2d006cae` bump, 2026-07-21).

## 1. Steps

    # 1. Move the pin (target must be an ancestor of atlas origin/main).
    git -C infra fetch origin
    git -C infra checkout <sha>

    # 2. Backfill any NEW upstream .env keys (additive; never rewrites values).
    (cd infra && ./start.sh env backfill)

    # 3. Consumer Compose validation and doctor: manifest validity, overlay env
    #    resolution, compose validity, and submodule cleanliness.
    (cd infra && ./start.sh --consumer "$PWD/../atlas.consumer.yml" compose validate)
    (cd infra && ./start.sh --consumer "$PWD/../atlas.consumer.yml" doctor --format json)

    # 4. Offline suite + repo verifier.
    uv run pytest -m "not infra and not network" -q
    make verify

    # 5. Start through the consumer launcher. Atlas automatically adds --build
    #    after the Atlas source commit changes; it records the built commit in
    #    the ignored .atlas-build-state file so a warm start cannot silently
    #    reuse locally-built images from the prior pin.
    make up

    # 6. The generated, ignored export is the supported host-endpoint contract.
    #    This consumer currently requires only the MinIO export.
    (cd infra && ./start.sh --consumer "$PWD/../atlas.consumer.yml" \
      endpoints assert --require ATLAS_MINIO_HOST_ENDPOINT)

    # 7. Live preflight + commit the pointer.
    make preflight
    git add infra && git commit -m "chore: bump Atlas pin to <sha> (<why>)"

## 2. Notes

- `BASE_PORT: auto` re-resolves if another stack took the block (atlas#780);
  ports live in `infra/.env` after start.
- `atlas-consumer.env` is generated and ignored. It currently exports only
  `ATLAS_MINIO_HOST_ENDPOINT`; host-side consumers resolve Iceberg REST, Trino,
  Redpanda, Zeppelin, and Airflow through an explicit override or `infra/.env`.
- If bring-up dies on port conflicts, another project's containers are squatting
  the range: `docker ps --format '{{.Names}}\t{{.Ports}}'` finds them.
- Use Docker-network DNS (for example, `iceberg-rest:8181` or `trino:8080`) for
  code that runs inside Atlas containers; host ports are for host-side tools only.
- A cold reset is still appropriate when intentionally deleting volumes or when
  testing uncommitted Atlas Dockerfile edits; it is not required for a committed
  pin change.
- A bump that changes Atlas *behavior* (not just versions) warrants re-running
  the full go-live checks (`docs/go-live.md`) before merging.
