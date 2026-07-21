# Atlas pin-bump runbook

How to move the `infra` submodule to a newer Atlas commit using only Atlas's
official headless commands (adapted from tableau's runbook; validated on the
`85ff46b2 → 2d006cae` bump, 2026-07-21).

## Steps

    # 1. Move the pin (target must be an ancestor of atlas origin/main).
    git -C infra fetch origin
    git -C infra checkout <sha>

    # 2. Backfill any NEW upstream .env keys (additive; never rewrites values).
    (cd infra && ./start.sh env backfill)

    # 3. Consumer doctor: manifest validity, compose validity, overlay env
    #    resolution, submodule cleanliness.
    (cd infra && ./start.sh --consumer "$PWD/../atlas.consumer.yml" doctor --format json)

    # 4. Offline suite + repo verifier.
    uv run pytest -m "not infra and not network" -q
    make verify

    # 5. Rebuild locally-built images (atlas#506, open): a warm start reuses
    #    cached airflow/jupyterhub/spark/jenkins images, silently running
    #    pre-bump code. Cold-start (or targeted compose build) before relying
    #    on the new pin:
    make down COLD=1 && make up

    # 6. Live preflight + commit the pointer.
    make preflight
    git add infra && git commit -m "chore: bump Atlas pin to <sha> (<why>)"

## Notes

- `BASE_PORT: auto` re-resolves if another stack took the block (atlas#780);
  ports live in `infra/.env` after start.
- If bring-up dies on port conflicts, another project's containers are squatting
  the range: `docker ps --format '{{.Names}}\t{{.Ports}}'` finds them.
- A bump that changes Atlas *behavior* (not just versions) warrants re-running
  the full go-live checks (`docs/go-live.md`) before merging.
