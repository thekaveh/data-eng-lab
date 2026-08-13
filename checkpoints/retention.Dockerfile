FROM ghcr.io/astral-sh/uv@sha256:4de5495181a281bc744845b9579acf7b221d6791f99bcc211b9ec13f417c2853 AS uv
FROM --platform=linux/amd64 python@sha256:cec9aa7aa96eea4fa036e9b82be1e6b325f2e3707f462d885868df51ec0a4b47

ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=UTC \
    PYTHONHASHSEED=0 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_CACHE_DIR=/tmp/uv-cache

COPY --from=uv /uv /uvx /bin/
WORKDIR /workspace
COPY pyproject.toml uv.lock /workspace/
RUN echo "a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1  /workspace/uv.lock" | sha256sum -c - && \
    uv sync --frozen --only-group dev --no-install-project && \
    rm -rf /tmp/uv-cache
COPY scripts/checkpoints /workspace/scripts/checkpoints
COPY checkpoints/retention-policy.yaml /workspace/checkpoints/retention-policy.yaml
RUN find /opt/venv /workspace -type f -name '*.pyc' -delete && \
    find /opt/venv /workspace -type d -name '__pycache__' -empty -delete

USER 65532:65532
ENTRYPOINT ["/opt/venv/bin/python", "-m", "scripts.checkpoints.service"]
