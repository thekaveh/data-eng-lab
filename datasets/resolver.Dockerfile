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
COPY datasets /workspace/datasets
COPY lakehouse /workspace/lakehouse

LABEL org.data-eng-lab.uv-lock-sha256="a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1"
USER 65532:65532
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/opt/venv/bin/python", "-c", "import socket,urllib.request; host=socket.inet_ntoa(bytes((127,0,0,1))); urllib.request.urlopen('http://'+host+':8080/healthz', timeout=2).read()"]
ENTRYPOINT ["/opt/venv/bin/python", "-m", "datasets.resolver_service"]
