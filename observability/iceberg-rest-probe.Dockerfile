FROM --platform=linux/amd64 python@sha256:cec9aa7aa96eea4fa036e9b82be1e6b325f2e3707f462d885868df51ec0a4b47

ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    TZ=UTC \
    PYTHONHASHSEED=0 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp

WORKDIR /workspace
COPY scripts/observability /workspace/scripts/observability
RUN find /workspace -type f -name '*.pyc' -delete && \
    find /workspace -type d -name '__pycache__' -empty -delete

USER 65532:65532
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=2).read()"]
ENTRYPOINT ["python", "-m", "scripts.observability.iceberg_rest_probe"]
