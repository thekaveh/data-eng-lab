from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_probe_dockerfile_is_minimal_pinned_and_nonroot() -> None:
    text = (ROOT / "observability/iceberg-rest-probe.Dockerfile").read_text()
    assert (
        "FROM --platform=linux/amd64 "
        "python@sha256:cec9aa7aa96eea4fa036e9b82be1e6b325f2e3707f462d885868df51ec0a4b47" in text
    )
    assert "COPY scripts/observability /workspace/scripts/observability" in text
    assert "COPY . " not in text
    assert "find /workspace -type f -name '*.pyc' -delete" in text
    assert "USER 65532:65532" in text
    assert 'ENTRYPOINT ["python", "-m", "scripts.observability.iceberg_rest_probe"]' in text
