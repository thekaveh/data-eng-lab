from lakehouse.atlas_endpoints import read_env_file, resolve_http_endpoint


def test_read_env_file_uses_last_assignment(tmp_path):
    path = tmp_path / ".env"
    path.write_text("# ignored\nMINIO_PORT=63020\nMINIO_PORT=64020\n")
    assert read_env_file(path) == {"MINIO_PORT": "64020"}


def test_explicit_override_wins_over_export_and_port(tmp_path):
    export = tmp_path / "atlas-consumer.env"
    export.write_text("ATLAS_MINIO_HOST_ENDPOINT=http://localhost:63020\n")
    env_file = tmp_path / ".env"
    env_file.write_text("MINIO_PORT=64020\n")
    value = resolve_http_endpoint(
        "MINIO_HOST_ENDPOINT",
        "MINIO_PORT",
        env={"MINIO_HOST_ENDPOINT": "http://example.test:9000"},
        env_file=env_file,
        export_key="ATLAS_MINIO_HOST_ENDPOINT",
        export_file=export,
    )
    assert value == "http://example.test:9000"


def test_supported_export_wins_over_local_port(tmp_path):
    export = tmp_path / "atlas-consumer.env"
    export.write_text("ATLAS_MINIO_HOST_ENDPOINT=http://localhost:63120\n")
    env_file = tmp_path / ".env"
    env_file.write_text("MINIO_PORT=63020\n")
    value = resolve_http_endpoint(
        "MINIO_HOST_ENDPOINT",
        "MINIO_PORT",
        env={},
        env_file=env_file,
        export_key="ATLAS_MINIO_HOST_ENDPOINT",
        export_file=export,
    )
    assert value == "http://localhost:63120"


def test_unexported_service_uses_resolved_port(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("TRINO_PORT=63140\n")
    assert resolve_http_endpoint(
        "TRINO_HOST_ENDPOINT", "TRINO_PORT", env={}, env_file=env_file
    ) == "http://localhost:63140"
