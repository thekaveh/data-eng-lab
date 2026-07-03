import importlib.util
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("dataset_s3", ROOT / "datasets" / "s3.py")
s3mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(s3mod)


@mock_aws
def test_upload_and_exists_roundtrip(tmp_path: Path):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="landing")
    f = tmp_path / "a.txt"
    f.write_text("hello")

    assert s3mod.object_exists(client, "landing", "nyc_taxi/a.txt") is False
    s3mod.upload_file(client, f, "landing", "nyc_taxi/a.txt")
    assert s3mod.object_exists(client, "landing", "nyc_taxi/a.txt") is True


def test_client_from_env_reads_infra_env(tmp_path: Path):
    infra = tmp_path / "infra"
    infra.mkdir()
    (infra / ".env").write_text(
        "MINIO_ROOT_USER=minioadmin\nMINIO_ROOT_PASSWORD=secret\nMINIO_PORT=64093\n"
    )
    client = s3mod.s3_client_from_env(infra)
    assert client.meta.endpoint_url == "http://localhost:64093"


def test_client_from_env_missing_creds_raises(tmp_path: Path):
    infra = tmp_path / "infra"
    infra.mkdir()
    (infra / ".env").write_text("MINIO_PORT=64093\n")
    with pytest.raises(RuntimeError):
        s3mod.s3_client_from_env(infra)
