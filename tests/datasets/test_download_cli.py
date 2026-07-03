import importlib.util
from pathlib import Path

import boto3
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("download_datasets", ROOT / "scripts" / "download_datasets.py")
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)

REG = ROOT / "datasets" / "registry.yaml"


def test_dry_run_plans_selected_datasets():
    pairs = cli.plan_uploads(cli.load_registry(REG), "tiny", only=["nyc_taxi"])
    assert pairs == [("nyc_taxi", "tiny")]


@mock_aws
def test_run_uploads_http_dataset(tmp_path: Path, monkeypatch):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="landing")

    # stub the http fetcher to avoid the network: land two fake files
    def fake_fetch(plan, dest):
        dest.mkdir(parents=True, exist_ok=True)
        a = dest / "part-0.parquet"
        a.write_bytes(b"X")
        return [a]

    monkeypatch.setattr(cli, "fetch_http", fake_fetch)

    n = cli.run(REG, infra_dir=tmp_path, scale="tiny", only=["nyc_taxi"],
                force=False, dry_run=False, client=client)
    assert n == 1
    assert cli.object_exists(client, "landing", "nyc_taxi/part-0.parquet")

    # idempotent: second run skips the existing object
    n2 = cli.run(REG, infra_dir=tmp_path, scale="tiny", only=["nyc_taxi"],
                 force=False, dry_run=False, client=client)
    assert n2 == 0
