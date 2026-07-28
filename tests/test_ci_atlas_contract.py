from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_ci_has_a_pinned_atlas_consumer_contract_job():
    text = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    for required in (
        "atlas-consumer-contract:",
        "submodules: recursive",
        "cp infra/.env.example infra/.env",
        "./start.sh env backfill",
        "--consumer ../atlas.consumer.yml compose validate",
        "--consumer ../atlas.consumer.yml doctor --format json",
    ):
        assert required in text
