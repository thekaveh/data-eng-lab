from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from datasets.publication import PublishMode, PublishResult

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("download_datasets", ROOT / "scripts" / "download_datasets.py")
cli = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cli)

REG = ROOT / "datasets" / "registry.yaml"


def test_dry_run_plans_selected_datasets() -> None:
    pairs = cli.plan_uploads(cli.load_registry(REG), "tiny", only=["nyc_taxi"])
    assert pairs == [("nyc_taxi", "tiny")]


def test_fetch_files_uses_current_typed_tpch_signature(tmp_path: Path, monkeypatch) -> None:
    plan = SimpleNamespace(dataset=SimpleNamespace(kind="tpch"), sf=0.001)
    calls: list[tuple[object, Path]] = []
    monkeypatch.setattr(cli, "generate_tpch", lambda selected, dest: calls.append((selected, dest)) or ())

    assert cli._fetch_files(plan, tmp_path) == ()
    assert calls == [(plan, tmp_path)]


def test_run_dispatches_publish_mode_and_preserves_registry_order(tmp_path: Path, monkeypatch) -> None:
    seen: list[tuple[str, PublishMode, bool]] = []

    def fake_publish(plan, *, mode, client, fetcher, rollback_sha256, dry_run, raw_registry_sha256):
        del client, fetcher, rollback_sha256, raw_registry_sha256
        seen.append((plan.dataset.name, mode, dry_run))
        return PublishResult(plan.dataset.name, plan.scale, "dry-run-refresh", None, None, 1)

    monkeypatch.setattr(cli, "publish_dataset", fake_publish)
    code = cli.run(
        REG,
        infra_dir=tmp_path,
        scale="tiny",
        only=["nyc_taxi", "movielens"],
        force=False,
        dry_run=True,
        refresh=True,
        client=object(),
    )

    assert code == 0
    assert seen == [
        ("nyc_taxi", PublishMode.REFRESH, True),
        ("movielens", PublishMode.REFRESH, True),
    ]


def test_run_reports_partial_failure_and_continues(tmp_path: Path, monkeypatch, capsys) -> None:
    seen: list[str] = []

    def fake_publish(plan, **_kwargs):
        seen.append(plan.dataset.name)
        if plan.dataset.name == "nyc_taxi":
            raise RuntimeError("source drift")
        return PublishResult(plan.dataset.name, plan.scale, "verified-existing", "a" * 64, "1" * 32, 1)

    monkeypatch.setattr(cli, "publish_dataset", fake_publish)
    code = cli.run(
        REG,
        infra_dir=tmp_path,
        scale="tiny",
        only=["nyc_taxi", "movielens"],
        force=False,
        dry_run=False,
        client=object(),
    )

    assert code == 1
    assert seen == ["nyc_taxi", "movielens"]
    output = capsys.readouterr()
    assert "nyc_taxi" in output.err
    assert "movielens" in output.out


@pytest.mark.parametrize(
    "argv",
    [
        ["--verify-only", "--refresh"],
        ["--force", "--refresh"],
        ["--verify-only", "--dry-run"],
        ["--rollback-manifest", "a" * 64],
        ["--rollback-manifest", "a" * 64, "--only", "movielens", "--scale", "tiny", "--only", "tpch"],
        ["--rollback-manifest", "not-a-digest", "--only", "movielens", "--scale", "tiny"],
    ],
)
def test_main_rejects_invalid_action_contracts(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        cli.main(argv)
    assert caught.value.code == 2
