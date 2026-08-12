from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from datasets.publication import PublicationFailure, PublishMode, PublishResult

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


def test_fetch_files_uses_current_typed_http_signature(tmp_path: Path, monkeypatch) -> None:
    plan = SimpleNamespace(dataset=SimpleNamespace(kind="http"))
    calls: list[tuple[object, Path]] = []
    monkeypatch.setattr(cli, "fetch_http", lambda selected, dest: calls.append((selected, dest)) or ())

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


def test_run_surfaces_inventory_state_and_residue_warning(tmp_path: Path, monkeypatch, capsys) -> None:
    def fake_publish(plan, **_kwargs):
        return PublishResult(
            plan.dataset.name,
            plan.scale,
            "published",
            "a" * 64,
            "1" * 32,
            1,
            cleanup_warning="owned staging residue retained after descriptor-safe cleanup",
            inventory_state="unavailable-warning",
        )

    monkeypatch.setattr(cli, "publish_dataset", fake_publish)
    code = cli.run(
        REG,
        infra_dir=tmp_path,
        scale="tiny",
        only=["nyc_taxi"],
        force=False,
        dry_run=False,
        refresh=True,
        client=object(),
    )

    captured = capsys.readouterr()
    assert code == 0
    assert '"inventory_state":"unavailable-warning"' in captured.out
    assert "owned staging residue" in captured.err


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


def test_run_surfaces_structured_cleanup_warning_from_publication_failure(tmp_path: Path, monkeypatch, capsys) -> None:
    cleanup = "owned publication staging cleanup failed: OSError"
    cause = OSError("cleanup")
    cause.add_note(cleanup)

    def fail(plan, **_kwargs):
        result = PublishResult(
            plan.dataset.name,
            plan.scale,
            "failed-candidate",
            None,
            "1" * 32,
            0,
            cleanup_warning=cleanup,
        )
        raise PublicationFailure(result, cause)

    monkeypatch.setattr(cli, "publish_dataset", fail)
    code = cli.run(
        REG,
        infra_dir=tmp_path,
        scale="tiny",
        only=["nyc_taxi"],
        force=False,
        dry_run=False,
        client=object(),
    )

    captured = capsys.readouterr()
    assert code == 1
    assert f'"cleanup_warning":"{cleanup}"' in captured.err
    assert f"note: {cleanup}" in captured.err


def test_resolve_failure_is_per_dataset_and_does_not_abort_remaining_work(tmp_path, monkeypatch) -> None:
    original = cli.resolve_scale
    published: list[str] = []

    def resolve(dataset, scale):
        if dataset.name == "nyc_taxi":
            raise ValueError("bad selected scale")
        return original(dataset, scale)

    monkeypatch.setattr(cli, "resolve_scale", resolve)
    monkeypatch.setattr(
        cli,
        "publish_dataset",
        lambda plan, **kwargs: (
            published.append(plan.dataset.name)
            or PublishResult(plan.dataset.name, plan.scale, "verified-existing", "a" * 64, "1" * 32, 1)
        ),
    )

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
    assert published == ["movielens"]


@pytest.mark.parametrize(
    ("argv", "environment", "expected"),
    [
        ([], {}, "small"),
        ([], {"DATASET_SCALE": "medium"}, "medium"),
        (["--scale", "tiny"], {"DATASET_SCALE": "medium"}, "tiny"),
    ],
)
def test_scale_precedence(argv, environment, expected, monkeypatch) -> None:
    monkeypatch.delenv("DATASET_SCALE", raising=False)
    for key, value in environment.items():
        monkeypatch.setenv(key, value)
    seen: list[str] = []
    monkeypatch.setattr(cli, "run", lambda *args, **kwargs: seen.append(args[2]) or 0)

    assert cli.main(argv) == 0
    assert seen == [expected]


def test_invalid_dataset_scale_environment_fails_before_run(monkeypatch) -> None:
    monkeypatch.setenv("DATASET_SCALE", "huge")
    monkeypatch.setattr(cli, "run", lambda *args, **kwargs: pytest.fail("run"))

    with pytest.raises(SystemExit) as caught:
        cli.main([])
    assert caught.value.code == 2


def test_force_emits_deprecation_warning(monkeypatch, capsys) -> None:
    calls: list[tuple[bool, bool]] = []
    monkeypatch.setattr(
        cli,
        "run",
        lambda *args, **kwargs: calls.append((args[4], kwargs["refresh"])) or 0,
    )
    assert cli.main(["--force"]) == 0
    assert "deprecated" in capsys.readouterr().err
    assert calls == [(True, False)]
    assert cli._publish_mode(force=True, refresh=False, verify_only=False, rollback=None) is PublishMode.REFRESH


def test_registry_snapshot_reads_original_bytes_once(monkeypatch) -> None:
    calls = 0
    original = Path.read_bytes

    def counted(path):
        nonlocal calls
        if path == REG:
            calls += 1
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", counted)
    registry, digest = cli._load_registry_snapshot(REG)

    assert calls == 1
    assert registry
    assert len(digest) == 64


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
