from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from datasets.registry import GeneratorScale, load_registry, resolve_scale
from datasets.sources import tpch
from datasets.verification import LockMismatch, VerifiedFile

ROOT = Path(__file__).resolve().parents[2]
TABLES = (
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
)


def _bytes(table: str) -> bytes:
    return f"locked-{table}\n".encode()


@pytest.fixture
def tpch_plan():
    plan = resolve_scale(load_registry(ROOT / "datasets" / "registry.yaml")["tpch"], "tiny")
    assert plan.generator_scale is not None
    outputs = tuple(
        replace(
            output,
            size_bytes=len(_bytes(output.table)),
            sha256=hashlib.sha256(_bytes(output.table)).hexdigest(),
        )
        for output in plan.generator_scale.outputs
    )
    return replace(plan, generator_scale=replace(plan.generator_scale, outputs=outputs))


class FakeRunner:
    def __init__(self, plan):
        contract = plan.dataset.generator
        assert contract is not None
        self.evidence = tpch.ImageEvidence(
            base_image=contract.environment.image,
            base_image_digest=contract.environment.image_digest,
            platform=contract.environment.platform,
            entrypoint=tpch.CANONICAL_ENTRYPOINT,
            environment=tpch.canonical_environment(contract),
            labels=tpch.canonical_labels(contract),
            uv_lock_sha256=contract.environment.uv_lock_sha256,
            duckdb_version=contract.engine_version,
            duckdb_wheel_sha256=contract.engine_wheel_sha256,
            uses_hashed_requirements=True,
        )
        self.metadata_environment = tpch.expected_metadata_environment(contract)
        self.ensure_calls = []
        self.run_args = None
        self.fail_after = None

    def ensure_image(self, contract):
        self.ensure_calls.append(contract)
        return self.evidence

    def run(self, contract, scale, output_root, metadata_path):
        self.run_args = tpch.ContainerRunArgs(
            platform=contract.environment.platform,
            network="none",
            scale=tpch.canonical_scale(scale.scale_factor),
            output_root=output_root,
            metadata_path=metadata_path,
        )
        outputs = {}
        for output in scale.outputs:
            if output.table == self.fail_after:
                raise RuntimeError("simulated container failure")
            path = output_root / output.object_name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(_bytes(output.table))
            outputs[output.table] = {
                "object_name": output.object_name,
                "size_bytes": output.size_bytes,
                "sha256": output.sha256,
            }
        metadata_path.write_text(
            yaml.safe_dump(
                {
                    "scale_factor": scale.scale_factor,
                    "environment": self.metadata_environment,
                    "outputs": outputs,
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )


@pytest.fixture
def fake_runner(tpch_plan):
    return FakeRunner(tpch_plan)


@pytest.fixture(autouse=True)
def accept_test_parquet_schemas(monkeypatch):
    calls = []
    monkeypatch.setattr(tpch, "verify_physical_schema", lambda *args: calls.append(args), raising=False)
    return calls


def test_generate_tpch_uses_network_none_and_exact_platform(tmp_path, fake_runner, tpch_plan):
    tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)

    assert fake_runner.run_args.platform == "linux/amd64"
    assert fake_runner.run_args.network == "none"


def test_generate_tpch_returns_eight_verified_files_in_registry_order(
    tmp_path, fake_runner, tpch_plan, accept_test_parquet_schemas
):
    files = tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)

    assert isinstance(files, tuple)
    assert all(isinstance(item, VerifiedFile) for item in files)
    assert tuple(item.path.name for item in files) == tuple(
        output.object_name for output in tpch_plan.generator_scale.outputs
    )
    assert tuple(item.expected.schema_id for item in files) == TABLES
    assert len(accept_test_parquet_schemas) == 8
    assert not list(tmp_path.glob(".dataset-tpch-*"))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("base_image", "python:drifted"),
        ("base_image_digest", "sha256:" + "0" * 64),
        ("platform", "linux/arm64"),
        ("entrypoint", ("python", "bad.py")),
        ("uv_lock_sha256", "0" * 64),
        ("duckdb_version", "1.5.3"),
        ("duckdb_wheel_sha256", "0" * 64),
        ("uses_hashed_requirements", False),
    ],
)
def test_generate_tpch_rejects_image_evidence_drift(tmp_path, fake_runner, tpch_plan, field, value):
    fake_runner.evidence = replace(fake_runner.evidence, **{field: value})

    with pytest.raises(LockMismatch, match=field):
        tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)

    assert not list(tmp_path.glob("*.parquet"))


def test_generate_tpch_rejects_image_environment_drift(tmp_path, fake_runner, tpch_plan):
    fake_runner.evidence.environment["TZ"] = "Europe/London"
    with pytest.raises(LockMismatch, match="environment"):
        tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)


def test_generate_tpch_rejects_image_label_drift(tmp_path, fake_runner, tpch_plan):
    fake_runner.evidence.labels["io.data-eng-lab.tpch.contract"] = "drifted"
    with pytest.raises(LockMismatch, match="labels"):
        tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("duckdb_version", "1.5.3"),
        ("duckdb_wheel_sha256", "0" * 64),
        ("uv_lock_sha256", "0" * 64),
        ("tpch_extension_sha256", "0" * 64),
        ("locale", "en_US.UTF-8"),
        ("timezone", "Europe/London"),
        ("threads", 4),
        ("preserve_insertion_order", False),
        ("format", "csv"),
        ("compression", "snappy"),
        ("row_group_size", 10),
    ],
)
def test_generate_tpch_rejects_metadata_environment_drift(tmp_path, fake_runner, tpch_plan, field, value):
    fake_runner.metadata_environment[field] = value

    with pytest.raises(LockMismatch, match=field):
        tpch.generate_tpch(tpch_plan, tmp_path, runner=fake_runner)

    assert not list(tmp_path.glob("*.parquet"))


def test_generate_tpch_rejects_scale_factor_drift(tmp_path, fake_runner, tpch_plan):
    fake_runner.run = lambda contract, scale, output_root, metadata_path: metadata_path.write_text(
        yaml.safe_dump(
            {
                "scale_factor": 1.0,
                "environment": fake_runner.metadata_environment,
                "outputs": {},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    with pytest.raises(LockMismatch, match="scale_factor"):
        tpch.generate_tpch(tpch_plan, tmp_path / "scale", runner=fake_runner)


def test_generate_tpch_rejects_unexpected_output_name(tmp_path, fake_runner, tpch_plan):
    original_run = FakeRunner.run
    fake_runner.run = lambda contract, scale, output_root, metadata_path: (
        original_run(fake_runner, contract, scale, output_root, metadata_path),
        (output_root / "unexpected.parquet").write_bytes(b"unexpected"),
    )
    with pytest.raises(LockMismatch, match="object_names"):
        tpch.generate_tpch(tpch_plan, tmp_path / "names", runner=fake_runner)


def test_generate_tpch_rejects_output_byte_drift(tmp_path, fake_runner, tpch_plan):
    original_run = FakeRunner.run

    def corrupt_run(contract, scale, output_root, metadata_path):
        original_run(fake_runner, contract, scale, output_root, metadata_path)
        (output_root / scale.outputs[0].object_name).write_bytes(b"corrupt")

    fake_runner.run = corrupt_run
    with pytest.raises(LockMismatch, match="size_bytes|sha256"):
        tpch.generate_tpch(tpch_plan, tmp_path / "bytes", runner=fake_runner)


def test_generate_tpch_rejects_physical_schema_drift(tmp_path, fake_runner, tpch_plan, monkeypatch):
    original_run = FakeRunner.run
    fake_runner.run = lambda contract, scale, output_root, metadata_path: original_run(
        fake_runner, contract, scale, output_root, metadata_path
    )
    monkeypatch.setattr(
        tpch,
        "verify_physical_schema",
        lambda *args: (_ for _ in ()).throw(LockMismatch(args[2], "schema fingerprint", "locked", "drifted")),
    )
    with pytest.raises(LockMismatch, match="schema fingerprint"):
        tpch.generate_tpch(tpch_plan, tmp_path / "schema", runner=fake_runner)


def test_generate_tpch_is_all_or_nothing_on_runner_or_publication_failure(
    tmp_path, fake_runner, tpch_plan, monkeypatch
):
    destination = tmp_path / "runner"
    fake_runner.fail_after = "part"
    with pytest.raises(RuntimeError, match="container failure"):
        tpch.generate_tpch(tpch_plan, destination, runner=fake_runner)
    assert list(destination.iterdir()) == []

    fake_runner.fail_after = None
    destination = tmp_path / "publication"
    real_link = tpch.os.link
    links = 0

    def fail_second_link(source, target):
        nonlocal links
        links += 1
        if links == 2:
            raise OSError("simulated publication failure")
        real_link(source, target)

    monkeypatch.setattr(tpch.os, "link", fail_second_link)
    with pytest.raises(OSError, match="publication failure"):
        tpch.generate_tpch(tpch_plan, destination, runner=fake_runner)
    assert list(destination.iterdir()) == []


def test_generate_tpch_has_no_host_fallback(tmp_path, tpch_plan, monkeypatch):
    class UnavailableDockerRunner:
        def ensure_image(self, contract):
            raise RuntimeError("Docker linux/amd64 unavailable")

    assert hasattr(tpch, "DockerContainerRunner")
    monkeypatch.setattr(tpch, "DockerContainerRunner", UnavailableDockerRunner)

    with pytest.raises(RuntimeError, match="Docker linux/amd64 unavailable"):
        tpch.generate_tpch(tpch_plan, tmp_path)

    assert not list(tmp_path.glob("*.parquet"))


def test_docker_runner_builds_only_when_absent_or_label_mismatched(tmp_path, tpch_plan, monkeypatch):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    assert hasattr(tpch, "DockerContainerRunner")
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    matching = tpch.canonical_labels(contract)
    inspections = [
        None,
        {"Config": {"Labels": matching}},
        {"Config": {"Labels": matching}},
        {"Config": {"Labels": {}}},
        {"Config": {"Labels": matching}},
    ]
    builds = []
    monkeypatch.setattr(runner, "_inspect_image", lambda: inspections.pop(0))
    monkeypatch.setattr(runner, "_build_image", lambda contract: builds.append(contract))
    monkeypatch.setattr(runner, "_image_evidence", lambda contract, inspection: "evidence")

    assert runner.ensure_image(contract) == "evidence"
    assert runner.ensure_image(contract) == "evidence"
    assert runner.ensure_image(contract) == "evidence"
    assert builds == [contract, contract]


def test_docker_runner_build_command_pins_platform_dockerfile_and_labels(tpch_plan, monkeypatch):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    commands = []
    monkeypatch.setattr(runner, "_execute", lambda command: commands.append(command))

    runner._build_image(contract)

    command = commands[0]
    assert command[:7] == [
        "docker",
        "build",
        "--platform",
        "linux/amd64",
        "--file",
        "datasets/tpch-lock.Dockerfile",
        "--label",
    ]
    assert command[-3:] == ["--tag", "data-eng-lab-tpch-lock:1.5.4", "."]
    assert {command[index + 1] for index, argument in enumerate(command) if argument == "--label"} == {
        f"{key}={value}" for key, value in tpch.canonical_labels(contract).items()
    }


def test_docker_image_inspection_proves_canonical_source_and_runtime_config(tpch_plan):
    contract = tpch_plan.dataset.generator
    assert contract is not None
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    evidence = runner._image_evidence(
        contract,
        {
            "Os": "linux",
            "Architecture": "amd64",
            "Config": {
                "Entrypoint": list(tpch.CANONICAL_ENTRYPOINT),
                "Env": [f"{key}={value}" for key, value in tpch.canonical_environment(contract).items()],
                "Labels": tpch.canonical_labels(contract),
            },
        },
    )

    tpch.verify_image_evidence(
        evidence,
        contract,
        tpch.VerificationContext("tpch", "tiny", "image"),
    )


def test_docker_runner_runtime_command_is_offline_and_does_not_install(tpch_plan, tmp_path, monkeypatch):
    contract = tpch_plan.dataset.generator
    scale = tpch_plan.generator_scale
    assert contract is not None and scale is not None
    assert hasattr(tpch, "DockerContainerRunner")
    runner = tpch.DockerContainerRunner(repository_root=ROOT)
    commands = []

    def execute(command):
        commands.append(command)
        (tmp_path / "output").mkdir()

    monkeypatch.setattr(runner, "_execute", execute)

    runner.run(contract, scale, tmp_path, tmp_path / "metadata.yaml")

    assert commands == [
        [
            "docker",
            "run",
            "--rm",
            "--network=none",
            "--platform",
            "linux/amd64",
            "--volume",
            f"{tmp_path.resolve()}:/out",
            "data-eng-lab-tpch-lock:1.5.4",
            "--scale",
            "0.01",
            "--output-dir",
            "/out/output",
            "--metadata",
            "/out/metadata.yaml",
        ]
    ]
    assert all("INSTALL" not in argument and "pip" not in argument for argument in commands[0])


def test_generate_tpch_requires_typed_generator_plan(tmp_path, tpch_plan, fake_runner):
    with pytest.raises(ValueError, match="generator contract"):
        plan = replace(tpch_plan, dataset=replace(tpch_plan.dataset, generator=None))
        tpch.generate_tpch(plan, tmp_path, fake_runner)
    with pytest.raises(ValueError, match="generator scale"):
        tpch.generate_tpch(replace(tpch_plan, generator_scale=None), tmp_path, fake_runner)
    with pytest.raises(ValueError, match="exactly eight"):
        bad_scale = GeneratorScale("tiny", 0.01, tpch_plan.generator_scale.outputs[:-1])
        tpch.generate_tpch(replace(tpch_plan, generator_scale=bad_scale), tmp_path, fake_runner)
