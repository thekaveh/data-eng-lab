"""Generate locked TPC-H outputs with the canonical offline container."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

import yaml

from datasets import acquisition
from datasets.registry import GeneratorContract, GeneratorOutput, GeneratorScale, ScalePlan
from datasets.schema_inspection import verify_physical_schema
from datasets.verification import (
    ExpectedObject,
    LockMismatch,
    VerificationContext,
    VerifiedFile,
    require_exact_names,
    verify_file,
)

TPCH_TABLES = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]
CANONICAL_ENTRYPOINT = ("python", "-m", "datasets.tpch_lock_export")
IMAGE_TAG = "data-eng-lab-tpch-lock:1.5.4"
_ROOT = Path(__file__).resolve().parents[2]
_DOCKERFILE = Path("datasets/tpch-lock.Dockerfile")
_REQUIREMENTS = Path("datasets/tpch-lock-requirements.txt")
_EXPORTER = Path("datasets/tpch_lock_export.py")
_LABEL_PREFIX = "io.data-eng-lab.tpch."


@dataclass(frozen=True)
class ImageEvidence:
    base_image: str
    base_image_digest: str
    platform: str
    entrypoint: tuple[str, ...]
    environment: Mapping[str, str]
    labels: Mapping[str, str]
    uv_lock_sha256: str
    duckdb_version: str
    duckdb_wheel_sha256: str
    uses_hashed_requirements: bool


@dataclass(frozen=True)
class ContainerRunArgs:
    platform: str
    network: str
    scale: str
    output_root: Path
    metadata_path: Path


class ContainerRunner(Protocol):
    def ensure_image(self, contract: GeneratorContract) -> ImageEvidence: ...

    def run(
        self,
        contract: GeneratorContract,
        scale: GeneratorScale,
        output_root: Path,
        metadata_path: Path,
    ) -> None: ...


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_environment(contract: GeneratorContract) -> dict[str, str]:
    return {
        "LANG": contract.environment.locale,
        "LC_ALL": contract.environment.locale,
        "TZ": contract.environment.timezone,
        "PYTHONHASHSEED": "0",
    }


def canonical_labels(
    contract: GeneratorContract,
    repository_root: Path = _ROOT,
) -> dict[str, str]:
    root = Path(repository_root)
    return {
        f"{_LABEL_PREFIX}base-image": contract.environment.image,
        f"{_LABEL_PREFIX}base-digest": contract.environment.image_digest,
        f"{_LABEL_PREFIX}platform": contract.environment.platform,
        f"{_LABEL_PREFIX}duckdb-version": contract.engine_version,
        f"{_LABEL_PREFIX}duckdb-wheel-sha256": contract.engine_wheel_sha256,
        f"{_LABEL_PREFIX}extension-sha256": contract.extension_sha256,
        f"{_LABEL_PREFIX}uv-lock-sha256": contract.environment.uv_lock_sha256,
        f"{_LABEL_PREFIX}dockerfile-sha256": _sha256(root / _DOCKERFILE),
        f"{_LABEL_PREFIX}requirements-sha256": _sha256(root / _REQUIREMENTS),
        f"{_LABEL_PREFIX}exporter-sha256": _sha256(root / _EXPORTER),
    }


def expected_metadata_environment(contract: GeneratorContract) -> dict[str, object]:
    return {
        "platform": contract.environment.platform,
        "base_image_digest": contract.environment.image_digest,
        "duckdb_version": contract.engine_version,
        "duckdb_wheel_sha256": contract.engine_wheel_sha256,
        "uv_lock_sha256": contract.environment.uv_lock_sha256,
        "tpch_extension_sha256": contract.extension_sha256,
        "locale": contract.environment.locale,
        "timezone": contract.environment.timezone,
        "threads": contract.environment.threads,
        "preserve_insertion_order": contract.environment.preserve_insertion_order,
        "format": contract.export_format,
        "compression": contract.compression,
        "row_group_size": contract.row_group_size,
    }


def canonical_scale(scale_factor: float) -> str:
    scales = {0.01: "0.01", 1.0: "1", 10.0: "10"}
    try:
        return scales[scale_factor]
    except KeyError as error:
        raise ValueError(f"unsupported canonical TPC-H scale factor: {scale_factor}") from error


class DockerContainerRunner:
    """Build, inspect, and run the canonical TPC-H image through Docker."""

    def __init__(self, repository_root: Path = _ROOT, image_tag: str = IMAGE_TAG):
        self.repository_root = Path(repository_root).resolve()
        self.image_tag = image_tag

    def _execute(self, command: list[str]) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                command,
                cwd=self.repository_root,
                check=True,
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as error:
            raise RuntimeError("Docker is required for canonical TPC-H generation") from error
        except subprocess.CalledProcessError as error:
            detail = error.stderr.strip() or error.stdout.strip() or "Docker command failed"
            raise RuntimeError(detail) from error

    def _inspect_image(self) -> dict[str, object] | None:
        try:
            result = self._execute(["docker", "image", "inspect", self.image_tag])
        except RuntimeError as error:
            if "No such image" in str(error) or "No such object" in str(error):
                return None
            raise
        document = json.loads(result.stdout)
        if not isinstance(document, list) or len(document) != 1 or not isinstance(document[0], dict):
            raise RuntimeError("Docker returned invalid image inspection evidence")
        return cast(dict[str, object], document[0])

    def _build_image(self, contract: GeneratorContract) -> None:
        command = [
            "docker",
            "build",
            "--platform",
            contract.environment.platform,
            "--file",
            str(_DOCKERFILE),
        ]
        for key, value in canonical_labels(contract, self.repository_root).items():
            command.extend(("--label", f"{key}={value}"))
        command.extend(("--tag", self.image_tag, "."))
        self._execute(command)

    def _source_evidence(self, contract: GeneratorContract) -> tuple[str, str, str, str, bool]:
        dockerfile = (self.repository_root / _DOCKERFILE).read_text(encoding="utf-8")
        requirements = (self.repository_root / _REQUIREMENTS).read_text(encoding="utf-8")
        base = re.search(r"^FROM --platform=([^ ]+) ([^@\s]+)@(sha256:[0-9a-f]{64})$", dockerfile, re.MULTILINE)
        wheel = re.search(r"^duckdb==([^ ]+) --hash=sha256:([0-9a-f]{64})$", requirements, re.MULTILINE)
        if base is None or wheel is None:
            raise RuntimeError("canonical TPC-H Docker inputs are malformed")
        hashed = (
            "pip install --no-cache-dir --require-hashes" in dockerfile
            and f"{_REQUIREMENTS.as_posix()} /tmp/requirements.txt" in dockerfile
            and f"{contract.environment.uv_lock_sha256}  /workspace/uv.lock" in dockerfile
            and f"{contract.extension_sha256}  /root/.duckdb/extensions/" in dockerfile
            and contract.environment.image.rsplit(":", 1)[0] == base.group(2)
        )
        return base.group(2), base.group(3), wheel.group(1), wheel.group(2), hashed

    def _image_evidence(
        self,
        contract: GeneratorContract,
        inspection: Mapping[str, object],
    ) -> ImageEvidence:
        config = inspection.get("Config")
        if not isinstance(config, Mapping):
            raise RuntimeError("Docker image inspection omitted Config")
        raw_environment = config.get("Env", ())
        raw_labels = config.get("Labels", {})
        if not isinstance(raw_environment, Sequence) or isinstance(raw_environment, str):
            raise RuntimeError("Docker image inspection returned invalid environment")
        if not isinstance(raw_labels, Mapping):
            raise RuntimeError("Docker image inspection returned invalid labels")
        environment = {
            item.split("=", 1)[0]: item.split("=", 1)[1]
            for item in raw_environment
            if isinstance(item, str) and "=" in item
        }
        expected_labels = canonical_labels(contract, self.repository_root)
        labels = {key: raw_labels.get(key) for key in expected_labels}
        _base_repository, base_digest, duckdb_version, wheel_sha256, hashed = self._source_evidence(contract)
        operating_system = inspection.get("Os")
        architecture = inspection.get("Architecture")
        return ImageEvidence(
            base_image=cast(str, labels.get(f"{_LABEL_PREFIX}base-image")),
            base_image_digest=base_digest,
            platform=f"{operating_system}/{architecture}",
            entrypoint=tuple(config.get("Entrypoint") or ()),
            environment={key: environment.get(key, "") for key in canonical_environment(contract)},
            labels=cast(Mapping[str, str], labels),
            uv_lock_sha256=_sha256(self.repository_root / "uv.lock"),
            duckdb_version=duckdb_version,
            duckdb_wheel_sha256=wheel_sha256,
            uses_hashed_requirements=hashed,
        )

    def ensure_image(self, contract: GeneratorContract) -> ImageEvidence:
        inspection = self._inspect_image()
        expected_labels = canonical_labels(contract, self.repository_root)
        actual_labels = {}
        if inspection is not None and isinstance(inspection.get("Config"), Mapping):
            labels = cast(Mapping[str, object], inspection["Config"]).get("Labels")
            if isinstance(labels, Mapping):
                actual_labels = labels
        if inspection is None or any(actual_labels.get(key) != value for key, value in expected_labels.items()):
            self._build_image(contract)
            inspection = self._inspect_image()
            if inspection is None:
                raise RuntimeError("canonical TPC-H image is unavailable after build")
        return self._image_evidence(contract, inspection)

    def run(
        self,
        contract: GeneratorContract,
        scale: GeneratorScale,
        output_root: Path,
        metadata_path: Path,
    ) -> None:
        generated = output_root / "output"
        command = [
            "docker",
            "run",
            "--rm",
            "--network=none",
            "--platform",
            contract.environment.platform,
            "--volume",
            f"{output_root.resolve()}:/out",
            self.image_tag,
            "--scale",
            canonical_scale(scale.scale_factor),
            "--output-dir",
            "/out/output",
            "--metadata",
            f"/out/{metadata_path.name}",
        ]
        self._execute(command)
        if not generated.is_dir():
            raise RuntimeError("canonical TPC-H container did not create its output directory")
        for path in generated.iterdir():
            path.rename(output_root / path.name)
        generated.rmdir()


def require_generator_contract(contract: GeneratorContract | None) -> GeneratorContract:
    if contract is None:
        raise ValueError("TPC-H plan requires a generator contract")
    return contract


def require_generator_scale(scale: GeneratorScale | None) -> GeneratorScale:
    if scale is None:
        raise ValueError("TPC-H plan requires a generator scale")
    if len(scale.outputs) != 8:
        raise ValueError("TPC-H generator scale must lock exactly eight outputs")
    return scale


def _mismatch(context: VerificationContext, field: str, expected: object, actual: object) -> None:
    if actual != expected:
        raise LockMismatch(context, field, expected, actual)


def verify_image_evidence(
    evidence: ImageEvidence,
    contract: GeneratorContract,
    context: VerificationContext,
) -> None:
    expected = {
        "base_image": contract.environment.image,
        "base_image_digest": contract.environment.image_digest,
        "platform": contract.environment.platform,
        "entrypoint": CANONICAL_ENTRYPOINT,
        "environment": canonical_environment(contract),
        "labels": canonical_labels(contract),
        "uv_lock_sha256": contract.environment.uv_lock_sha256,
        "duckdb_version": contract.engine_version,
        "duckdb_wheel_sha256": contract.engine_wheel_sha256,
        "uses_hashed_requirements": True,
    }
    for field, value in expected.items():
        _mismatch(context, field, value, getattr(evidence, field))


@contextmanager
def owned_directory(destination: Path) -> Iterator[Path]:
    destination.mkdir(parents=True, exist_ok=True)
    acquisition._require_trusted_parent(destination)
    root = Path(tempfile.mkdtemp(prefix=".dataset-tpch-", dir=destination))
    status = root.lstat()
    identity = (status.st_dev, status.st_ino)
    primary: BaseException | None = None
    try:
        yield root
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            acquisition._quarantine_owned_path(root, identity, directory=True)
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note(f"TPC-H transaction cleanup failed: {cleanup_error}")


def _metadata_document(path: Path, context: VerificationContext) -> Mapping[str, object]:
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as error:
        raise LockMismatch(context, "metadata", "valid exporter metadata", type(error).__name__) from error
    if not isinstance(document, Mapping):
        raise LockMismatch(context, "metadata", "mapping", type(document).__name__)
    return document


def _expected_object(output: GeneratorOutput) -> ExpectedObject:
    return ExpectedObject(
        object_name=output.object_name,
        size_bytes=output.size_bytes,
        sha256=output.sha256,
        schema_id=output.schema_id,
    )


def verify_tpch_outputs(
    plan: ScalePlan,
    output_root: Path,
    metadata_path: Path,
) -> tuple[VerifiedFile, ...]:
    contract = require_generator_contract(plan.dataset.generator)
    scale = require_generator_scale(plan.generator_scale)
    context = VerificationContext(plan.dataset.name, plan.scale, "generator metadata")
    expected_tables = tuple(output.table for output in scale.outputs)
    expected_names = tuple(output.object_name for output in scale.outputs)
    _mismatch(context, "tables", tuple(contract.order_by), expected_tables)
    _mismatch(context, "output_count", 8, len(expected_names))

    document = _metadata_document(metadata_path, context)
    _mismatch(
        context,
        "metadata_fields",
        ("scale_factor", "environment", "outputs"),
        tuple(document),
    )
    _mismatch(context, "scale_factor", scale.scale_factor, document.get("scale_factor"))
    raw_environment = document.get("environment")
    if not isinstance(raw_environment, Mapping):
        raise LockMismatch(context, "environment", expected_metadata_environment(contract), raw_environment)
    expected_environment = expected_metadata_environment(contract)
    for field, value in expected_environment.items():
        _mismatch(context, field, value, raw_environment.get(field))
    _mismatch(context, "environment_fields", tuple(expected_environment), tuple(raw_environment))

    raw_outputs = document.get("outputs")
    if not isinstance(raw_outputs, Mapping):
        raise LockMismatch(context, "outputs", expected_tables, raw_outputs)
    require_exact_names(expected_tables, tuple(raw_outputs), context)

    existing_names = {path.name for path in output_root.iterdir() if path != metadata_path}
    actual_names = tuple(name for name in expected_names if name in existing_names) + tuple(
        sorted(existing_names - set(expected_names))
    )
    require_exact_names(expected_names, actual_names, context)

    verified: list[VerifiedFile] = []
    for output in scale.outputs:
        output_context = VerificationContext(
            plan.dataset.name,
            plan.scale,
            "generator output",
            object_name=output.object_name,
        )
        raw_output = raw_outputs[output.table]
        expected_metadata = {
            "object_name": output.object_name,
            "size_bytes": output.size_bytes,
            "sha256": output.sha256,
        }
        _mismatch(output_context, "metadata", expected_metadata, raw_output)
        try:
            schema = plan.dataset.schemas[output.schema_id]
        except KeyError:
            raise LockMismatch(
                output_context,
                "schema_id",
                tuple(plan.dataset.schemas),
                output.schema_id,
            ) from None
        item = verify_file(output_root / output.object_name, _expected_object(output), output_context)
        verify_physical_schema(item, schema, output_context)
        verified.append(item)
    return tuple(verified)


def publish_verified_files(
    files: Sequence[VerifiedFile],
    output_root: Path,
    destination: Path,
) -> tuple[VerifiedFile, ...]:
    del output_root
    targets = tuple(destination / item.expected.object_name for item in files)
    for target in targets:
        if target.exists() or target.is_symlink():
            raise FileExistsError(target)
    published: list[Path] = []
    try:
        for item, target in zip(files, targets, strict=True):
            os.link(item.path, target)
            published.append(target)
        return tuple(
            VerifiedFile(path.resolve(strict=True), item.expected) for item, path in zip(files, targets, strict=True)
        )
    except BaseException:
        for path in reversed(published):
            path.unlink(missing_ok=True)
        raise


def generate_tpch(
    plan: ScalePlan,
    dest: Path,
    runner: ContainerRunner | None = None,
) -> tuple[VerifiedFile, ...]:
    """Generate, verify, and atomically publish one locked TPC-H scale."""
    contract = require_generator_contract(plan.dataset.generator)
    scale = require_generator_scale(plan.generator_scale)
    active_runner = runner or DockerContainerRunner()
    image_context = VerificationContext(plan.dataset.name, plan.scale, "image")
    evidence = active_runner.ensure_image(contract)
    verify_image_evidence(evidence, contract, image_context)
    destination = Path(dest)
    published: tuple[VerifiedFile, ...] = ()
    try:
        with owned_directory(destination) as output_root:
            metadata_path = output_root / "metadata.json"
            active_runner.run(contract, scale, output_root, metadata_path)
            verified = verify_tpch_outputs(plan, output_root, metadata_path)
            published = publish_verified_files(verified, output_root, destination)
        for item in published:
            output = next(output for output in scale.outputs if output.object_name == item.expected.object_name)
            context = VerificationContext(
                plan.dataset.name,
                plan.scale,
                "published output",
                object_name=output.object_name,
            )
            verify_file(item.path, item.expected, context)
    except BaseException:
        for item in published:
            item.path.unlink(missing_ok=True)
        raise
    return published
