"""Load and resolve datasets/registry.yaml."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import TypeVar, cast

import yaml

from datasets.schema import validate_registry_v2

_Value = TypeVar("_Value")


def _empty_mapping() -> Mapping[str, _Value]:
    return MappingProxyType({})


def _freeze(value: object) -> object:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _as_mapping(value: object) -> Mapping[str, object]:
    return cast(Mapping[str, object], value)


@dataclass(frozen=True)
class Provenance:
    publisher: str
    homepage: str
    license_name: str
    license_url: str
    attribution: str
    source_stability: str
    update_policy: str


@dataclass(frozen=True)
class SchemaField:
    name: str
    logical_type: str
    nullable: bool


@dataclass(frozen=True)
class SchemaContract:
    id: str
    format: str
    mode: str
    fields: tuple[SchemaField, ...]
    options: Mapping[str, object]
    fingerprint: str


@dataclass(frozen=True)
class SourceVersion:
    kind: str
    value: str


@dataclass(frozen=True)
class RawArtifact:
    name: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class LandingObject:
    object_name: str
    size_bytes: int
    sha256: str
    schema_id: str
    member_path: str | None
    raw_identity: bool


@dataclass(frozen=True)
class HttpArtifact:
    id: str
    url: str
    version: SourceVersion
    stability: str
    evidence: Mapping[str, str]
    raw: RawArtifact
    outputs: tuple[LandingObject, ...]
    provenance: Provenance | None = None


@dataclass(frozen=True)
class GeneratorEnvironment:
    image: str
    image_digest: str
    platform: str
    uv_lock_sha256: str
    locale: str
    timezone: str
    threads: int
    preserve_insertion_order: bool


@dataclass(frozen=True)
class GeneratorOutput:
    table: str
    object_name: str
    size_bytes: int
    sha256: str
    schema_id: str


@dataclass(frozen=True)
class GeneratorScale:
    name: str
    scale_factor: float
    outputs: tuple[GeneratorOutput, ...]


@dataclass(frozen=True)
class GeneratorContract:
    engine_name: str
    engine_version: str
    engine_wheel_sha256: str
    extension_name: str
    extension_version_relation: str
    extension_repository_url: str
    extension_sha256: str
    environment: GeneratorEnvironment
    procedure: str
    scale_parameter: str
    export_format: str
    compression: str
    row_group_size: int
    order_by: Mapping[str, tuple[str, ...]]
    scales: Mapping[str, GeneratorScale]


@dataclass(frozen=True)
class Dataset:
    name: str
    description: str
    format: str
    license: str
    landing_prefix: str
    kind: str
    unzip: bool
    scales: Mapping[str, tuple[str, ...] | GeneratorScale]
    provenance: Provenance | None = None
    schemas: Mapping[str, SchemaContract] = field(default_factory=_empty_mapping)
    artifacts: Mapping[str, HttpArtifact] = field(default_factory=_empty_mapping)
    generator: GeneratorContract | None = None


@dataclass(frozen=True)
class ScalePlan:
    dataset: Dataset
    scale: str
    urls: tuple[str, ...]
    sf: float | None
    artifacts: tuple[HttpArtifact, ...] = ()
    generator_scale: GeneratorScale | None = None


def load_registry(path: Path) -> dict[str, Dataset]:
    return load_registry_v2(path)


def _parse_provenance(raw: Mapping[str, object]) -> Provenance:
    return Provenance(
        publisher=cast(str, raw["publisher"]),
        homepage=cast(str, raw["homepage"]),
        license_name=cast(str, raw["license_name"]),
        license_url=cast(str, raw["license_url"]),
        attribution=cast(str, raw["attribution"]),
        source_stability=cast(str, raw["source_stability"]),
        update_policy=cast(str, raw["update_policy"]),
    )


def _parse_schemas(raw: Mapping[str, object]) -> Mapping[str, SchemaContract]:
    schemas: dict[str, SchemaContract] = {}
    for schema_id, value in raw.items():
        schema = _as_mapping(value)
        raw_fields = cast(list[object], schema["fields"])
        fields = tuple(
            SchemaField(
                name=cast(str, field_spec["name"]),
                logical_type=cast(str, field_spec["logical_type"]),
                nullable=cast(bool, field_spec["nullable"]),
            )
            for field_spec in (_as_mapping(item) for item in raw_fields)
        )
        schemas[schema_id] = SchemaContract(
            id=schema_id,
            format=cast(str, schema["format"]),
            mode=cast(str, schema["mode"]),
            fields=fields,
            options=cast(Mapping[str, object], _freeze(schema["options"])),
            fingerprint=cast(str, schema["fingerprint"]),
        )
    return MappingProxyType(schemas)


def _parse_artifacts(raw: Mapping[str, object]) -> Mapping[str, HttpArtifact]:
    artifacts: dict[str, HttpArtifact] = {}
    for artifact_id, value in raw.items():
        artifact = _as_mapping(value)
        version = _as_mapping(artifact["version"])
        raw_artifact = _as_mapping(artifact["raw"])
        raw_outputs = cast(list[object], artifact["outputs"])
        outputs = tuple(
            LandingObject(
                object_name=cast(str, output["object_name"]),
                size_bytes=cast(int, output["size_bytes"]),
                sha256=cast(str, output["sha256"]),
                schema_id=cast(str, output["schema"]),
                member_path=cast(str | None, output.get("member_path")),
                raw_identity=cast(bool, output.get("raw_identity", False)),
            )
            for output in (_as_mapping(item) for item in raw_outputs)
        )
        artifacts[artifact_id] = HttpArtifact(
            id=artifact_id,
            url=cast(str, artifact["url"]),
            version=SourceVersion(
                kind=cast(str, version["kind"]),
                value=cast(str, version["value"]),
            ),
            stability=cast(str, artifact["stability"]),
            evidence=cast(Mapping[str, str], _freeze(artifact["evidence"])),
            raw=RawArtifact(
                name=cast(str, raw_artifact["name"]),
                size_bytes=cast(int, raw_artifact["size_bytes"]),
                sha256=cast(str, raw_artifact["sha256"]),
            ),
            outputs=outputs,
            provenance=(_parse_provenance(_as_mapping(artifact["provenance"])) if "provenance" in artifact else None),
        )
    return MappingProxyType(artifacts)


def _parse_generator(raw: Mapping[str, object]) -> GeneratorContract:
    engine = _as_mapping(raw["engine"])
    extension = _as_mapping(raw["extension"])
    environment = _as_mapping(raw["environment"])
    command = _as_mapping(raw["command"])
    export = _as_mapping(raw["export"])
    raw_scales = _as_mapping(raw["scales"])
    scales: dict[str, GeneratorScale] = {}
    for scale_name, value in raw_scales.items():
        scale = _as_mapping(value)
        raw_outputs = cast(list[object], scale["outputs"])
        scales[scale_name] = GeneratorScale(
            name=scale_name,
            scale_factor=float(cast(float | int, scale["scale_factor"])),
            outputs=tuple(
                GeneratorOutput(
                    table=cast(str, output["table"]),
                    object_name=cast(str, output["object_name"]),
                    size_bytes=cast(int, output["size_bytes"]),
                    sha256=cast(str, output["sha256"]),
                    schema_id=cast(str, output["schema"]),
                )
                for output in (_as_mapping(item) for item in raw_outputs)
            ),
        )
    frozen_scales = MappingProxyType(scales)
    raw_order_by = _as_mapping(export["order_by"])
    order_by = MappingProxyType({table: tuple(cast(list[str], columns)) for table, columns in raw_order_by.items()})
    return GeneratorContract(
        engine_name=cast(str, engine["name"]),
        engine_version=cast(str, engine["version"]),
        engine_wheel_sha256=cast(str, engine["wheel_sha256"]),
        extension_name=cast(str, extension["name"]),
        extension_version_relation=cast(str, extension["version_relation"]),
        extension_repository_url=cast(str, extension["repository_url"]),
        extension_sha256=cast(str, extension["sha256"]),
        environment=GeneratorEnvironment(
            image=cast(str, environment["image"]),
            image_digest=cast(str, environment["image_digest"]),
            platform=cast(str, environment["platform"]),
            uv_lock_sha256=cast(str, environment["uv_lock_sha256"]),
            locale=cast(str, environment["locale"]),
            timezone=cast(str, environment["timezone"]),
            threads=cast(int, environment["threads"]),
            preserve_insertion_order=cast(bool, environment["preserve_insertion_order"]),
        ),
        procedure=cast(str, command["procedure"]),
        scale_parameter=cast(str, command["scale_parameter"]),
        export_format=cast(str, export["format"]),
        compression=cast(str, export["compression"]),
        row_group_size=cast(int, export["row_group_size"]),
        order_by=order_by,
        scales=frozen_scales,
    )


def _parse_v2_datasets(raw: Mapping[str, object]) -> dict[str, Dataset]:
    datasets: dict[str, Dataset] = {}
    for name, value in raw.items():
        dataset = _as_mapping(value)
        fetch = _as_mapping(dataset["fetch"])
        kind = cast(str, fetch["kind"])
        schemas = _parse_schemas(_as_mapping(dataset["schemas"]))
        provenance = _parse_provenance(_as_mapping(dataset["provenance"]))
        if kind == "http":
            artifacts = _parse_artifacts(_as_mapping(dataset["artifacts"]))
            raw_scales = _as_mapping(dataset["scales"])
            scales: Mapping[str, tuple[str, ...] | GeneratorScale] = MappingProxyType(
                {
                    scale_name: tuple(cast(list[str], _as_mapping(scale)["artifacts"]))
                    for scale_name, scale in raw_scales.items()
                }
            )
            generator = None
        else:
            artifacts = cast(Mapping[str, HttpArtifact], _empty_mapping())
            generator = _parse_generator(_as_mapping(dataset["generator"]))
            scales = generator.scales
        datasets[name] = Dataset(
            name=name,
            description=cast(str, dataset["description"]),
            format=cast(str, dataset["format"]),
            license=cast(str, dataset["license"]),
            landing_prefix=cast(str, dataset["landing_prefix"]),
            kind=kind,
            unzip=cast(bool, fetch.get("unzip", False)),
            scales=scales,
            provenance=provenance,
            schemas=schemas,
            artifacts=artifacts,
            generator=generator,
        )
    return datasets


def load_registry_v2(path: Path) -> dict[str, Dataset]:
    doc = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    errors = validate_registry_v2(doc)
    if errors:
        raise ValueError("invalid registry:\n  - " + "\n  - ".join(errors))
    registry = _as_mapping(doc)
    return _parse_v2_datasets(_as_mapping(registry["datasets"]))


def resolve_scale(ds: Dataset, scale: str) -> ScalePlan:
    if scale not in ds.scales:
        raise KeyError(f"dataset '{ds.name}' has no scale '{scale}' (have: {sorted(ds.scales)})")
    spec = ds.scales[scale]
    if isinstance(spec, GeneratorScale):
        return ScalePlan(
            dataset=ds,
            scale=scale,
            urls=(),
            sf=spec.scale_factor,
            generator_scale=spec,
        )
    artifacts = tuple(ds.artifacts[artifact_id] for artifact_id in spec)
    return ScalePlan(
        dataset=ds,
        scale=scale,
        urls=tuple(artifact.url for artifact in artifacts),
        sf=None,
        artifacts=artifacts,
    )
