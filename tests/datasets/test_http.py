from __future__ import annotations

import hashlib
import io
import zipfile
from dataclasses import replace
from pathlib import Path
from typing import Callable

import pytest
import responses

from datasets import registry as reg
from datasets.acquisition import DownloadedFile, ResponseEvidence
from datasets.locking import schema_fingerprint
from datasets.sources import http
from datasets.verification import LockMismatch, VerifiedFile


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _text_schema() -> reg.SchemaContract:
    raw = {
        "format": "text",
        "mode": "exact",
        "fields": [],
        "options": {"encoding": "utf-8"},
    }
    return reg.SchemaContract(
        id="text-v1",
        format="text",
        mode="exact",
        fields=(),
        options=raw["options"],
        fingerprint=schema_fingerprint(raw),
    )


TEXT_SCHEMA = _text_schema()


def _output(
    object_name: str,
    payload: bytes,
    *,
    member_path: str | None = None,
    raw_identity: bool = False,
) -> reg.LandingObject:
    return reg.LandingObject(
        object_name=object_name,
        size_bytes=len(payload),
        sha256=_sha256(payload),
        schema_id=TEXT_SCHEMA.id,
        member_path=member_path,
        raw_identity=raw_identity,
    )


def _artifact(
    artifact_id: str,
    url: str,
    raw_name: str,
    raw_payload: bytes,
    outputs: tuple[reg.LandingObject, ...],
) -> reg.HttpArtifact:
    return reg.HttpArtifact(
        id=artifact_id,
        url=url,
        version=reg.SourceVersion(kind="revision", value=f"{artifact_id}-v1"),
        stability="immutable",
        evidence={},
        raw=reg.RawArtifact(raw_name, len(raw_payload), _sha256(raw_payload)),
        outputs=outputs,
    )


def _plan(*artifacts: reg.HttpArtifact, unzip: bool) -> reg.ScalePlan:
    dataset = reg.Dataset(
        name="locked",
        description="locked fixture",
        format="text",
        license="fixture",
        landing_prefix="locked",
        kind="http",
        unzip=unzip,
        scales={"tiny": tuple(artifact.id for artifact in artifacts)},
        schemas={TEXT_SCHEMA.id: TEXT_SCHEMA},
        artifacts={artifact.id: artifact for artifact in artifacts},
    )
    return reg.resolve_scale(dataset, "tiny")


def _direct_plan(
    payload: bytes = b"direct\n",
    *,
    url: str = "https://example.test/url-derived.txt",
    raw_name: str = "registry-owned.txt",
) -> reg.ScalePlan:
    output = _output(raw_name, payload, raw_identity=True)
    return _plan(_artifact("direct", url, raw_name, payload, (output,)), unzip=False)


def _zip_bytes(members: list[tuple[str, bytes]]) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        for name, payload in members:
            archive.writestr(name, payload)
    return stream.getvalue()


def _archive_plan(
    members: list[tuple[str, bytes]],
    *,
    outputs: tuple[reg.LandingObject, ...] | None = None,
    artifact_id: str = "archive",
    url: str | None = None,
) -> tuple[reg.HttpArtifact, bytes]:
    archive = _zip_bytes(members)
    locked_outputs = outputs or tuple(
        _output(Path(member_path).name, payload, member_path=member_path)
        for member_path, payload in members
    )
    artifact = _artifact(
        artifact_id,
        url or f"https://example.test/{artifact_id}.zip",
        f"{artifact_id}.zip",
        archive,
        locked_outputs,
    )
    return artifact, archive


@pytest.fixture
def serve(monkeypatch: pytest.MonkeyPatch) -> Callable[[reg.ScalePlan, tuple[bytes, ...]], None]:
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mocked:

        def register(plan: reg.ScalePlan, payloads: tuple[bytes, ...]) -> None:
            bodies = dict(zip((artifact.url for artifact in plan.artifacts), payloads, strict=True))
            for url, payload in bodies.items():
                mocked.get(url, body=payload)

            def fake_download(url: str, destination: Path, max_bytes: int) -> DownloadedFile:
                payload = bodies[url]
                destination.write_bytes(payload)
                return DownloadedFile(destination, ResponseEvidence())

            monkeypatch.setattr(http, "download_bounded", fake_download, raising=False)

        yield register


def _assert_empty(path: Path) -> None:
    assert list(path.iterdir()) == []


def test_fetch_direct_uses_registry_raw_and_object_identity(
    tmp_path: Path,
    serve: Callable[[reg.ScalePlan, tuple[bytes, ...]], None],
):
    payload = b"registry identity\n"
    plan = _direct_plan(payload)
    serve(plan, (payload,))

    result = http.fetch_http(plan, tmp_path)

    assert isinstance(result, tuple)
    assert all(isinstance(item, VerifiedFile) for item in result)
    assert tuple(item.expected.object_name for item in result) == ("registry-owned.txt",)
    assert tuple(item.path.name for item in result) == ("registry-owned.txt",)
    assert (tmp_path / "registry-owned.txt").read_bytes() == payload
    assert not (tmp_path / "url-derived.txt").exists()


@pytest.mark.parametrize(
    ("raw_change", "field"),
    [
        ({"size_bytes": len(b"expected\n") + 1}, "size_bytes"),
        ({"sha256": _sha256(b"different\n")}, "sha256"),
    ],
)
def test_fetch_rejects_direct_raw_size_and_digest(
    tmp_path: Path,
    serve: Callable[[reg.ScalePlan, tuple[bytes, ...]], None],
    raw_change: dict[str, object],
    field: str,
):
    payload = b"expected\n"
    plan = _direct_plan(payload)
    artifact = replace(plan.artifacts[0], raw=replace(plan.artifacts[0].raw, **raw_change))
    plan = replace(plan, artifacts=(artifact,))
    serve(plan, (payload,))

    with pytest.raises(LockMismatch, match=field):
        http.fetch_http(plan, tmp_path)

    _assert_empty(tmp_path)


def test_fetch_rejects_raw_digest_before_archive_open(
    tmp_path: Path,
    serve: Callable[[reg.ScalePlan, tuple[bytes, ...]], None],
    monkeypatch: pytest.MonkeyPatch,
):
    artifact, archive = _archive_plan([("nested/data.txt", b"good\n")])
    artifact = replace(artifact, raw=replace(artifact.raw, sha256=_sha256(b"not the archive")))
    plan = _plan(artifact, unzip=True)
    serve(plan, (archive,))
    archive_opened = False

    def forbidden_archive_open(*args: object, **kwargs: object) -> object:
        nonlocal archive_opened
        archive_opened = True
        raise AssertionError("archive opened before raw verification")

    monkeypatch.setattr(http, "validated_zip_members", forbidden_archive_open, raising=False)

    with pytest.raises(LockMismatch, match="sha256"):
        http.fetch_http(plan, tmp_path)

    assert archive_opened is False
    _assert_empty(tmp_path)


@pytest.mark.parametrize(
    ("members", "expected_members", "field"),
    [
        ([('a.txt', b'a\n')], [('a.txt', b'a\n'), ('b.txt', b'b\n')], "member_paths"),
        ([('a.txt', b'a\n'), ('extra.txt', b'x\n')], [('a.txt', b'a\n')], "member_paths"),
        ([('actual/a.txt', b'a\n')], [('locked/a.txt', b'a\n')], "member_paths"),
    ],
)
def test_fetch_requires_exact_archive_member_paths(
    tmp_path: Path,
    serve: Callable[[reg.ScalePlan, tuple[bytes, ...]], None],
    members: list[tuple[str, bytes]],
    expected_members: list[tuple[str, bytes]],
    field: str,
):
    outputs = tuple(
        _output(Path(member).name, payload, member_path=member) for member, payload in expected_members
    )
    artifact, archive = _archive_plan(members, outputs=outputs)
    plan = _plan(artifact, unzip=True)
    serve(plan, (archive,))

    with pytest.raises(LockMismatch, match=field):
        http.fetch_http(plan, tmp_path)

    _assert_empty(tmp_path)


def test_fetch_rejects_duplicate_archive_members(
    tmp_path: Path,
    serve: Callable[[reg.ScalePlan, tuple[bytes, ...]], None],
):
    members = [("data.txt", b"first\n"), ("data.txt", b"second\n")]
    outputs = (_output("data.txt", b"first\n", member_path="data.txt"),)
    with pytest.warns(UserWarning, match="Duplicate name"):
        artifact, archive = _archive_plan(members, outputs=outputs)
    plan = _plan(artifact, unzip=True)
    serve(plan, (archive,))

    with pytest.raises(ValueError, match="duplicate"):
        http.fetch_http(plan, tmp_path)

    _assert_empty(tmp_path)


def test_fetch_rejects_wrong_flattened_object_name(
    tmp_path: Path,
    serve: Callable[[reg.ScalePlan, tuple[bytes, ...]], None],
):
    payload = b"data\n"
    outputs = (_output("renamed.txt", payload, member_path="nested/data.txt"),)
    artifact, archive = _archive_plan([("nested/data.txt", payload)], outputs=outputs)
    plan = _plan(artifact, unzip=True)
    serve(plan, (archive,))

    with pytest.raises(LockMismatch, match="object_names"):
        http.fetch_http(plan, tmp_path)

    _assert_empty(tmp_path)


@pytest.mark.parametrize(
    ("output_change", "field"),
    [
        ({"size_bytes": len(b"locked\n") + 1}, "size_bytes"),
        ({"sha256": _sha256(b"wrong\n")}, "sha256"),
    ],
)
def test_fetch_rejects_locked_output_size_and_digest(
    tmp_path: Path,
    serve: Callable[[reg.ScalePlan, tuple[bytes, ...]], None],
    output_change: dict[str, object],
    field: str,
):
    payload = b"locked\n"
    output = replace(_output("data.txt", payload, member_path="nested/data.txt"), **output_change)
    artifact, archive = _archive_plan([("nested/data.txt", payload)], outputs=(output,))
    plan = _plan(artifact, unzip=True)
    serve(plan, (archive,))

    with pytest.raises(LockMismatch, match=field):
        http.fetch_http(plan, tmp_path)

    _assert_empty(tmp_path)


def test_fetch_rejects_locked_output_schema(
    tmp_path: Path,
    serve: Callable[[reg.ScalePlan, tuple[bytes, ...]], None],
):
    payload = b"invalid utf-8: \xff"
    artifact, archive = _archive_plan([("nested/data.txt", payload)])
    plan = _plan(artifact, unzip=True)
    serve(plan, (archive,))

    with pytest.raises(LockMismatch, match="UTF-8"):
        http.fetch_http(plan, tmp_path)

    _assert_empty(tmp_path)


def test_fetch_returns_registry_order_even_when_archive_order_differs(
    tmp_path: Path,
    serve: Callable[[reg.ScalePlan, tuple[bytes, ...]], None],
):
    members = [("nested/b.txt", b"b\n"), ("nested/a.txt", b"a\n")]
    outputs = (
        _output("a.txt", b"a\n", member_path="nested/a.txt"),
        _output("b.txt", b"b\n", member_path="nested/b.txt"),
    )
    artifact, archive = _archive_plan(members, outputs=outputs)
    plan = _plan(artifact, unzip=True)
    serve(plan, (archive,))

    result = http.fetch_http(plan, tmp_path)

    assert tuple(item.expected.object_name for item in result) == ("a.txt", "b.txt")
    assert tuple(item.path.name for item in result) == ("a.txt", "b.txt")


def test_fetch_returns_multiple_artifacts_in_registry_order(
    tmp_path: Path,
    serve: Callable[[reg.ScalePlan, tuple[bytes, ...]], None],
):
    first_payload = b"first\n"
    archive_artifact, archive = _archive_plan(
        [("nested/b.txt", b"b\n"), ("nested/a.txt", b"a\n")],
        outputs=(
            _output("a.txt", b"a\n", member_path="nested/a.txt"),
            _output("b.txt", b"b\n", member_path="nested/b.txt"),
        ),
        artifact_id="bundle",
    )
    direct = _artifact(
        "direct",
        "https://example.test/first.txt",
        "first.txt",
        first_payload,
        (_output("first.txt", first_payload, raw_identity=True),),
    )
    plan = _plan(direct, archive_artifact, unzip=True)
    serve(plan, (first_payload, archive))

    result = http.fetch_http(plan, tmp_path)

    assert tuple(item.expected.object_name for item in result) == ("first.txt", "a.txt", "b.txt")


def test_fetch_does_not_publish_an_earlier_artifact_when_a_later_one_fails(
    tmp_path: Path,
    serve: Callable[[reg.ScalePlan, tuple[bytes, ...]], None],
):
    first_payload = b"first\n"
    second_payload = b"second\n"
    first = _artifact(
        "first",
        "https://example.test/first.txt",
        "first.txt",
        first_payload,
        (_output("first.txt", first_payload, raw_identity=True),),
    )
    second = _artifact(
        "second",
        "https://example.test/second.txt",
        "second.txt",
        second_payload,
        (_output("second.txt", b"wrong\n", raw_identity=True),),
    )
    plan = _plan(first, second, unzip=False)
    serve(plan, (first_payload, second_payload))

    with pytest.raises(LockMismatch):
        http.fetch_http(plan, tmp_path)

    _assert_empty(tmp_path)


def test_fetch_rolls_back_publication_and_keeps_caller_owned_files(
    tmp_path: Path,
    serve: Callable[[reg.ScalePlan, tuple[bytes, ...]], None],
):
    sentinel = tmp_path / "b.txt"
    sentinel.write_bytes(b"caller owned")
    artifact, archive = _archive_plan(
        [("a.txt", b"a\n"), ("b.txt", b"b\n")],
    )
    plan = _plan(artifact, unzip=True)
    serve(plan, (archive,))

    with pytest.raises(FileExistsError):
        http.fetch_http(plan, tmp_path)

    assert sentinel.read_bytes() == b"caller owned"
    assert not (tmp_path / "a.txt").exists()
    assert list(tmp_path.iterdir()) == [sentinel]
