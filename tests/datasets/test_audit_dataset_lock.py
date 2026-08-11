from __future__ import annotations

import hashlib
import stat
import tempfile
import zipfile
from pathlib import Path

import pytest
import responses
import yaml

from scripts import audit_dataset_lock as audit

ROOT = Path(__file__).resolve().parents[2]


def zip_bytes(members: dict[str, bytes]) -> bytes:
    with tempfile.SpooledTemporaryFile() as stream:
        with zipfile.ZipFile(stream, "w") as archive:
            for name, payload in members.items():
                archive.writestr(name, payload)
        stream.seek(0)
        return stream.read()


@responses.activate
def test_audit_http_emits_raw_direct_output_and_response_evidence():
    payload = b"id,name\n1,Ada\n"
    responses.add(
        responses.GET,
        "https://source.invalid/data.csv",
        body=payload,
        status=200,
        headers={"ETag": '"locked"', "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT"},
    )

    result = audit.audit_http("https://source.invalid/data.csv", archive=False)

    expected_sha256 = hashlib.sha256(payload).hexdigest()
    assert result == {
        "url": "https://source.invalid/data.csv",
        "evidence": {
            "etag": '"locked"',
            "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT",
        },
        "raw": {"name": "data.csv", "size_bytes": 14, "sha256": expected_sha256},
        "outputs": [
            {
                "object_name": "data.csv",
                "size_bytes": 14,
                "sha256": expected_sha256,
                "raw_identity": True,
            }
        ],
    }


@responses.activate
def test_audit_http_streams_with_120_second_timeout(monkeypatch: pytest.MonkeyPatch):
    responses.add(responses.GET, "https://source.invalid/data.csv", body=b"locked", status=200)
    request_get = audit.requests.get
    arguments: dict[str, object] = {}

    def recording_get(url: str, **kwargs: object):
        arguments.update(kwargs)
        return request_get(url, **kwargs)

    monkeypatch.setattr(audit.requests, "get", recording_get)

    audit.audit_http("https://source.invalid/data.csv", archive=False)

    assert arguments == {"stream": True, "timeout": 120}


@responses.activate
def test_audit_http_owns_and_cleans_temporary_directory(monkeypatch: pytest.MonkeyPatch):
    responses.add(responses.GET, "https://source.invalid/data.csv", body=b"locked", status=200)
    real_temporary_directory = tempfile.TemporaryDirectory
    created: list[Path] = []

    class RecordingTemporaryDirectory:
        def __init__(self) -> None:
            self._temporary_directory = real_temporary_directory()

        def __enter__(self) -> str:
            path = self._temporary_directory.__enter__()
            created.append(Path(path))
            return path

        def __exit__(self, *args: object) -> object:
            return self._temporary_directory.__exit__(*args)

    monkeypatch.setattr(audit.tempfile, "TemporaryDirectory", RecordingTemporaryDirectory)

    result = audit.audit_http("https://source.invalid/data.csv", archive=False)

    assert result["raw"]["size_bytes"] == 6
    assert len(created) == 1
    assert not created[0].exists()


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com/data.csv",
        "https://localhost/data.csv",
        "https://127.0.0.1/data.csv",
        "https://user:secret@example.com/data.csv",
        "https://example.com:443/data.csv",
        "https://example.com/data.csv#fragment",
    ],
)
def test_audit_http_reuses_authoritative_https_policy(url: str):
    with pytest.raises(ValueError, match="must be an authoritative HTTPS URL"):
        audit.audit_http(url, archive=False)


@responses.activate
def test_audit_http_rejects_empty_artifact():
    responses.add(responses.GET, "https://source.invalid/empty.csv", body=b"", status=200)

    with pytest.raises(ValueError, match="artifact must not be empty"):
        audit.audit_http("https://source.invalid/empty.csv", archive=False)


@responses.activate
def test_audit_zip_preserves_exact_member_paths_and_hashes_saved_bytes():
    members = {"a/data.csv": b"a", "b/other.csv": b"other"}
    payload = zip_bytes(members)
    responses.add(responses.GET, "https://source.invalid/data.zip", body=payload, status=200)

    result = audit.audit_http("https://source.invalid/data.zip", archive=True)

    assert result["raw"] == {
        "name": "data.zip",
        "size_bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    assert result["outputs"] == [
        {
            "object_name": "data.csv",
            "member_path": "a/data.csv",
            "size_bytes": 1,
            "sha256": hashlib.sha256(b"a").hexdigest(),
        },
        {
            "object_name": "other.csv",
            "member_path": "b/other.csv",
            "size_bytes": 5,
            "sha256": hashlib.sha256(b"other").hexdigest(),
        },
    ]


@responses.activate
def test_audit_zip_rejects_flattened_basename_collision():
    payload = zip_bytes({"a/data.csv": b"a", "b/data.csv": b"b"})
    responses.add(responses.GET, "https://source.invalid/data.zip", body=payload, status=200)

    with pytest.raises(ValueError, match="flatten to duplicate object name data.csv"):
        audit.audit_http("https://source.invalid/data.zip", archive=True)


@pytest.mark.parametrize("member", ["/absolute.csv", "../escape.csv", "a/../escape.csv", "a\\data.csv", "a/."])
@responses.activate
def test_audit_zip_rejects_unsafe_member_paths(member: str):
    responses.add(
        responses.GET,
        "https://source.invalid/data.zip",
        body=zip_bytes({member: b"unsafe"}),
        status=200,
    )

    with pytest.raises(ValueError, match="must be a safe relative POSIX path"):
        audit.audit_http("https://source.invalid/data.zip", archive=True)


@responses.activate
def test_audit_zip_rejects_symlinks():
    with tempfile.SpooledTemporaryFile() as stream:
        with zipfile.ZipFile(stream, "w") as archive:
            member = zipfile.ZipInfo("link.csv")
            member.create_system = 3
            member.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(member, "target.csv")
        stream.seek(0)
        responses.add(responses.GET, "https://source.invalid/data.zip", body=stream.read(), status=200)

    with pytest.raises(ValueError, match="symlink"):
        audit.audit_http("https://source.invalid/data.zip", archive=True)


@responses.activate
def test_audit_zip_rejects_non_file_members():
    responses.add(
        responses.GET,
        "https://source.invalid/data.zip",
        body=zip_bytes({"directory/": b""}),
        status=200,
    )

    with pytest.raises(ValueError, match="regular file"):
        audit.audit_http("https://source.invalid/data.zip", archive=True)


def test_cli_requires_explicit_output_and_never_changes_registry(tmp_path: Path):
    registry = ROOT / "datasets" / "registry.yaml"
    before = registry.read_bytes()

    assert audit.main(["http", "--url", "https://source.invalid/data.csv"]) == 2

    assert registry.read_bytes() == before
    assert list(tmp_path.iterdir()) == []


def test_cli_rejects_registry_as_output_without_fetching():
    registry = ROOT / "datasets" / "registry.yaml"
    before = registry.read_bytes()

    assert audit.main(
        ["http", "--url", "https://source.invalid/data.csv", "--output", str(registry)]
    ) == 2

    assert registry.read_bytes() == before


@responses.activate
def test_cli_rejects_hard_link_to_registry_without_fetching(tmp_path: Path):
    registry = ROOT / "datasets" / "registry.yaml"
    before = registry.read_bytes()
    output = tmp_path / "registry-alias.yaml"
    output.hardlink_to(registry)

    assert audit.main(
        ["http", "--url", "https://source.invalid/data.csv", "--output", str(output)]
    ) == 2

    assert registry.read_bytes() == before


@responses.activate
def test_cli_keeps_only_requested_deterministic_metadata_output(tmp_path: Path):
    url = "https://source.invalid/data.csv"
    responses.add(responses.GET, url, body=b"locked", status=200)
    responses.add(responses.GET, url, body=b"locked", status=200)
    first = tmp_path / "candidate.yaml"
    second = tmp_path / "candidate-again.yaml"

    assert audit.main(["http", "--url", url, "--output", str(first)]) == 0
    assert audit.main(["http", "--url", url, "--output", str(second)]) == 0

    assert first.read_bytes() == second.read_bytes()
    assert yaml.safe_load(first.read_text()) == {
        "url": url,
        "evidence": {},
        "raw": {
            "name": "data.csv",
            "size_bytes": 6,
            "sha256": hashlib.sha256(b"locked").hexdigest(),
        },
        "outputs": [
            {
                "object_name": "data.csv",
                "size_bytes": 6,
                "sha256": hashlib.sha256(b"locked").hexdigest(),
                "raw_identity": True,
            }
        ],
    }
    assert sorted(path.name for path in tmp_path.iterdir()) == ["candidate-again.yaml", "candidate.yaml"]
