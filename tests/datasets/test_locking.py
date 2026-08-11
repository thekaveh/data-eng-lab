from pathlib import Path

from datasets.locking import (
    canonical_json,
    file_metadata,
    schema_fingerprint,
    validate_relative_path,
    validate_sha256,
    validate_size,
)


def test_canonical_json_sorts_mapping_keys_and_preserves_field_order():
    left = {"mode": "exact", "fields": [{"name": "b"}, {"name": "a"}], "format": "csv"}
    right = {"format": "csv", "fields": [{"name": "b"}, {"name": "a"}], "mode": "exact"}
    assert canonical_json(left) == canonical_json(right)
    assert canonical_json(left).decode() == (
        '{"fields":[{"name":"b"},{"name":"a"}],"format":"csv","mode":"exact"}'
    )


def test_schema_fingerprint_ignores_existing_fingerprint():
    schema = {"format": "csv", "mode": "exact", "fields": [], "fingerprint": "0" * 64}
    assert schema_fingerprint(schema) == schema_fingerprint(
        {"format": "csv", "mode": "exact", "fields": []}
    )


def test_file_metadata_returns_positive_size_and_sha256(tmp_path: Path):
    artifact = tmp_path / "artifact.bin"
    artifact.write_bytes(b"locked-bytes")
    assert file_metadata(artifact) == (
        12,
        "60170e42b944363f7cc231ceb230fea6f13f7691b66976c933f343042f9b39ff",
    )


def test_lock_scalar_validators_reject_malformed_values():
    assert validate_sha256("A" * 64, "x.sha256") == [
        "x.sha256: must be 64 lowercase hexadecimal characters"
    ]
    assert validate_size(True, "x.size_bytes") == [
        "x.size_bytes: must be a positive integer"
    ]
    assert validate_size(0, "x.size_bytes") == [
        "x.size_bytes: must be a positive integer"
    ]
    assert validate_relative_path("../escape.csv", "x.object_name") == [
        "x.object_name: must be a safe relative POSIX path"
    ]
    assert validate_relative_path("/absolute.csv", "x.object_name") == [
        "x.object_name: must be a safe relative POSIX path"
    ]
