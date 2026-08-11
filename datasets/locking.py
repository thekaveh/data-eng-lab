"""Pure canonicalization and scalar validation for dataset provenance locks."""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def canonical_json(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def schema_fingerprint(schema: Mapping[str, object]) -> str:
    payload = {key: value for key, value in schema.items() if key != "fingerprint"}
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def file_metadata(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def validate_sha256(value: object, path: str) -> list[str]:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        return [f"{path}: must be 64 lowercase hexadecimal characters"]
    return []


def validate_size(value: object, path: str) -> list[str]:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return [f"{path}: must be a positive integer"]
    return []


def validate_relative_path(value: object, path: str) -> list[str]:
    if not isinstance(value, str) or not value:
        return [f"{path}: must be a safe relative POSIX path"]
    candidate = PurePosixPath(value)
    if (
        candidate == PurePosixPath(".")
        or candidate.is_absolute()
        or ".." in candidate.parts
        or "\\" in value
        or value.endswith("/")
        or value.endswith("/.")
    ):
        return [f"{path}: must be a safe relative POSIX path"]
    return []
