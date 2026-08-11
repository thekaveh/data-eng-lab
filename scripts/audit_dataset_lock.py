#!/usr/bin/env python3
"""Download an HTTP artifact into temporary storage and emit lock metadata."""
from __future__ import annotations

import argparse
import shutil
import stat
import sys
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from datasets import schema as dataset_schema  # noqa: E402
from datasets.locking import file_metadata, validate_relative_path  # noqa: E402

REGISTRY_PATH = ROOT / "datasets" / "registry.yaml"
DOWNLOAD_TIMEOUT_SECONDS = 120


def _validate_url(url: str) -> None:
    errors = dataset_schema._https(url, "url")
    if errors:
        raise ValueError(errors[0])


def _artifact_name(url: str) -> str:
    name = unquote(urlsplit(url).path.rsplit("/", 1)[-1])
    if (
        validate_relative_path(name, "url path")
        or PurePosixPath(name).name != name
    ):
        raise ValueError("url path must end with a safe artifact name")
    return name


def _metadata(path: Path) -> tuple[int, str]:
    size, sha256 = file_metadata(path)
    if size == 0:
        raise ValueError("artifact must not be empty")
    return size, sha256


def _member_is_symlink(member: zipfile.ZipInfo) -> bool:
    return member.create_system == 3 and stat.S_ISLNK(member.external_attr >> 16)


def _member_is_regular_file(member: zipfile.ZipInfo) -> bool:
    if member.is_dir():
        return False
    mode = member.external_attr >> 16
    file_type = stat.S_IFMT(mode)
    return file_type == 0 or stat.S_ISREG(mode)


def _archive_outputs(raw_path: Path, temporary_root: Path) -> list[dict[str, object]]:
    outputs: list[dict[str, object]] = []
    object_names: set[str] = set()
    with zipfile.ZipFile(raw_path) as archive_file:
        members = archive_file.infolist()
        for member in members:
            if _member_is_symlink(member):
                raise ValueError(f"archive member {member.filename!r} must not be a symlink")
            if not _member_is_regular_file(member):
                raise ValueError(f"archive member {member.filename!r} must be a regular file")
            errors = validate_relative_path(member.filename, "archive member")
            if errors:
                raise ValueError(errors[0])

            object_name = PurePosixPath(member.filename).name
            if object_name in object_names:
                raise ValueError(f"archive members flatten to duplicate object name {object_name}")
            object_names.add(object_name)

        extracted_root = temporary_root / "members"
        extracted_root.mkdir()
        for index, member in enumerate(members):
            extracted_path = extracted_root / str(index)
            with archive_file.open(member) as source, extracted_path.open("wb") as target:
                shutil.copyfileobj(source, target)
            size, sha256 = _metadata(extracted_path)
            outputs.append(
                {
                    "object_name": PurePosixPath(member.filename).name,
                    "member_path": member.filename,
                    "size_bytes": size,
                    "sha256": sha256,
                }
            )

    if not outputs:
        raise ValueError("archive must contain at least one regular file")
    return outputs


def audit_http(url: str, *, archive: bool) -> dict[str, object]:
    """Return metadata for a temporary HTTP download and optional ZIP members."""
    _validate_url(url)
    raw_name = _artifact_name(url)

    with tempfile.TemporaryDirectory() as temporary_directory:
        temporary_root = Path(temporary_directory)
        raw_path = temporary_root / "download"
        with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
            response.raise_for_status()
            _validate_url(response.url)
            with raw_path.open("wb") as target:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    if chunk:
                        target.write(chunk)

            evidence = {}
            if etag := response.headers.get("ETag"):
                evidence["etag"] = etag
            if last_modified := response.headers.get("Last-Modified"):
                evidence["last_modified"] = last_modified

        raw_size, raw_sha256 = _metadata(raw_path)
        raw = {"name": raw_name, "size_bytes": raw_size, "sha256": raw_sha256}
        if archive:
            outputs = _archive_outputs(raw_path, temporary_root)
        else:
            outputs = [
                {
                    "object_name": raw_name,
                    "size_bytes": raw_size,
                    "sha256": raw_sha256,
                    "raw_identity": True,
                }
            ]
        return {"url": url, "evidence": evidence, "raw": raw, "outputs": outputs}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    http = commands.add_parser("http", help="audit an authoritative HTTPS artifact")
    http.add_argument("--url", required=True)
    http.add_argument("--archive", action="store_true", help="treat the downloaded artifact as ZIP")
    http.add_argument("--output", required=True, type=Path, help="metadata YAML output path")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as error:
        return int(error.code)

    output = args.output.resolve()
    registry = REGISTRY_PATH.resolve()
    if output == registry or (output.exists() and output.samefile(registry)):
        parser.print_usage(sys.stderr)
        print(f"{parser.prog}: error: refusing to write dataset registry", file=sys.stderr)
        return 2

    try:
        result = audit_http(args.url, archive=args.archive)
    except ValueError as error:
        parser.print_usage(sys.stderr)
        print(f"{parser.prog}: error: {error}", file=sys.stderr)
        return 2
    output.write_text(yaml.safe_dump(result, sort_keys=False), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
