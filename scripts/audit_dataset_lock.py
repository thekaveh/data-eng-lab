#!/usr/bin/env python3
"""Download an HTTP artifact into temporary storage and emit lock metadata."""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from datasets.acquisition import (  # noqa: E402
    DownloadedFile,
    ZipLimits,
    bound_download_metadata,
    bound_extracted_metadata,
    download_bounded,
    extract_members,
    validated_zip_members,
)
from datasets.locking import validate_relative_path  # noqa: E402

REGISTRY_PATH = ROOT / "datasets" / "registry.yaml"
DOWNLOAD_TIMEOUT_SECONDS = 120
MAX_DOWNLOAD_BYTES = 2 * 1024**3
MAX_ARCHIVE_MEMBERS = 10_000
MAX_MEMBER_BYTES = 2 * 1024**3
MAX_TOTAL_UNCOMPRESSED_BYTES = 4 * 1024**3
MAX_COMPRESSION_RATIO = 200
MAX_CENTRAL_DIRECTORY_BYTES = 128 * 1024**2
DOWNLOAD_TRANSPORT = None


def _artifact_name(url: str) -> str:
    name = unquote(urlsplit(url).path.rsplit("/", 1)[-1])
    if validate_relative_path(name, "url path") or PurePosixPath(name).name != name:
        raise ValueError("url path must end with a safe artifact name")
    return name


def _zip_limits() -> ZipLimits:
    return ZipLimits(
        max_entries=MAX_ARCHIVE_MEMBERS,
        max_central_directory_bytes=MAX_CENTRAL_DIRECTORY_BYTES,
        max_total_expanded_bytes=MAX_TOTAL_UNCOMPRESSED_BYTES,
        max_compression_ratio=MAX_COMPRESSION_RATIO,
        max_member_bytes=MAX_MEMBER_BYTES,
    )


def _bound_raw_metadata(downloaded: DownloadedFile) -> tuple[int, str]:
    try:
        return bound_download_metadata(downloaded)
    except ValueError as error:
        raise ValueError("archive changed during audit") from error


def _archive_outputs(raw_path: Path, temporary_root: Path) -> list[dict[str, object]]:
    entries = validated_zip_members(raw_path, _zip_limits())
    extracted = extract_members(raw_path, entries, temporary_root / "members")
    metadata = bound_extracted_metadata(extracted)
    outputs: list[dict[str, object]] = []
    for entry, (size, sha256) in zip(entries, metadata, strict=True):
        outputs.append(
            {
                "object_name": entry.object_name,
                "member_path": entry.member_path,
                "size_bytes": size,
                "sha256": sha256,
            }
        )
    return outputs


def audit_http(url: str, *, archive: bool) -> dict[str, object]:
    """Return metadata for a temporary HTTP download and optional ZIP members."""
    raw_name = _artifact_name(url)

    with tempfile.TemporaryDirectory() as temporary_directory:
        temporary_root = Path(temporary_directory)
        raw_path = temporary_root / "download"
        downloaded = download_bounded(
            url,
            raw_path,
            MAX_DOWNLOAD_BYTES,
            deadline_seconds=DOWNLOAD_TIMEOUT_SECONDS,
            transport=DOWNLOAD_TRANSPORT,
        )

        raw_size, raw_sha256 = _bound_raw_metadata(downloaded)
        if raw_size == 0:
            raise ValueError("artifact must not be empty")
        raw = {"name": raw_name, "size_bytes": raw_size, "sha256": raw_sha256}
        if archive:
            outputs = _archive_outputs(raw_path, temporary_root)
            if _bound_raw_metadata(downloaded) != (raw_size, raw_sha256):
                raise ValueError("archive changed during audit")
        else:
            outputs = [
                {
                    "object_name": raw_name,
                    "size_bytes": raw_size,
                    "sha256": raw_sha256,
                    "raw_identity": True,
                }
            ]
        evidence = {
            key: value
            for key, value in {
                "etag": downloaded.evidence.etag,
                "last_modified": downloaded.evidence.last_modified,
            }.items()
            if value is not None
        }
        return {"url": url, "evidence": evidence, "raw": raw, "outputs": outputs}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    http = commands.add_parser("http", help="audit an authoritative HTTPS artifact")
    http.add_argument("--url", required=True)
    http.add_argument("--archive", action="store_true", help="treat the downloaded artifact as ZIP")
    http.add_argument("--output", required=True, type=Path, help="metadata YAML output path")
    return parser


def _absolute_path(path: Path) -> Path:
    return Path(os.path.abspath(path))


def _validate_output_target(output: Path) -> None:
    registry = REGISTRY_PATH.resolve()
    if output.resolve() == registry or (output.exists() and output.samefile(registry)):
        raise ValueError("refusing to write dataset registry")


def _publish_yaml(result: dict[str, object], output: Path) -> None:
    serialized = yaml.safe_dump(result, sort_keys=False)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=output.parent,
            prefix=f".{output.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary_path = Path(stream.name)
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        _validate_output_target(output)
        os.replace(temporary_path, output)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as error:
        return int(error.code)

    output = _absolute_path(args.output)
    try:
        _validate_output_target(output)
    except ValueError as error:
        parser.print_usage(sys.stderr)
        print(f"{parser.prog}: error: {error}", file=sys.stderr)
        return 2

    try:
        result = audit_http(args.url, archive=args.archive)
        _publish_yaml(result, output)
    except ValueError as error:
        parser.print_usage(sys.stderr)
        print(f"{parser.prog}: error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
