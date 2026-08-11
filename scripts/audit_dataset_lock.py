#!/usr/bin/env python3
"""Download an HTTP artifact into temporary storage and emit lock metadata."""
from __future__ import annotations

import argparse
import io
import os
import stat
import struct
import sys
import tempfile
import time
import zipfile
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urljoin, urlsplit

import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from datasets import schema as dataset_schema  # noqa: E402
from datasets.locking import file_metadata, validate_relative_path  # noqa: E402

REGISTRY_PATH = ROOT / "datasets" / "registry.yaml"
DOWNLOAD_TIMEOUT_SECONDS = 120
MAX_REDIRECTS = 5
MAX_DOWNLOAD_BYTES = 2 * 1024**3
MAX_ARCHIVE_MEMBERS = 10_000
MAX_MEMBER_BYTES = 2 * 1024**3
MAX_TOTAL_UNCOMPRESSED_BYTES = 4 * 1024**3
MAX_COMPRESSION_RATIO = 200
MAX_CENTRAL_DIRECTORY_BYTES = 128 * 1024**2
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_SUPPORTED_ZIP_COMPRESSION = frozenset({zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED})
_EOCD_SIGNATURE = b"PK\x05\x06"
_ZIP64_EOCD_SIGNATURE = b"PK\x06\x06"
_ZIP64_LOCATOR_SIGNATURE = b"PK\x06\x07"
_CENTRAL_DIRECTORY_SIGNATURE = b"PK\x01\x02"
_EOCD_SIZE = 22
_ZIP64_EOCD_SIZE = 56
_ZIP64_LOCATOR_SIZE = 20
_CENTRAL_DIRECTORY_HEADER_SIZE = 46


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


def _remaining_download_time(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ValueError("download deadline exceeded")
    return remaining


def _response_evidence(response: requests.Response) -> dict[str, str]:
    evidence: dict[str, str] = {}
    if raw_etag := response.headers.get("ETag"):
        etag = raw_etag.strip()
        value = etag[2:].lstrip() if etag.startswith("W/") else etag
        if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        if value.strip():
            evidence["etag"] = etag
    if raw_last_modified := response.headers.get("Last-Modified"):
        if last_modified := raw_last_modified.strip():
            evidence["last_modified"] = last_modified
    return evidence


def _response_socket(response: requests.Response) -> object | None:
    connection = getattr(response.raw, "_connection", None)
    if socket := getattr(connection, "sock", None):
        return socket
    http_response = getattr(response.raw, "_fp", None)
    buffered_reader = getattr(http_response, "fp", None)
    socket_io = getattr(buffered_reader, "raw", None)
    return getattr(socket_io, "_sock", None)


def _bounded_response_chunks(response: requests.Response, deadline: float):
    read1 = getattr(response.raw, "read1", None)
    if not callable(read1):
        raise ValueError("HTTP transport does not support bounded reads")
    socket = _response_socket(response)
    if socket is None and not isinstance(getattr(response.raw, "_fp", None), io.BytesIO):
        raise ValueError("HTTP transport does not expose a bounded socket")
    while True:
        remaining = _remaining_download_time(deadline)
        if socket is not None:
            socket.settimeout(remaining)
        chunk = read1(1 << 20, decode_content=False)
        _remaining_download_time(deadline)
        if not chunk:
            return
        yield chunk


def _member_is_symlink(member: zipfile.ZipInfo) -> bool:
    return member.create_system == 3 and stat.S_ISLNK(member.external_attr >> 16)


def _member_is_regular_file(member: zipfile.ZipInfo) -> bool:
    if member.is_dir():
        return False
    mode = member.external_attr >> 16
    file_type = stat.S_IFMT(mode)
    return file_type == 0 or stat.S_ISREG(mode)


def _bounded_zip_metadata(entries: int, central_directory_size: int) -> None:
    if entries > MAX_ARCHIVE_MEMBERS:
        raise ValueError(f"archive contains more than {MAX_ARCHIVE_MEMBERS} members")
    if central_directory_size > MAX_CENTRAL_DIRECTORY_BYTES:
        raise ValueError(f"archive central directory exceeds {MAX_CENTRAL_DIRECTORY_BYTES} bytes")


def _selected_eocd(path: Path) -> tuple[bytes, int]:
    file_size = path.stat().st_size
    tail_start = max(file_size - (1 << 16) - _EOCD_SIZE, 0)
    with path.open("rb") as stream:
        stream.seek(tail_start)
        tail = stream.read()

    selected = -1
    if (
        len(tail) >= _EOCD_SIZE
        and tail[-_EOCD_SIZE : -_EOCD_SIZE + 4] == _EOCD_SIGNATURE
        and tail[-2:] == b"\0\0"
    ):
        selected = len(tail) - _EOCD_SIZE
    else:
        selected = tail.rfind(_EOCD_SIGNATURE)
    if selected < 0 or len(tail) - selected < _EOCD_SIZE:
        raise ValueError("artifact is not a valid ZIP archive")

    record = tail[selected : selected + _EOCD_SIZE]
    comment_size = struct.unpack_from("<H", record, 20)[0]
    if selected + _EOCD_SIZE + comment_size != len(tail):
        raise ValueError("artifact has an inconsistent end-of-central-directory record")

    search_end = selected
    while search_end:
        candidate = tail.rfind(_EOCD_SIGNATURE, 0, search_end)
        if candidate < 0:
            break
        candidate_record = tail[candidate : candidate + _EOCD_SIZE]
        if len(candidate_record) == _EOCD_SIZE:
            candidate_comment_size = struct.unpack_from("<H", candidate_record, 20)[0]
            if candidate + _EOCD_SIZE + candidate_comment_size == len(tail):
                raise ValueError("artifact has an ambiguous end-of-central-directory record")
        search_end = candidate
    return record, tail_start + selected


def _zip64_metadata(path: Path, eocd_offset: int) -> tuple[int, int, int] | None:
    locator_offset = eocd_offset - _ZIP64_LOCATOR_SIZE
    if locator_offset < 0:
        return None
    with path.open("rb") as stream:
        stream.seek(locator_offset)
        locator = stream.read(_ZIP64_LOCATOR_SIZE)
        if len(locator) != _ZIP64_LOCATOR_SIZE:
            return None
        signature, disk, zip64_offset, disk_count = struct.unpack("<4sLQL", locator)
        if signature != _ZIP64_LOCATOR_SIGNATURE:
            return None
        if disk != 0 or disk_count != 1:
            raise ValueError("multi-disk ZIP archives are not supported")
        record_offset = locator_offset - _ZIP64_EOCD_SIZE
        if record_offset < 0:
            raise ValueError("ZIP64 record layout is inconsistent")
        stream.seek(record_offset)
        record = stream.read(_ZIP64_EOCD_SIZE)

    if len(record) != _ZIP64_EOCD_SIZE:
        raise ValueError("ZIP64 record layout is inconsistent")
    (
        signature,
        record_size,
        _version_made,
        _version_needed,
        disk,
        central_directory_disk,
        entries_on_disk,
        entries,
        central_directory_size,
        central_directory_offset,
    ) = struct.unpack("<4sQ2H2L4Q", record)
    if signature != _ZIP64_EOCD_SIGNATURE or record_size != 44:
        raise ValueError("ZIP64 record layout is inconsistent")
    if disk != 0 or central_directory_disk != 0 or entries_on_disk != entries:
        raise ValueError("multi-disk ZIP archives are not supported")
    _bounded_zip_metadata(entries, central_directory_size)
    if zip64_offset != central_directory_offset + central_directory_size:
        raise ValueError("ZIP64 record layout is inconsistent")
    return entries, central_directory_size, record_offset


def _stream_validate_central_directory(
    path: Path,
    start: int,
    size: int,
    declared_entries: int,
) -> None:
    if start < 0:
        raise ValueError("artifact has an invalid central-directory offset")
    remaining = size
    actual_entries = 0
    with path.open("rb") as stream:
        stream.seek(start)
        while remaining:
            if remaining < _CENTRAL_DIRECTORY_HEADER_SIZE:
                raise ValueError("central directory has a truncated fixed header")
            header = stream.read(_CENTRAL_DIRECTORY_HEADER_SIZE)
            if len(header) != _CENTRAL_DIRECTORY_HEADER_SIZE:
                raise ValueError("central directory has a truncated fixed header")
            if header[:4] != _CENTRAL_DIRECTORY_SIGNATURE:
                raise ValueError("central directory has an invalid file-header signature")

            filename_size, extra_size, comment_size = struct.unpack_from("<3H", header, 28)
            variable_size = filename_size + extra_size + comment_size
            record_size = _CENTRAL_DIRECTORY_HEADER_SIZE + variable_size
            if record_size > remaining:
                raise ValueError("central directory record exceeds declared region")
            while variable_size:
                chunk = stream.read(min(variable_size, 1 << 20))
                if not chunk:
                    raise ValueError("central directory record exceeds declared region")
                variable_size -= len(chunk)
            remaining -= record_size
            actual_entries += 1
            if actual_entries > MAX_ARCHIVE_MEMBERS:
                raise ValueError(f"archive contains more than {MAX_ARCHIVE_MEMBERS} members")

    if actual_entries != declared_entries:
        raise ValueError(
            f"central directory contains {actual_entries} records but declares {declared_entries}"
        )


def _preflight_zip(path: Path) -> None:
    eocd, eocd_offset = _selected_eocd(path)

    (
        _signature,
        disk,
        central_directory_disk,
        entries_on_disk,
        entries,
        central_directory_size,
        central_directory_offset,
        _comment_size,
    ) = struct.unpack("<4s4H2LH", eocd)
    sentinel_metadata = (
        entries_on_disk == 0xFFFF
        or entries == 0xFFFF
        or central_directory_size == 0xFFFFFFFF
        or central_directory_offset == 0xFFFFFFFF
    )
    zip64_metadata = _zip64_metadata(path, eocd_offset)
    if zip64_metadata is not None:
        entries, central_directory_size, central_directory_end = zip64_metadata
    else:
        if sentinel_metadata:
            raise ValueError("ZIP64 record layout is inconsistent")
        if disk != 0 or central_directory_disk != 0 or entries_on_disk != entries:
            raise ValueError("multi-disk ZIP archives are not supported")
        _bounded_zip_metadata(entries, central_directory_size)
        central_directory_end = eocd_offset

    central_directory_start = central_directory_end - central_directory_size
    _stream_validate_central_directory(
        path,
        central_directory_start,
        central_directory_size,
        entries,
    )


def _validate_archive_members(members: list[zipfile.ZipInfo]) -> None:
    if len(members) > MAX_ARCHIVE_MEMBERS:
        raise ValueError(f"archive contains more than {MAX_ARCHIVE_MEMBERS} members")

    total_size = 0
    object_names: set[str] = set()
    for member in members:
        if member.flag_bits & ((1 << 0) | (1 << 6)):
            raise ValueError(f"encrypted archive member {member.filename} is not supported")
        if member.compress_type not in _SUPPORTED_ZIP_COMPRESSION:
            raise ValueError(
                f"archive member {member.filename} uses unsupported compression method "
                f"{member.compress_type}"
            )
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

        if member.file_size > MAX_MEMBER_BYTES:
            raise ValueError(f"archive member {member.filename} exceeds {MAX_MEMBER_BYTES} bytes")
        total_size += member.file_size
        if total_size > MAX_TOTAL_UNCOMPRESSED_BYTES:
            raise ValueError(f"archive exceeds {MAX_TOTAL_UNCOMPRESSED_BYTES} uncompressed bytes")
        if member.file_size and (
            member.compress_size == 0
            or member.file_size > MAX_COMPRESSION_RATIO * member.compress_size
        ):
            raise ValueError(
                f"archive member {member.filename} exceeds compression ratio {MAX_COMPRESSION_RATIO}"
            )


def _archive_outputs(raw_path: Path, temporary_root: Path) -> list[dict[str, object]]:
    outputs: list[dict[str, object]] = []
    _preflight_zip(raw_path)
    try:
        with zipfile.ZipFile(raw_path) as archive_file:
            members = archive_file.infolist()
            _validate_archive_members(members)

            extracted_root = temporary_root / "members"
            extracted_root.mkdir()
            total_extracted = 0
            for index, member in enumerate(members):
                extracted_path = extracted_root / str(index)
                member_size = 0
                with archive_file.open(member) as source, extracted_path.open("wb") as target:
                    for chunk in iter(lambda: source.read(1 << 20), b""):
                        member_size += len(chunk)
                        total_extracted += len(chunk)
                        if member_size > MAX_MEMBER_BYTES:
                            raise ValueError(
                                f"archive member {member.filename} exceeds {MAX_MEMBER_BYTES} bytes"
                            )
                        if total_extracted > MAX_TOTAL_UNCOMPRESSED_BYTES:
                            raise ValueError(
                                f"archive exceeds {MAX_TOTAL_UNCOMPRESSED_BYTES} uncompressed bytes"
                            )
                        if member_size and (
                            member.compress_size == 0
                            or member_size > MAX_COMPRESSION_RATIO * member.compress_size
                        ):
                            raise ValueError(
                                f"archive member {member.filename} exceeds compression ratio "
                                f"{MAX_COMPRESSION_RATIO}"
                            )
                        target.write(chunk)
                size, sha256 = _metadata(extracted_path)
                outputs.append(
                    {
                        "object_name": PurePosixPath(member.filename).name,
                        "member_path": member.filename,
                        "size_bytes": size,
                        "sha256": sha256,
                    }
                )
    except zipfile.BadZipFile as error:
        raise ValueError("artifact is not a valid ZIP archive") from error

    if not outputs:
        raise ValueError("archive must contain at least one regular file")
    return outputs


def audit_http(url: str, *, archive: bool) -> dict[str, object]:
    """Return metadata for a temporary HTTP download and optional ZIP members."""
    _validate_url(url)
    raw_name = _artifact_name(url)
    deadline = time.monotonic() + DOWNLOAD_TIMEOUT_SECONDS

    with tempfile.TemporaryDirectory() as temporary_directory:
        temporary_root = Path(temporary_directory)
        raw_path = temporary_root / "download"
        current_url = url
        redirect_count = 0
        while True:
            timeout = _remaining_download_time(deadline)
            with requests.get(
                current_url,
                stream=True,
                timeout=timeout,
                allow_redirects=False,
                headers={"Accept-Encoding": "identity"},
            ) as response:
                response.raise_for_status()
                if response.status_code in _REDIRECT_STATUSES:
                    location = response.headers.get("Location")
                    if not location or not location.strip():
                        raise ValueError("redirect response is missing Location")
                    if redirect_count >= MAX_REDIRECTS:
                        raise ValueError(f"too many redirects (maximum {MAX_REDIRECTS})")
                    next_url = urljoin(current_url, location.strip())
                    _validate_url(next_url)
                    current_url = next_url
                    redirect_count += 1
                    continue

                downloaded = 0
                with raw_path.open("wb") as target:
                    for chunk in _bounded_response_chunks(response, deadline):
                        downloaded += len(chunk)
                        if downloaded > MAX_DOWNLOAD_BYTES:
                            raise ValueError(f"download exceeds {MAX_DOWNLOAD_BYTES} bytes")
                        target.write(chunk)
                evidence = _response_evidence(response)
                break

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
