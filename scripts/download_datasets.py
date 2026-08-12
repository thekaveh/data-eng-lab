#!/usr/bin/env python3
"""Verify and atomically publish locked dataset generations to MinIO."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from datasets.locking import canonical_json  # noqa: E402
from datasets.publication import PublicationFailure, PublishMode, publish_dataset  # noqa: E402
from datasets.registry import load_registry, resolve_scale  # noqa: E402
from datasets.s3 import s3_client_from_env  # noqa: E402
from datasets.sources.http import fetch_http  # noqa: E402
from datasets.sources.tpch import generate_tpch  # noqa: E402

BUCKET = "landing"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def plan_uploads(datasets: dict, scale: str, only: list[str] | None) -> list[tuple[str, str]]:
    if only:
        unknown = tuple(name for name in only if name not in datasets)
        if unknown:
            raise ValueError(f"unknown dataset selection: {', '.join(unknown)}")
        selected = set(only)
        names = [name for name in datasets if name in selected]
    else:
        names = list(datasets)
    return [(name, scale) for name in names]


def _fetch_files(plan, dest: Path):
    if plan.dataset.kind == "http":
        return fetch_http(plan, dest)
    if plan.dataset.kind == "tpch":
        return generate_tpch(plan, dest)
    raise ValueError(f"unknown fetch kind: {plan.dataset.kind}")


def _load_registry_snapshot(path: Path) -> tuple[dict, str]:
    snapshot = Path(path).read_bytes()
    digest = hashlib.sha256(snapshot).hexdigest()
    descriptor, temporary_name = tempfile.mkstemp(prefix="dataset-registry-", suffix=".yaml")
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(snapshot)
        registry = load_registry(Path(temporary_name))
    finally:
        Path(temporary_name).unlink(missing_ok=True)
    return registry, digest


def _publish_mode(*, force: bool, refresh: bool, verify_only: bool, rollback: str | None) -> PublishMode:
    selected = sum((force, refresh, verify_only, rollback is not None))
    if selected > 1:
        raise ValueError("verify-only, refresh, force, and rollback are mutually exclusive")
    if rollback is not None:
        return PublishMode.ROLLBACK
    if verify_only:
        return PublishMode.VERIFY_ONLY
    if force or refresh:
        return PublishMode.REFRESH
    return PublishMode.DEFAULT


def run(
    registry_path,
    infra_dir,
    scale,
    only,
    force,
    dry_run,
    client=None,
    *,
    verify_only: bool = False,
    refresh: bool = False,
    rollback_manifest_digest: str | None = None,
) -> int:
    mode = _publish_mode(
        force=force,
        refresh=refresh,
        verify_only=verify_only,
        rollback=rollback_manifest_digest,
    )
    if verify_only and dry_run:
        raise ValueError("--verify-only and --dry-run cannot be combined")
    registry_path = Path(registry_path)
    datasets, raw_registry_sha256 = _load_registry_snapshot(registry_path)
    pairs = plan_uploads(datasets, scale, only)
    if mode is PublishMode.ROLLBACK:
        if len(pairs) != 1 or not only or len(only) != 1:
            raise ValueError("rollback requires exactly one explicit dataset")
        if rollback_manifest_digest is None or _SHA256_RE.fullmatch(rollback_manifest_digest) is None:
            raise ValueError("rollback manifest must be 64 lowercase hexadecimal characters")
    if client is None:
        client = s3_client_from_env(Path(infra_dir))

    failed = False
    for name, selected_scale in pairs:
        try:
            plan = resolve_scale(datasets[name], selected_scale)
            result = publish_dataset(
                plan,
                mode=mode,
                client=client,
                fetcher=_fetch_files,
                rollback_sha256=rollback_manifest_digest,
                dry_run=dry_run,
                raw_registry_sha256=raw_registry_sha256,
            )
        except Exception as error:
            failed = True
            if isinstance(error, PublicationFailure):
                print(canonical_json(error.result.to_document()).decode("utf-8"), file=sys.stderr)
            print(f"! {name} @ {selected_scale}: {type(error).__name__}: {error}", file=sys.stderr)
            for note in getattr(error, "__notes__", ()):
                print(f"! {name} note: {note}", file=sys.stderr)
            continue
        print(canonical_json(result.to_document()).decode("utf-8"))
        if result.cleanup_warning is not None:
            print(f"! {name} cleanup: {result.cleanup_warning}", file=sys.stderr)
    return 1 if failed else 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify or publish locked datasets in MinIO.")
    parser.add_argument("--scale", choices=["tiny", "small", "medium"])
    parser.add_argument("--only", action="append", help="dataset name (repeatable)")
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--verify-only", action="store_true", help="verify the complete active generation")
    actions.add_argument("--refresh", action="store_true", help="publish a fresh verified generation")
    actions.add_argument("--force", action="store_true", help="deprecated alias for --refresh")
    actions.add_argument("--rollback-manifest", metavar="SHA256", help="repoint to a verified retained manifest")
    parser.add_argument("--dry-run", action="store_true", help="describe the action without source or S3 mutation")
    parser.add_argument("--registry", default=str(ROOT / "datasets" / "registry.yaml"))
    parser.add_argument("--infra-dir", default=str(ROOT / "infra"))
    return parser


def main(argv=None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.verify_only and args.dry_run:
        parser.error("--verify-only and --dry-run cannot be combined")
    if args.rollback_manifest is not None:
        if _SHA256_RE.fullmatch(args.rollback_manifest) is None:
            parser.error("--rollback-manifest requires a 64-character lowercase hexadecimal digest")
        if args.only is None or len(args.only) != 1:
            parser.error("rollback requires exactly one --only dataset")
        if args.scale is None:
            parser.error("rollback requires one explicit --scale")
    environment_scale = os.environ.get("DATASET_SCALE")
    if environment_scale is not None and environment_scale not in {"tiny", "small", "medium"}:
        parser.error("DATASET_SCALE must be one of: tiny, small, medium")
    effective_scale = args.scale or environment_scale or "small"
    if args.force:
        print("warning: --force is deprecated; use --refresh", file=sys.stderr)
    try:
        return run(
            args.registry,
            args.infra_dir,
            effective_scale,
            args.only,
            args.force,
            args.dry_run,
            verify_only=args.verify_only,
            refresh=args.refresh,
            rollback_manifest_digest=args.rollback_manifest,
        )
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    sys.exit(main())
