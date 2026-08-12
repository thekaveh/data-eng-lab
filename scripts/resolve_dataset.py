#!/usr/bin/env python3
"""Resolve one active verified dataset generation from the host."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from datasets.locking import canonical_json  # noqa: E402
from datasets.publication import resolve_active_dataset  # noqa: E402
from datasets.registry import load_registry  # noqa: E402
from datasets.s3 import s3_client_from_env  # noqa: E402

_SCALES = ("tiny", "small", "medium")


def run(dataset: str, scale: str, registry_path: Path, infra_dir: Path, client=None) -> bytes:
    registry = load_registry(Path(registry_path))
    if client is None:
        client = s3_client_from_env(Path(infra_dir))
    resolved = resolve_active_dataset(client, registry, dataset, scale)
    return canonical_json(asdict(resolved))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Resolve one active verified dataset generation.")
    parser.add_argument("dataset", help="dataset identifier")
    parser.add_argument("--scale", choices=_SCALES)
    parser.add_argument("--registry", default=str(ROOT / "datasets" / "registry.yaml"))
    parser.add_argument("--infra-dir", default=str(ROOT / "infra"))
    return parser


def main(argv=None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    environment_scale = os.environ.get("DATASET_SCALE")
    if environment_scale is not None and environment_scale not in _SCALES:
        parser.error("DATASET_SCALE must be one of: tiny, small, medium")
    effective_scale = args.scale or environment_scale or "small"
    try:
        body = run(args.dataset, effective_scale, Path(args.registry), Path(args.infra_dir))
    except Exception:
        print("dataset resolution failed", file=sys.stderr)
        return 1
    sys.stdout.buffer.write(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
