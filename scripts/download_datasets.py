#!/usr/bin/env python3
"""Download/generate datasets at a chosen SCALE and land them into MinIO's 'landing' bucket."""
from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from datasets.registry import load_registry, resolve_scale  # noqa: E402
from datasets.s3 import object_exists, s3_client_from_env, upload_file  # noqa: E402
from datasets.sources.http import fetch_http  # noqa: E402
from datasets.sources.tpch import generate_tpch  # noqa: E402

BUCKET = "landing"


def plan_uploads(datasets: dict, scale: str, only: list[str] | None) -> list[tuple[str, str]]:
    names = [n for n in datasets if (not only or n in only)]
    return [(n, scale) for n in names]


def _fetch_files(plan, dest: Path) -> list[Path]:
    if plan.dataset.kind == "http":
        return fetch_http(plan, dest)
    if plan.dataset.kind == "tpch":
        return generate_tpch(plan.sf, dest)
    raise ValueError(f"unknown fetch kind: {plan.dataset.kind}")


def run(registry_path, infra_dir, scale, only, force, dry_run, client=None) -> int:
    datasets = load_registry(Path(registry_path))
    pairs = plan_uploads(datasets, scale, only)
    if dry_run:
        for name, sc in pairs:
            print(f"+ would land {name} @ {sc} -> s3://{BUCKET}/{datasets[name].landing_prefix}/")
        return 0

    if client is None:
        client = s3_client_from_env(Path(infra_dir))

    uploaded = 0
    for name, sc in pairs:
        ds = datasets[name]
        plan = resolve_scale(ds, sc)
        with tempfile.TemporaryDirectory() as tmp:
            files = _fetch_files(plan, Path(tmp))
            for f in files:
                key = f"{ds.landing_prefix}/{f.name}"
                if not force and object_exists(client, BUCKET, key):
                    print(f"= skip existing s3://{BUCKET}/{key}")
                    continue
                upload_file(client, f, BUCKET, key)
                uploaded += 1
                print(f"↑ s3://{BUCKET}/{key}")
    return uploaded


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Land datasets into MinIO.")
    ap.add_argument("--scale", choices=["tiny", "small", "medium"], default="small")
    ap.add_argument("--only", action="append", help="dataset name (repeatable)")
    ap.add_argument("--force", action="store_true", help="re-upload even if present")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--registry", default=str(ROOT / "datasets" / "registry.yaml"))
    ap.add_argument("--infra-dir", default=str(ROOT / "infra"))
    args = ap.parse_args(argv)
    n = run(args.registry, args.infra_dir, args.scale, args.only, args.force, args.dry_run)
    print(f"\nlanded {n} object(s) into s3://{BUCKET}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
