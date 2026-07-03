#!/usr/bin/env python3
"""Create the medallion namespaces (bronze/silver/gold) in the Iceberg REST catalog."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lakehouse.catalog import ensure_namespaces, rest_catalog_from_env  # noqa: E402

DEFAULT_NAMESPACES = ["bronze", "silver", "gold"]


def run(infra_dir, namespaces=None, catalog=None) -> list[str]:
    namespaces = namespaces or DEFAULT_NAMESPACES
    if catalog is None:
        catalog = rest_catalog_from_env(Path(infra_dir))
    created = ensure_namespaces(catalog, namespaces)
    for ns in created:
        print(f"+ created namespace lakehouse.{ns}")
    for ns in namespaces:
        if ns not in created:
            print(f"= namespace lakehouse.{ns} already present")
    return created


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Register medallion namespaces in the Iceberg REST catalog.")
    ap.add_argument("--infra-dir", default=str(ROOT / "infra"))
    ap.add_argument("--namespace", action="append", help="namespace (repeatable; default bronze/silver/gold)")
    args = ap.parse_args(argv)
    run(args.infra_dir, args.namespace or DEFAULT_NAMESPACES)
    return 0


if __name__ == "__main__":
    sys.exit(main())
