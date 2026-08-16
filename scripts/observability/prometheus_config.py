"""Generate the consumer-owned Prometheus configuration from pinned Atlas input."""

from __future__ import annotations

import argparse
import copy
import hashlib
from collections.abc import Mapping, Sequence
from pathlib import Path

import yaml

BASE_CONFIG_SHA256 = "038325adcb2e12658e740416216968aa3509633a6ff907894bfddfdbd4c4325e"
JOB_NAME = "iceberg-rest-synthetic"
ROOT = Path(__file__).resolve().parents[2]
BASE_PATH = ROOT / "infra/services/prometheus/config/prometheus.yml"
OUTPUT_PATH = ROOT / "observability/prometheus/prometheus.yml"


class ConfigFailure(Exception):
    """A closed Prometheus generation failure."""


def _invalid() -> None:
    raise ConfigFailure("prometheus_config_invalid")


def render_prometheus_config(base: Mapping[str, object]) -> bytes:
    """Append the one fixed synthetic scrape job to a validated Atlas mapping."""

    if not isinstance(base, dict) or set(base) != {
        "global",
        "rule_files",
        "scrape_configs",
    }:
        _invalid()
    if not isinstance(base["global"], dict) or not isinstance(base["rule_files"], list):
        _invalid()
    jobs = base["scrape_configs"]
    if not isinstance(jobs, list) or not jobs:
        _invalid()
    names: list[str] = []
    for job in jobs:
        if not isinstance(job, dict) or not isinstance(job.get("job_name"), str):
            _invalid()
        names.append(job["job_name"])
    if len(names) != len(set(names)) or JOB_NAME in names:
        _invalid()

    rendered = copy.deepcopy(base)
    rendered_jobs = rendered["scrape_configs"]
    if not isinstance(rendered_jobs, list):
        _invalid()
    rendered_jobs.append(
        {
            "job_name": JOB_NAME,
            "scrape_interval": "30s",
            "scrape_timeout": "5s",
            "metrics_path": "/metrics",
            "static_configs": [
                {
                    "targets": ["iceberg-rest-probe:8080"],
                    "labels": {"target": "catalog"},
                }
            ],
        }
    )
    return yaml.safe_dump(
        rendered,
        sort_keys=False,
        allow_unicode=False,
        default_flow_style=False,
    ).encode("ascii")


def _load_base() -> dict[str, object]:
    body = BASE_PATH.read_bytes()
    if hashlib.sha256(body).hexdigest() != BASE_CONFIG_SHA256:
        raise ConfigFailure("prometheus_base_changed")
    value = yaml.safe_load(body)
    if not isinstance(value, dict):
        _invalid()
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--write", action="store_true")
    args = parser.parse_args(argv)
    expected = render_prometheus_config(_load_base())
    if args.check:
        if not OUTPUT_PATH.is_file() or OUTPUT_PATH.read_bytes() != expected:
            raise ConfigFailure("prometheus_output_stale")
        return 0
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_bytes(expected)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
