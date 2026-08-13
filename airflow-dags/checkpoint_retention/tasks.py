"""Bounded fixed-origin task for retention dry-run summaries."""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from collections.abc import Mapping


class RetentionTaskFailure(ValueError):
    """A sanitized Airflow task failure category."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


CHECKPOINT_IDS = (
    "streaming-events-v1",
    "streaming-event-windows-v1",
    "streaming-online-retail-cdc-v1",
    "streaming-gh-archive-file-v1",
    "go-live-streaming-test-v1",
)
_ORIGIN = "http://checkpoint-retention:8080"
_MAX_BODY = 65_536
_SHA256 = re.compile(r"[0-9a-f]{64}")
_CODE = re.compile(r"[a-z][a-z0-9_]{0,63}")


def run_retention_plans() -> dict[str, object]:
    """Request the complete registry-ordered dry run and return summaries only."""

    token = os.environ.get("CHECKPOINT_RETENTION_OPERATOR_TOKEN")
    origin = os.environ.get("CHECKPOINT_RETENTION_URI", _ORIGIN)
    if origin != _ORIGIN or not isinstance(token, str) or not token or len(token.encode()) > 256:
        raise RetentionTaskFailure("configuration_invalid")
    body = json.dumps(
        {"actor": "airflow-dry-run", "checkpoint_ids": CHECKPOINT_IDS},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    request = urllib.request.Request(
        _ORIGIN + "/v1/plans",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Length": str(len(body)),
            "Content-Type": "application/json",
        },
    )
    response = None
    primary: BaseException | None = None
    try:
        response = _open(request, timeout=30)
        raw = response.read(_MAX_BODY + 1)
        value = _decode(raw)
        _validate_response(value)
        return value
    except (KeyboardInterrupt, SystemExit, RetentionTaskFailure) as error:
        primary = error
        raise
    except BaseException:
        primary = RetentionTaskFailure("service_failure")
        raise primary from None
    finally:
        if response is not None:
            try:
                response.close()
            except (KeyboardInterrupt, SystemExit):
                if primary is None:
                    raise
            except BaseException:
                if primary is None:
                    raise RetentionTaskFailure("response_close_failed") from None


def _open(request, timeout):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return opener.open(request, timeout=timeout)


def _decode(body: object) -> dict[str, object]:
    if type(body) is not bytes or len(body) > _MAX_BODY:
        raise RetentionTaskFailure("response_invalid")

    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise RetentionTaskFailure("response_invalid")
            result[key] = value
        return result

    try:
        value = json.loads(body.decode("utf-8"), object_pairs_hook=pairs)
    except RetentionTaskFailure:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise RetentionTaskFailure("response_invalid") from None
    if not isinstance(value, dict):
        raise RetentionTaskFailure("response_invalid")
    return value


def _validate_response(value: Mapping[str, object]) -> None:
    if set(value) != {"plans", "state"} or value.get("state") != "accepted":
        raise RetentionTaskFailure("response_invalid")
    plans = value.get("plans")
    if not isinstance(plans, list) or len(plans) != len(CHECKPOINT_IDS):
        raise RetentionTaskFailure("response_invalid")
    for checkpoint_id, summary in zip(CHECKPOINT_IDS, plans, strict=True):
        if not isinstance(summary, dict) or set(summary) != {
            "checkpoint_id",
            "decision",
            "inventory",
            "policy_sha256",
            "refusal_codes",
        }:
            raise RetentionTaskFailure("response_invalid")
        inventory = summary["inventory"]
        refusals = summary["refusal_codes"]
        if (
            summary["checkpoint_id"] != checkpoint_id
            or summary["decision"] not in {"eligible", "refused"}
            or not isinstance(summary["policy_sha256"], str)
            or _SHA256.fullmatch(summary["policy_sha256"]) is None
            or not isinstance(inventory, dict)
            or set(inventory) != {"object_count", "total_bytes"}
            or any(type(inventory[key]) is not int or inventory[key] < 0 for key in inventory)
            or not isinstance(refusals, list)
            or len(refusals) > 32
            or any(not isinstance(code, str) or _CODE.fullmatch(code) is None for code in refusals)
        ):
            raise RetentionTaskFailure("response_invalid")
        if (summary["decision"] == "eligible") != (refusals == []):
            raise RetentionTaskFailure("response_invalid")


__all__ = ["CHECKPOINT_IDS", "RetentionTaskFailure", "run_retention_plans"]
