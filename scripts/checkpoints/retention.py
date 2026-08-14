"""Fixed-origin command line client for checkpoint retention."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path


class CliFailure(ValueError):
    def __init__(self, code: str, exit_code: int) -> None:
        super().__init__(code)
        self.code = code
        self.exit_code = exit_code


_ORIGIN = "http://checkpoint-retention:8080"
_MAX_BODY = 65_536
_MAX_PLAN_BODY = 128 * 1024 * 1024


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="checkpoint-retention")
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--checkpoint-id", required=True)
    plan.add_argument("--prefix", required=True)
    plan.add_argument("--facts", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--plan", type=Path, required=True)
    prepare.add_argument("--plan-sha256", required=True)
    prepare.add_argument("--review", required=True)
    prepare.add_argument("--actor", required=True)
    apply = commands.add_parser("apply")
    apply.add_argument("--operation-id", required=True)
    apply.add_argument("--plan-sha256", required=True)
    apply.add_argument("--confirm-prefix", required=True)
    status = commands.add_parser("status")
    status.add_argument("--operation-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0].startswith("--") and arguments[0] != "--help":
        arguments.insert(0, "plan")
    namespace = _parser().parse_args(arguments)
    try:
        origin = os.environ.get("CHECKPOINT_RETENTION_URI", _ORIGIN)
        token = os.environ.get("CHECKPOINT_RETENTION_OPERATOR_TOKEN")
        if origin != _ORIGIN or not isinstance(token, str) or not token or len(token.encode()) > 256:
            raise CliFailure("configuration_invalid", 2)
        path, payload, output = _command(namespace)
        response = _request(path, payload, token)
        if output is not None:
            _write_exclusive(output, response)
        sys.stdout.buffer.write(response + b"\n")
        state = json.loads(response).get("state")
        return {"not_ready": 3, "refused": 3, "partial": 4}.get(state, 0)
    except CliFailure as error:
        print(error.code, file=sys.stderr)
        return error.exit_code


def _command(namespace) -> tuple[str, dict[str, object] | None, Path | None]:
    if namespace.command == "plan":
        facts = _read_json_file(namespace.facts)
        if set(facts) != {"actor"} or not isinstance(facts["actor"], str):
            raise CliFailure("file_invalid", 2)
        payload = {
            "actor": facts["actor"],
            "checkpoint_id": namespace.checkpoint_id,
            "prefix": namespace.prefix,
        }
        return "/v1/plans", payload, namespace.output
    if namespace.command == "prepare":
        raw, plan = _read_json_file_with_bytes(namespace.plan, max_bytes=_MAX_PLAN_BODY)
        if hashlib.sha256(raw).hexdigest() != namespace.plan_sha256:
            raise CliFailure("plan_digest_mismatch", 2)
        return (
            "/v1/operations/prepare",
            {
                "actor": namespace.actor,
                "plan": plan,
                "plan_sha256": namespace.plan_sha256,
                "review": namespace.review,
            },
            None,
        )
    if namespace.command == "apply":
        return (
            f"/v1/operations/{namespace.operation_id}/apply",
            {"confirm_prefix": namespace.confirm_prefix, "plan_sha256": namespace.plan_sha256},
            None,
        )
    return f"/v1/operations/{namespace.operation_id}", None, None


def _request(path: str, payload: dict[str, object] | None, token: str) -> bytes:
    request_bound = _MAX_PLAN_BODY if path == "/v1/operations/prepare" else _MAX_BODY
    response_bound = _MAX_PLAN_BODY if path == "/v1/plans" else _MAX_BODY
    body = None if payload is None else _canonical(payload, max_bytes=request_bound)
    request = urllib.request.Request(
        _ORIGIN + path,
        data=body,
        method="GET" if body is None else "POST",
        headers={
            "Authorization": f"Bearer {token}",
            **({"Content-Type": "application/json", "Content-Length": str(len(body))} if body is not None else {}),
        },
    )
    response = None
    result: bytes | None = None
    primary: BaseException | None = None
    try:
        response = _open(request, timeout=930 if path.endswith("/apply") else 30)
        raw = response.read(response_bound + 1)
        if type(raw) is not bytes or len(raw) > response_bound:
            raise CliFailure("response_invalid", 5)
        decoded = _decode_response(raw)
        if not isinstance(decoded, dict):
            raise CliFailure("response_invalid", 5)
        result = _canonical(decoded, max_bytes=response_bound)
    except CliFailure as error:
        primary = error
        raise
    except urllib.error.HTTPError as error:
        response = error
        try:
            raw = error.read(_MAX_BODY + 1)
            value = _decode_response(raw)
            state = value.get("state")
        except BaseException:
            state = None
        exit_code = (
            4
            if state == "partial"
            else 3
            if state == "refused" or error.code in {409, 412, 423}
            else 2
            if error.code == 400
            else 5
        )
        primary = CliFailure(
            "service_partial" if exit_code == 4 else "service_refused" if exit_code == 3 else "service_failure",
            exit_code,
        )
        raise primary from None
    except (KeyboardInterrupt, SystemExit) as error:
        primary = error
        raise
    except BaseException:
        primary = CliFailure("service_failure", 5)
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
                    raise CliFailure("response_close_failed", 5) from None
    if result is None:
        raise CliFailure("response_invalid", 5)
    return result


def _open(request, timeout):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return opener.open(request, timeout=timeout)


def _read_json_file(path: Path, *, max_bytes: int = _MAX_BODY) -> dict[str, object]:
    return _read_json_file_with_bytes(path, max_bytes=max_bytes)[1]


def _read_json_file_with_bytes(path: Path, *, max_bytes: int = _MAX_BODY) -> tuple[bytes, dict[str, object]]:
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise CliFailure("file_invalid", 2)
            result[key] = value
        return result

    try:
        with path.open("rb") as handle:
            body = handle.read(max_bytes + 1)
        if len(body) > max_bytes:
            raise CliFailure("file_too_large", 2)
        value = json.loads(body.decode("utf-8"), object_pairs_hook=pairs)
    except CliFailure:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise CliFailure("file_invalid", 2) from None
    if not isinstance(value, dict):
        raise CliFailure("file_invalid", 2)
    return body, value


def _write_exclusive(path: Path, body: bytes) -> None:
    temporary: Path | None = None
    try:
        if path.exists():
            raise FileExistsError
        descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        temporary = Path(name)
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except FileExistsError:
        raise CliFailure("output_exists", 2) from None
    except OSError:
        raise CliFailure("output_write_failed", 5) from None
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _canonical(value: object, *, max_bytes: int = _MAX_BODY) -> bytes:
    try:
        body = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    except (TypeError, ValueError, UnicodeError):
        raise CliFailure("json_invalid", 2) from None
    if len(body) > max_bytes:
        raise CliFailure("body_too_large", 2)
    return body


def _decode_response(body: bytes) -> dict[str, object]:
    def pairs(values):
        result = {}
        for key, value in values:
            if key in result:
                raise CliFailure("response_invalid", 5)
            result[key] = value
        return result

    try:
        value = json.loads(body.decode("utf-8"), object_pairs_hook=pairs)
    except CliFailure:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise CliFailure("response_invalid", 5) from None
    if not isinstance(value, dict):
        raise CliFailure("response_invalid", 5)
    return value


if __name__ == "__main__":
    raise SystemExit(main())
