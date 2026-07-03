import importlib.util
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.infra


def _load(mod):
    spec = importlib.util.spec_from_file_location(mod, ROOT / "tests" / "infra" / f"{mod}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1",
                    reason="set RUN_INFRA=1 with a live Atlas data-eng stack")
def test_layer1_all_pass_against_live_stack():
    preflight = _load("preflight")
    manifest = _load("manifest")
    results = preflight.run_layer1(manifest.EXPECTED_SERVICES, docker_ok=True)
    print("\n" + preflight.render_matrix(results))
    fails = [r for r in results if r.status == "fail"]
    assert not fails, f"preflight L1 failures: {fails}"
