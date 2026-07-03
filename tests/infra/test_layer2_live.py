import importlib.util
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.infra


@pytest.mark.skipif(os.environ.get("RUN_INFRA") != "1",
                    reason="set RUN_INFRA=1 with a live enhanced-Atlas stack")
def test_layer2_matrix_all_pass():
    spec = importlib.util.spec_from_file_location("layer2", ROOT / "tests" / "infra" / "layer2.py")
    layer2 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(layer2)
    results = layer2.run_layer2(layer2.EDGES, layer2.default_exec, docker_ok=True)
    print("\n" + layer2.render_matrix(results))
    assert not [r for r in results if r.status == "fail"], results
