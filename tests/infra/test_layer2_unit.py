import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load(mod):
    spec = importlib.util.spec_from_file_location(mod, ROOT / "tests" / "infra" / f"{mod}.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


layer2 = _load("layer2")


def _ok_edge(name):
    return layer2.Edge(name, True, lambda exec_fn: (True, "ok"))


def _bad_edge(name):
    return layer2.Edge(name, True, lambda exec_fn: (False, "boom"))


def test_disabled_edge_skipped():
    e = layer2.Edge("x", False, lambda f: (True, "ok"))
    assert layer2.run_layer2([e], exec_fn=None, docker_ok=True)[0].status == "skipped"


def test_docker_down_blocks():
    assert layer2.run_layer2([_ok_edge("x")], exec_fn=None, docker_ok=False)[0].status == "blocked"


def test_pass_and_fail_branches():
    res = layer2.run_layer2([_ok_edge("a"), _bad_edge("b")], exec_fn=lambda c, a: (0, ""), docker_ok=True)
    by = {r.name: r.status for r in res}
    assert by == {"a": "pass", "b": "fail"}


def test_probe_receives_exec_fn():
    captured = {}

    def probe(exec_fn):
        captured["rc"], captured["out"] = exec_fn("jupyterhub", ["echo", "hi"])
        return captured["rc"] == 0, captured["out"]

    fake_exec = lambda container, argv: (0, f"{container}:{' '.join(argv)}")
    res = layer2.run_layer2([layer2.Edge("e", True, probe)], exec_fn=fake_exec, docker_ok=True)
    assert res[0].status == "pass"
    assert captured["out"] == "jupyterhub:echo hi"


def test_render_matrix_contains_status():
    out = layer2.render_matrix([layer2.Result("a", "pass", "ok")])
    assert "a" in out and "PASS" in out
