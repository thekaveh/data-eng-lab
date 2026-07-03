import configparser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_gitmodules_points_at_atlas():
    gm = ROOT / ".gitmodules"
    assert gm.exists(), ".gitmodules missing"
    cp = configparser.ConfigParser()
    cp.read_string(gm.read_text())
    sections = {s: dict(cp.items(s)) for s in cp.sections()}
    infra = next((v for v in sections.values() if v.get("path") == "infra"), None)
    assert infra is not None, "no submodule at path 'infra'"
    assert "thekaveh/atlas" in infra["url"], infra["url"]
