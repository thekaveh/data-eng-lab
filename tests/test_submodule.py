import configparser
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ATLAS_PIN = "882877a4a168e5c611bfd3cff8704eeefcf97c9d"


def test_gitmodules_points_at_atlas():
    gm = ROOT / ".gitmodules"
    assert gm.exists(), ".gitmodules missing"
    cp = configparser.ConfigParser()
    cp.read_string(gm.read_text())
    sections = {s: dict(cp.items(s)) for s in cp.sections()}
    infra = next((v for v in sections.values() if v.get("path") == "infra"), None)
    assert infra is not None, "no submodule at path 'infra'"
    assert "thekaveh/atlas" in infra["url"], infra["url"]


def test_infra_gitlink_is_the_reviewed_atlas_commit():
    out = subprocess.run(
        ["git", "ls-files", "-s", "--", "infra"],
        cwd=ROOT,
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()
    mode, sha, stage, path = out.split()
    assert (mode, stage, path) == ("160000", "0", "infra")
    assert sha == ATLAS_PIN
