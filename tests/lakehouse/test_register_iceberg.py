import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("register_iceberg", ROOT / "scripts" / "register_iceberg.py")
reg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(reg)


class _FakeCatalog:
    def __init__(self):
        self._ns = []

    def list_namespaces(self):
        return list(self._ns)

    def create_namespace(self, name):
        self._ns.append((name,))


def test_run_creates_medallion_namespaces():
    fake = _FakeCatalog()
    created = reg.run(infra_dir=Path("/unused"), namespaces=["bronze", "silver", "gold"], catalog=fake)
    assert created == ["bronze", "silver", "gold"]
    # idempotent second run
    assert reg.run(infra_dir=Path("/unused"), namespaces=["bronze", "silver", "gold"], catalog=fake) == []
