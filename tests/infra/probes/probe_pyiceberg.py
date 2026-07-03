"""Probe: PyIceberg loads the REST catalog and lists the medallion namespaces. Exit 0 on success."""
from pyiceberg.catalog import load_catalog

cat = load_catalog("lakehouse", **{"type": "rest", "uri": "http://iceberg-rest:8181"})
names = {ns[0] if isinstance(ns, tuple) else ns for ns in cat.list_namespaces()}
assert {"bronze", "silver", "gold"} <= names, f"missing namespaces: {names}"
print("pyiceberg->rest OK")
