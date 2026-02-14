"""Shared pytest fixtures for dbt-viz tests."""

import json
from pathlib import Path

import pytest

from dbt_viz.manifest import ManifestParser


@pytest.fixture
def manifest_path() -> Path:
    """Path to test manifest.json fixture."""
    return Path("tests/fixtures/manifest.json")


@pytest.fixture
def catalog_path() -> Path:
    """Path to test catalog.json fixture."""
    return Path("tests/fixtures/catalog.json")


@pytest.fixture
def compiled_path() -> Path:
    """Path to test compiled directory fixture."""
    return Path("tests/fixtures/compiled")


@pytest.fixture
def manifest_parser(manifest_path: Path) -> ManifestParser:
    """Parsed ManifestParser instance from test fixtures."""
    parser = ManifestParser(manifest_path)
    parser.parse()
    return parser


@pytest.fixture
def manifest_data(manifest_path: Path) -> dict:
    """Raw manifest data as dict from JSON."""
    with open(manifest_path) as f:
        return json.load(f)


# SQL fixture strings for testing sql_lineage.py


@pytest.fixture
def sample_sql_passthrough() -> str:
    """Simple passthrough SELECT with no transformations."""
    return "SELECT id, name FROM customers"


@pytest.fixture
def sample_sql_rename() -> str:
    """SELECT with column rename using AS."""
    return "SELECT id, customer_name AS name FROM customers"


@pytest.fixture
def sample_sql_aggregation() -> str:
    """SELECT with aggregation function and GROUP BY."""
    return "SELECT customer_id, COUNT(*) AS order_count FROM orders GROUP BY customer_id"


@pytest.fixture
def sample_sql_window() -> str:
    """SELECT with window function."""
    return (
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at) "
        "AS rn FROM orders"
    )


@pytest.fixture
def sample_sql_cte() -> str:
    """SELECT with single CTE."""
    return "WITH cte AS (SELECT id, name FROM customers) SELECT * FROM cte"


@pytest.fixture
def sample_sql_nested_cte() -> str:
    """SELECT with nested CTEs."""
    return "WITH a AS (SELECT id FROM customers), b AS (SELECT id FROM a) SELECT * FROM b"


@pytest.fixture
def sample_sql_union() -> str:
    """SELECT with UNION ALL."""
    return "SELECT id, name FROM customers UNION ALL SELECT id, name FROM prospects"


@pytest.fixture
def sample_sql_subquery() -> str:
    """SELECT with subquery."""
    return "SELECT * FROM (SELECT id, name FROM customers) sub"


@pytest.fixture
def sample_sql_star() -> str:
    """SELECT * from table."""
    return "SELECT * FROM customers"


@pytest.fixture
def sample_sql_derived() -> str:
    """SELECT with derived column (concatenation)."""
    return "SELECT id, first_name || ' ' || last_name AS full_name FROM customers"


@pytest.fixture
def sample_sql_literal() -> str:
    """SELECT with literal value."""
    return "SELECT id, 'active' AS status FROM customers"


@pytest.fixture
def tmp_manifest(tmp_path: Path) -> Path:
    """Temporary manifest fixture for isolated tests."""
    manifest = {"nodes": {}, "sources": {}}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return path
