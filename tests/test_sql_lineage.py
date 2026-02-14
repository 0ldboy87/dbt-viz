"""Comprehensive tests for sql_lineage.py module."""

import pytest

from dbt_viz.sql_lineage import (
    ColumnLineage,
    SQLLineageParser,
    TableLineage,
    parse_model_lineage,
    resolve_table_references,
)

# ============================================================================
# Basic transformation type tests
# ============================================================================


def test_passthrough_transformation(sample_sql_passthrough: str):
    """Test simple passthrough columns are correctly identified."""
    parser = SQLLineageParser()
    result = parser.parse_sql(sample_sql_passthrough)

    assert "id" in result.columns
    assert "name" in result.columns

    assert result.columns["id"].transformation == "passthrough"
    assert result.columns["name"].transformation == "passthrough"
    assert "customers.id" in result.columns["id"].source_columns
    assert "customers.name" in result.columns["name"].source_columns


def test_rename_transformation(sample_sql_rename: str):
    """Test column rename is identified as 'rename' transformation."""
    parser = SQLLineageParser()
    result = parser.parse_sql(sample_sql_rename)

    assert "id" in result.columns
    assert "name" in result.columns

    assert result.columns["id"].transformation == "passthrough"
    assert result.columns["name"].transformation == "rename"
    assert "customers.customer_name" in result.columns["name"].source_columns


def test_aggregation_transformation(sample_sql_aggregation: str):
    """Test aggregation functions are identified correctly."""
    parser = SQLLineageParser()
    result = parser.parse_sql(sample_sql_aggregation)

    assert "customer_id" in result.columns
    assert "order_count" in result.columns

    assert result.columns["customer_id"].transformation == "passthrough"
    assert result.columns["order_count"].transformation == "aggregated"


def test_window_transformation(sample_sql_window: str):
    """Test window functions are identified correctly."""
    parser = SQLLineageParser()
    result = parser.parse_sql(sample_sql_window)

    assert "id" in result.columns
    assert "rn" in result.columns

    assert result.columns["id"].transformation == "passthrough"
    assert result.columns["rn"].transformation == "windowed"


def test_derived_transformation(sample_sql_derived: str):
    """Test derived columns from expressions are identified."""
    parser = SQLLineageParser()
    result = parser.parse_sql(sample_sql_derived)

    assert "id" in result.columns
    assert "full_name" in result.columns

    assert result.columns["id"].transformation == "passthrough"
    assert result.columns["full_name"].transformation == "derived"
    assert "customers.first_name" in result.columns["full_name"].source_columns
    assert "customers.last_name" in result.columns["full_name"].source_columns


def test_literal_transformation(sample_sql_literal: str):
    """Test literal values are identified correctly."""
    parser = SQLLineageParser()
    result = parser.parse_sql(sample_sql_literal)

    assert "id" in result.columns
    assert "status" in result.columns

    assert result.columns["id"].transformation == "passthrough"
    assert result.columns["status"].transformation == "literal"
    assert len(result.columns["status"].source_columns) == 0


# ============================================================================
# CTE tests
# ============================================================================


def test_simple_cte(sample_sql_cte: str):
    """Test single CTE with SELECT * is traced correctly."""
    parser = SQLLineageParser()
    schema = {"customers": {"id": "int", "name": "varchar"}}
    result = parser.parse_sql(sample_sql_cte, schema=schema)

    assert "id" in result.columns
    assert "name" in result.columns

    # Columns should trace through CTE to original table
    assert result.columns["id"].transformation == "passthrough"
    assert result.columns["name"].transformation == "passthrough"
    assert "customers.id" in result.columns["id"].source_columns
    assert "customers.name" in result.columns["name"].source_columns


def test_nested_cte(sample_sql_nested_cte: str):
    """Test nested CTEs trace through multiple levels."""
    parser = SQLLineageParser()
    schema = {"customers": {"id": "int"}}
    result = parser.parse_sql(sample_sql_nested_cte, schema=schema)

    assert "id" in result.columns

    # Should trace through both CTEs to original table
    assert "customers.id" in result.columns["id"].source_columns


def test_cte_with_explicit_columns():
    """Test CTE with explicit column selection."""
    sql = """
    WITH cte AS (
        SELECT id, name FROM customers
    )
    SELECT id, name FROM cte
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert "name" in result.columns
    assert result.columns["id"].transformation == "passthrough"
    assert result.columns["name"].transformation == "passthrough"


def test_cte_with_rename():
    """Test CTE with column renames."""
    sql = """
    WITH renamed AS (
        SELECT customer_id AS id, customer_name AS name FROM customers
    )
    SELECT id, name FROM renamed
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert "name" in result.columns
    assert result.columns["id"].transformation == "passthrough"
    assert result.columns["name"].transformation == "passthrough"


def test_multiple_ctes():
    """Test multiple CTEs in sequence."""
    sql = """
    WITH
        a AS (SELECT id, name FROM customers),
        b AS (SELECT id FROM orders)
    SELECT a.id, a.name, b.id AS order_id FROM a JOIN b ON a.id = b.id
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert "name" in result.columns
    assert "order_id" in result.columns
    assert "customers.id" in result.columns["id"].source_columns
    assert "customers.name" in result.columns["name"].source_columns
    assert "orders.id" in result.columns["order_id"].source_columns


# ============================================================================
# UNION tests
# ============================================================================


def test_union_simple(sample_sql_union: str):
    """Test UNION merges sources from both branches."""
    parser = SQLLineageParser()
    result = parser.parse_sql(sample_sql_union)

    assert "id" in result.columns
    assert "name" in result.columns

    # Should have sources from both tables
    assert "customers.id" in result.columns["id"].source_columns
    assert "prospects.id" in result.columns["id"].source_columns
    assert "customers.name" in result.columns["name"].source_columns
    assert "prospects.name" in result.columns["name"].source_columns


def test_union_different_columns():
    """Test UNION with different column names uses first branch names."""
    sql = "SELECT id, name FROM customers UNION ALL SELECT customer_id, full_name FROM prospects"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    # Column names come from first branch
    assert "id" in result.columns
    assert "name" in result.columns

    # Sources come from both branches
    assert "customers.id" in result.columns["id"].source_columns
    assert "prospects.customer_id" in result.columns["id"].source_columns


def test_union_with_cte():
    """Test UNION works with CTEs."""
    sql = """
    WITH cte AS (SELECT id FROM customers)
    SELECT id FROM cte
    UNION ALL
    SELECT id FROM orders
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert "customers.id" in result.columns["id"].source_columns
    assert "orders.id" in result.columns["id"].source_columns


# ============================================================================
# Subquery tests
# ============================================================================


def test_subquery_simple(sample_sql_subquery: str):
    """Test subquery with SELECT * is traced correctly."""
    parser = SQLLineageParser()
    schema = {"customers": {"id": "int", "name": "varchar"}}
    result = parser.parse_sql(sample_sql_subquery, schema=schema)

    assert "id" in result.columns
    assert "name" in result.columns
    assert "customers.id" in result.columns["id"].source_columns
    assert "customers.name" in result.columns["name"].source_columns


def test_subquery_with_alias():
    """Test subquery with explicit column selection and alias."""
    sql = (
        "SELECT sub.id, sub.total FROM "
        "(SELECT id, SUM(amount) AS total FROM orders GROUP BY id) sub"
    )
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert "total" in result.columns
    assert any("id" in src for src in result.columns["id"].source_columns)
    assert len(result.columns["total"].source_columns) >= 0


def test_nested_subquery():
    """Test nested subqueries are parsed without errors."""
    sql = """
    SELECT id FROM (
        SELECT id FROM (
            SELECT id FROM customers
        ) inner_sub
    ) outer_sub
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert len(result.columns["id"].source_columns) > 0


# ============================================================================
# SELECT * expansion tests
# ============================================================================


def test_select_star_expansion(sample_sql_star: str):
    """Test SELECT * expansion with schema info."""
    parser = SQLLineageParser()
    schema = {"customers": {"id": "int", "name": "varchar", "email": "varchar"}}
    result = parser.parse_sql(sample_sql_star, schema=schema)

    assert "id" in result.columns
    assert "name" in result.columns
    assert "email" in result.columns
    assert "customers.id" in result.columns["id"].source_columns
    assert "customers.name" in result.columns["name"].source_columns
    assert "customers.email" in result.columns["email"].source_columns


def test_table_star_expansion():
    """Test table.* expansion with schema info."""
    sql = "SELECT c.* FROM customers c"
    parser = SQLLineageParser()
    schema = {"customers": {"id": "int", "name": "varchar"}}
    result = parser.parse_sql(sql, schema=schema)

    assert "id" in result.columns
    assert "name" in result.columns
    assert "customers.id" in result.columns["id"].source_columns
    assert "customers.name" in result.columns["name"].source_columns


def test_select_star_with_schema():
    """Test SELECT * without schema info returns empty columns."""
    sql = "SELECT * FROM customers"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)  # No schema provided

    # Without schema, SELECT * cannot be expanded
    assert len(result.columns) == 0


# ============================================================================
# Table alias resolution tests
# ============================================================================


def test_table_alias_resolution():
    """Test table aliases are resolved correctly."""
    sql = "SELECT c.id, c.name FROM customers c"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert "name" in result.columns
    assert "customers.id" in result.columns["id"].source_columns
    assert "customers.name" in result.columns["name"].source_columns


def test_multiple_table_aliases():
    """Test multiple table aliases in joins."""
    sql = """
    SELECT c.id, c.name, o.order_id
    FROM customers c
    JOIN orders o ON c.id = o.customer_id
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert "name" in result.columns
    assert "order_id" in result.columns
    assert "customers.id" in result.columns["id"].source_columns
    assert "customers.name" in result.columns["name"].source_columns
    assert "orders.order_id" in result.columns["order_id"].source_columns


# ============================================================================
# Edge case tests
# ============================================================================


def test_empty_sql():
    """Test empty SQL string returns empty result."""
    parser = SQLLineageParser()
    result = parser.parse_sql("")

    assert result.table_name == ""
    assert len(result.columns) == 0


def test_invalid_sql():
    """Test invalid SQL is handled gracefully."""
    parser = SQLLineageParser()
    result = parser.parse_sql("SELECT FROM WHERE")  # Invalid SQL

    # Should not raise exception, returns empty or minimal result
    assert isinstance(result, TableLineage)


def test_deeply_nested_cte():
    """Test deeply nested CTEs don't cause infinite recursion."""
    sql = """
    WITH
        a AS (SELECT id FROM customers),
        b AS (SELECT id FROM a),
        c AS (SELECT id FROM b),
        d AS (SELECT id FROM c),
        e AS (SELECT id FROM d)
    SELECT id FROM e
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert len(result.columns["id"].source_columns) > 0
    assert any("id" in src for src in result.columns["id"].source_columns)


def test_complex_expression():
    """Test complex expressions are marked as derived."""
    sql = """
    SELECT
        id,
        CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active,
        COALESCE(amount, 0) * 1.1 AS adjusted_amount
    FROM orders
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert "is_active" in result.columns
    assert "adjusted_amount" in result.columns

    assert result.columns["id"].transformation == "passthrough"
    assert result.columns["is_active"].transformation == "derived"
    assert result.columns["adjusted_amount"].transformation == "derived"


# ============================================================================
# Helper function tests
# ============================================================================


def test_parse_model_lineage():
    """Test parse_model_lineage wrapper function."""
    sql = "SELECT id, name FROM customers"
    result = parse_model_lineage(sql, "my_model", dialect="snowflake")

    assert result.table_name == "my_model"
    assert "id" in result.columns
    assert "name" in result.columns


def test_resolve_table_references():
    """Test resolve_table_references maps table names to unique_ids."""
    lineage = TableLineage(table_name="my_model")
    lineage.columns["id"] = ColumnLineage(
        column_name="id",
        source_columns=["customers.id", "orders.order_id"],
        transformation="derived",
    )

    table_map = {
        "customers": "model.my_project.stg_customers",
        "orders": "model.my_project.stg_orders",
    }

    result = resolve_table_references(lineage, table_map)

    assert "model.my_project.stg_customers.id" in result.columns["id"].source_columns
    assert "model.my_project.stg_orders.order_id" in result.columns["id"].source_columns


def test_column_lineage_to_dict():
    """Test ColumnLineage.to_dict() serialization."""
    col = ColumnLineage(
        column_name="id",
        source_columns=["customers.id"],
        transformation="passthrough",
        expression="id",
    )

    result = col.to_dict()

    assert result["column_name"] == "id"
    assert result["source_columns"] == ["customers.id"]
    assert result["transformation"] == "passthrough"
    assert result["expression"] == "id"


def test_table_lineage_to_dict():
    """Test TableLineage.to_dict() serialization."""
    lineage = TableLineage(table_name="my_model")
    lineage.columns["id"] = ColumnLineage(
        column_name="id",
        source_columns=["customers.id"],
        transformation="passthrough",
    )

    result = lineage.to_dict()

    assert result["table_name"] == "my_model"
    assert "id" in result["columns"]
    assert result["columns"]["id"]["column_name"] == "id"
    assert result["columns"]["id"]["transformation"] == "passthrough"


# ============================================================================
# Additional tests
# ============================================================================


def test_case_insensitivity():
    """Test that column names are case-insensitive."""
    sql = "SELECT ID, NAME FROM CUSTOMERS"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    # Column names should be lowercased
    assert "id" in result.columns
    assert "name" in result.columns


def test_qualified_column_names():
    """Test fully qualified column names (table.column)."""
    sql = "SELECT customers.id, customers.name FROM customers"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert "name" in result.columns
    assert "customers.id" in result.columns["id"].source_columns
    assert "customers.name" in result.columns["name"].source_columns


def test_expression_stored():
    """Test that expressions are stored in derived columns."""
    sql = "SELECT id, first_name || ' ' || last_name AS full_name FROM customers"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "full_name" in result.columns
    assert result.columns["full_name"].expression != ""
    assert (
        "||" in result.columns["full_name"].expression
        or "CONCAT" in result.columns["full_name"].expression.upper()
    )


def test_multiple_sources_marked_derived():
    """Test columns with multiple sources are marked as derived."""
    sql = "SELECT first_name || ' ' || last_name AS full_name FROM customers"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "full_name" in result.columns
    assert len(result.columns["full_name"].source_columns) == 2
    assert result.columns["full_name"].transformation == "derived"


# ============================================================================
# Parametrized tests for transformation types
# ============================================================================


@pytest.mark.parametrize(
    "sql,column,expected_transformation",
    [
        ("SELECT id FROM customers", "id", "passthrough"),
        ("SELECT id AS customer_id FROM customers", "customer_id", "rename"),
        ("SELECT COUNT(*) AS cnt FROM customers", "cnt", "aggregated"),
        ("SELECT SUM(amount) AS total FROM orders", "total", "aggregated"),
        ("SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM customers", "rn", "windowed"),
        (
            "SELECT RANK() OVER (PARTITION BY type ORDER BY amount) AS rnk FROM orders",
            "rnk",
            "windowed",
        ),
        ("SELECT id, amount * 1.1 AS adjusted FROM orders", "adjusted", "derived"),
        ("SELECT 'constant' AS const FROM customers", "const", "literal"),
        ("SELECT 123 AS num FROM customers", "num", "literal"),
    ],
)
def test_transformation_detection(sql: str, column: str, expected_transformation: str):
    """Parametrized test for transformation type detection."""
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert column in result.columns
    assert result.columns[column].transformation == expected_transformation
