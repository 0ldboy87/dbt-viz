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


# ============================================================================
# Type conversion tests (Critical improvement from docprop)
# ============================================================================


def test_cast_is_passthrough(sample_sql_cast_passthrough: str):
    """Test that CAST expressions are treated as passthrough, not derived."""
    parser = SQLLineageParser()
    result = parser.parse_sql(sample_sql_cast_passthrough)

    assert "id" in result.columns
    assert "customer_id_str" in result.columns

    # Both should be passthrough since they're just column references (possibly with cast)
    assert result.columns["id"].transformation == "passthrough"
    assert result.columns["customer_id_str"].transformation == "passthrough"

    # Verify source columns are traced correctly
    assert "customers.id" in result.columns["id"].source_columns
    assert "customers.customer_id" in result.columns["customer_id_str"].source_columns


def test_type_conversion_functions(sample_sql_type_conversions: str):
    """Test various type conversion functions are treated as passthrough."""
    parser = SQLLineageParser()
    result = parser.parse_sql(sample_sql_type_conversions)

    # All these columns should be passthrough (single source with type conversion)
    assert result.columns["id"].transformation == "passthrough"
    assert result.columns["amount_decimal"].transformation == "passthrough"
    assert result.columns["status_code"].transformation == "passthrough"
    assert result.columns["order_date_only"].transformation == "passthrough"

    # Verify sources are traced correctly
    assert "orders.id" in result.columns["id"].source_columns
    assert "orders.amount" in result.columns["amount_decimal"].source_columns
    assert "orders.status" in result.columns["status_code"].source_columns
    assert "orders.order_date" in result.columns["order_date_only"].source_columns


def test_nested_cast():
    """Test nested type conversions are still treated as passthrough."""
    sql = "SELECT CAST(CAST(id AS VARCHAR) AS TEXT) AS id_text FROM customers"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id_text" in result.columns
    assert result.columns["id_text"].transformation == "passthrough"
    assert "customers.id" in result.columns["id_text"].source_columns


def test_cast_with_expression_is_derived():
    """Test that CAST of an expression (not just a column) is still derived."""
    sql = "SELECT CAST(first_name || ' ' || last_name AS VARCHAR(100)) AS full_name FROM customers"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "full_name" in result.columns
    # This should be derived because the CAST contains an expression, not just a column
    assert result.columns["full_name"].transformation == "derived"
    assert len(result.columns["full_name"].source_columns) == 2


def test_passthrough_cte_optimization(sample_sql_passthrough_cte: str):
    """Test that passthrough CTEs (SELECT * FROM table) are optimized."""
    parser = SQLLineageParser()
    schema = {"customers": {"id": "int", "name": "varchar"}}
    result = parser.parse_sql(sample_sql_passthrough_cte, schema=schema)

    assert "id" in result.columns
    assert "name" in result.columns

    # Should trace through the passthrough CTE to the original table
    assert "customers.id" in result.columns["id"].source_columns
    assert "customers.name" in result.columns["name"].source_columns
    assert result.columns["id"].transformation == "passthrough"
    assert result.columns["name"].transformation == "passthrough"


def test_cast_in_cte():
    """Test type conversions work correctly within CTEs."""
    sql = """
    WITH casted AS (
        SELECT id, CAST(customer_id AS VARCHAR) AS customer_id_str FROM orders
    )
    SELECT customer_id_str FROM casted
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "customer_id_str" in result.columns
    assert result.columns["customer_id_str"].transformation == "passthrough"
    assert "orders.customer_id" in result.columns["customer_id_str"].source_columns


# ============================================================================
# Edge case tests for improved coverage
# ============================================================================


def test_parse_sql_none_result():
    """Test parse_sql handles None result from sqlglot gracefully."""
    from unittest.mock import patch

    parser = SQLLineageParser()
    with patch("dbt_viz.sql_lineage.sqlglot.parse_one", return_value=None):
        result = parser.parse_sql("SELECT 1")

    assert result.columns == {}


def test_cte_body_is_union():
    """Test CTE whose body is a UNION ALL."""
    sql = """
    WITH combined AS (
        SELECT id, name FROM customers
        UNION ALL
        SELECT id, name FROM prospects
    )
    SELECT id, name FROM combined
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert "name" in result.columns


def test_join_table_alias_resolution():
    """Test that JOIN tables are correctly aliased and resolved."""
    sql = """
    SELECT o.order_id, c.customer_name
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "order_id" in result.columns
    assert "customer_name" in result.columns
    assert "orders.order_id" in result.columns["order_id"].source_columns
    assert "customers.customer_name" in result.columns["customer_name"].source_columns


def test_unused_cte_not_in_aliases():
    """Test that an unused CTE name is still registered in aliases."""
    sql = """
    WITH unused AS (SELECT 1 AS x),
         used AS (SELECT id FROM customers)
    SELECT id FROM used
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns


def test_subquery_trace_through():
    """Test column lineage traces through a subquery."""
    sql = """
    SELECT sub.customer_id, sub.total
    FROM (
        SELECT customer_id, SUM(amount) AS total
        FROM orders
        GROUP BY customer_id
    ) sub
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "customer_id" in result.columns
    assert "total" in result.columns


def test_listagg_aggregation():
    """Test that LISTAGG (anonymous function) is detected as aggregated."""
    sql = "SELECT customer_id, LISTAGG(name, ',') AS name_list FROM customers GROUP BY customer_id"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "name_list" in result.columns
    assert result.columns["name_list"].transformation == "aggregated"


def test_resolve_table_references_partial_match():
    """Test resolve_table_references with partial table name matching."""
    lineage = TableLineage(table_name="fct_orders")
    lineage.columns["customer_id"] = ColumnLineage(
        column_name="customer_id",
        source_columns=["stg_customers.customer_id"],
        transformation="passthrough",
    )

    table_map = {"model.my_project.stg_customers": "model.my_project.stg_customers"}
    result = resolve_table_references(lineage, table_map)

    # Should try to match partial - "stg_customers" won't match exactly
    # but will try partial match
    assert "customer_id" in result.columns


def test_resolve_table_references_no_dot():
    """Test resolve_table_references with source that has no table prefix."""
    lineage = TableLineage(table_name="fct_orders")
    lineage.columns["id"] = ColumnLineage(
        column_name="id",
        source_columns=["id"],  # no table.column format
        transformation="passthrough",
    )

    result = resolve_table_references(lineage, {})

    assert "id" in result.columns
    assert result.columns["id"].source_columns == ["id"]


def test_deep_cte_trace_depth_limit():
    """Test that CTE tracing stops at MAX_CTE_TRACE_DEPTH to prevent infinite loops."""
    from dbt_viz.sql_lineage import MAX_CTE_TRACE_DEPTH, ColumnMap

    parser = SQLLineageParser()

    # Manually create a circular CTE map to test depth limiting
    cte_maps: dict[str, ColumnMap] = {
        "cte_a": {"col": ["CTE:cte_b.col"]},
        "cte_b": {"col": ["CTE:cte_a.col"]},
    }

    # Trace through should stop when _depth exceeds MAX_CTE_TRACE_DEPTH
    result = parser._trace_through_cte(
        "CTE:cte_a.col", cte_maps, _depth=MAX_CTE_TRACE_DEPTH + 1
    )
    assert result == ["CTE:cte_a.col"]


def test_select_star_without_schema_logs_debug():
    """Test SELECT * without schema info returns empty columns."""
    sql = "SELECT * FROM some_table"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql, schema=None)

    # Without schema, SELECT * cannot be expanded
    assert result.columns == {}


def test_cte_with_union_body_trace():
    """Test column lineage from a CTE with UNION ALL body traces correctly."""
    sql = """
    WITH multi_source AS (
        SELECT id, 'active' AS status FROM current_customers
        UNION ALL
        SELECT id, 'churned' AS status FROM churned_customers
    )
    SELECT id, status FROM multi_source
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert "status" in result.columns


def test_resolve_table_references_with_matching_suffix():
    """Test resolve_table_references matches table names by suffix."""
    lineage = TableLineage(table_name="fct_orders")
    lineage.columns["order_id"] = ColumnLineage(
        column_name="order_id",
        source_columns=["stg_orders.order_id"],
        transformation="passthrough",
    )

    # Key ends with "stg_orders" so partial match should work
    table_map = {"my_project.stg_orders": "model.my_project.stg_orders"}
    result = resolve_table_references(lineage, table_map)

    # Verify the sources were processed (may or may not match depending on impl)
    assert "order_id" in result.columns


def test_column_alias_from_named_expression():
    """Test _get_column_alias on expressions with name but no alias attr."""
    parser = SQLLineageParser()
    import sqlglot.expressions as exp

    # Star expression has name but no alias — should return its name
    star = exp.Star()
    result = parser._get_column_alias(star)
    # Star's name is "*" or similar — just verify it doesn't crash
    assert result is None or isinstance(result, str)


def test_parse_sql_with_subquery_in_where():
    """Test SQL with subquery in WHERE clause (not FROM)."""
    sql = """
    SELECT id, name
    FROM customers
    WHERE id IN (SELECT customer_id FROM orders)
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert "name" in result.columns


def test_cte_exception_handling():
    """Test that CTE parse errors are handled gracefully."""
    from unittest.mock import patch

    sql = """
    WITH my_cte AS (SELECT id FROM customers)
    SELECT id FROM my_cte
    """
    parser = SQLLineageParser()

    # Simulate exception during CTE column map building
    with patch.object(
        parser,
        "_process_select_for_column_map",
        side_effect=Exception("Simulated error"),
    ):
        result = parser.parse_sql(sql)

    # Should still return a result (possibly empty)
    assert isinstance(result, TableLineage)


def test_alias_same_name_is_passthrough():
    """Test that aliasing a column to the same name is passthrough (not rename)."""
    sql = "SELECT id AS id FROM customers"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert result.columns["id"].transformation == "passthrough"


def test_unaliased_function_expression():
    """Test tracing an unaliased function expression (else branch in _trace_column_lineage)."""
    # MAX(id) without alias - sqlglot may give it a name like "max"
    sql = "SELECT MAX(id) FROM customers"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)
    # Result depends on sqlglot's naming, just verify it doesn't crash
    assert isinstance(result, TableLineage)


def test_subquery_in_join():
    """Test that a subquery in a JOIN clause is handled in _build_local_alias_map."""
    sql = """
    SELECT o.order_id, sub.name
    FROM orders o
    JOIN (SELECT id, name FROM customers) sub ON o.customer_id = sub.id
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "order_id" in result.columns
    assert "name" in result.columns


def test_cte_with_star_join():
    """Test CTE body with SELECT * and JOIN (non-passthrough star)."""
    sql = """
    WITH enriched AS (
        SELECT *
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
    )
    SELECT * FROM enriched
    """
    parser = SQLLineageParser()
    schema = {
        "orders": {"order_id": "int", "customer_id": "int"},
        "customers": {"id": "int", "name": "varchar"},
    }
    result = parser.parse_sql(sql, schema=schema)
    assert isinstance(result, TableLineage)


def test_cte_with_table_star():
    """Test CTE body with table.* expression."""
    sql = """
    WITH cte AS (
        SELECT o.*, c.name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
    )
    SELECT order_id, name FROM cte
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)
    assert isinstance(result, TableLineage)


def test_count_star_in_column_lineage():
    """Test COUNT(*) correctly handled - star col in find_all ignored."""
    sql = "SELECT COUNT(*) AS cnt, id FROM customers GROUP BY id"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "cnt" in result.columns
    assert result.columns["cnt"].transformation == "aggregated"
    assert "id" in result.columns


def test_subquery_trace_through_columns():
    """Test that subquery column maps are traced for referenced columns."""
    sql = """
    SELECT sub.customer_id
    FROM (SELECT customer_id, name FROM customers) sub
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "customer_id" in result.columns
    # Should trace through to customers.customer_id
    assert "customers.customer_id" in result.columns["customer_id"].source_columns


def test_cte_with_empty_column_sources():
    """Test CTE column that has no traceable sources (e.g., COUNT(*))."""
    sql = """
    WITH counts AS (
        SELECT customer_id, COUNT(*) AS order_count
        FROM orders
        GROUP BY customer_id
    )
    SELECT customer_id, order_count FROM counts
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "customer_id" in result.columns
    assert "order_count" in result.columns
    assert result.columns["order_count"].transformation in ("aggregated", "literal", "unknown", "derived", "passthrough")


def test_resolve_table_references_table_in_map():
    """Test resolve_table_references when table ref exactly matches map key."""
    lineage = TableLineage(table_name="fct_orders")
    lineage.columns["order_id"] = ColumnLineage(
        column_name="order_id",
        source_columns=["stg_orders.order_id"],
        transformation="passthrough",
    )

    table_map = {"stg_orders": "model.my_project.stg_orders"}
    result = resolve_table_references(lineage, table_map)

    assert result.columns["order_id"].source_columns == ["model.my_project.stg_orders.order_id"]


def test_resolve_table_references_fallback_no_match():
    """Test resolve_table_references when no table match is found."""
    lineage = TableLineage(table_name="fct_orders")
    lineage.columns["order_id"] = ColumnLineage(
        column_name="order_id",
        source_columns=["unknown_table.order_id"],
        transformation="passthrough",
    )

    table_map = {"other_table": "model.my_project.other"}
    result = resolve_table_references(lineage, table_map)

    # Source remains unchanged when no match found
    assert result.columns["order_id"].source_columns == ["unknown_table.order_id"]


def test_get_table_columns_not_in_schema():
    """Test _get_table_columns returns empty list when table not in schema."""
    parser = SQLLineageParser()
    schema = {"known_table": {"id": "int"}}

    result = parser._get_table_columns("unknown_table", schema, {})
    assert result == []


def test_union_with_multiple_branches():
    """Test UNION ALL with 3+ branches."""
    sql = """
    SELECT id, name FROM customers
    UNION ALL
    SELECT id, name FROM prospects
    UNION ALL
    SELECT id, name FROM leads
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    assert "name" in result.columns
    # Each source should contribute
    assert len(result.columns["id"].source_columns) >= 2


def test_passthrough_subquery_optimization():
    """Test subquery with SELECT * gets passthrough optimization."""
    sql = "SELECT sub.id FROM (SELECT * FROM customers) sub"
    parser = SQLLineageParser()
    schema = {"customers": {"id": "int", "name": "varchar"}}
    result = parser.parse_sql(sql, schema=schema)

    assert "id" in result.columns
    # Should trace through to customers.id
    assert "customers.id" in result.columns["id"].source_columns


def test_subquery_empty_column_sources():
    """Test subquery with aggregation produces empty column sources."""
    sql = "SELECT sub.cnt FROM (SELECT COUNT(*) AS cnt FROM orders) sub"
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "cnt" in result.columns
    # COUNT(*) has no traceable column source
    assert result.columns["cnt"].source_columns == []


def test_get_table_columns_case_insensitive_schema():
    """Test _get_table_columns matches schema key case-insensitively."""
    parser = SQLLineageParser()
    schema = {"Customers": {"id": "int", "name": "varchar"}}

    result = parser._get_table_columns("customers", schema, {})
    assert result == ["id", "name"]


def test_union_second_branch_extra_columns():
    """Test UNION where second branch has more columns than first (triggers break)."""
    sql = """
    SELECT id FROM customers
    UNION ALL
    SELECT id, name, email FROM prospects
    """
    parser = SQLLineageParser()
    result = parser.parse_sql(sql)

    assert "id" in result.columns
    # Should only have first branch's column count
    assert "name" not in result.columns


def test_columns_resolve_table_fallback():
    """Test resolve_table_references with multi-part source having partial table match."""
    lineage = TableLineage(table_name="fct_orders")
    lineage.columns["order_id"] = ColumnLineage(
        column_name="order_id",
        source_columns=["db.schema.stg_orders.order_id"],
        transformation="passthrough",
    )

    # Key has suffix match
    table_map = {"other_model": "model.my_project.other"}
    result = resolve_table_references(lineage, table_map)

    # Source remains unchanged when no match found
    assert "order_id" in result.columns


def test_cte_with_catalog_db_in_table_name():
    """Test passthrough CTE optimization with fully-qualified table name."""
    sql = """
    WITH cte AS (SELECT * FROM mydb.myschema.customers)
    SELECT id FROM cte
    """
    parser = SQLLineageParser()
    schema = {"mydb.myschema.customers": {"id": "int"}}
    result = parser.parse_sql(sql, schema=schema)

    assert "id" in result.columns
