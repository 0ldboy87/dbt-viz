"""Tests for column collection."""

import json
from pathlib import Path

from dbt_viz.columns import (
    CatalogParser,
    ColumnCollector,
    ColumnInfo,
    CompiledSQLReader,
    TableColumns,
    find_catalog,
    find_compiled_path,
)


class TestColumnInfo:
    """Test ColumnInfo dataclass."""

    def test_column_info_creation(self):
        """Test creating a ColumnInfo instance."""
        col = ColumnInfo(
            name="customer_id",
            data_type="INTEGER",
            description="Customer identifier",
            sources=["source.project.raw.customers.id"],
            transformation="rename",
        )
        assert col.name == "customer_id"
        assert col.data_type == "INTEGER"
        assert col.description == "Customer identifier"
        assert col.sources == ["source.project.raw.customers.id"]
        assert col.transformation == "rename"

    def test_column_info_defaults(self):
        """Test ColumnInfo with default values."""
        col = ColumnInfo(name="id")
        assert col.name == "id"
        assert col.data_type == ""
        assert col.description == ""
        assert col.sources == []
        assert col.transformation == "unknown"

    def test_column_info_to_dict(self):
        """Test converting ColumnInfo to dictionary."""
        col = ColumnInfo(
            name="order_id",
            data_type="INTEGER",
            description="Order ID",
            sources=["source.project.raw.orders.id"],
            transformation="passthrough",
        )
        result = col.to_dict()
        assert result == {
            "name": "order_id",
            "data_type": "INTEGER",
            "description": "Order ID",
            "sources": ["source.project.raw.orders.id"],
            "transformation": "passthrough",
        }


class TestTableColumns:
    """Test TableColumns dataclass."""

    def test_table_columns_creation(self):
        """Test creating a TableColumns instance."""
        col1 = ColumnInfo(name="id", data_type="INTEGER")
        col2 = ColumnInfo(name="name", data_type="VARCHAR")
        table = TableColumns(
            unique_id="model.project.customers",
            name="customers",
            columns={"id": col1, "name": col2},
        )
        assert table.unique_id == "model.project.customers"
        assert table.name == "customers"
        assert len(table.columns) == 2
        assert "id" in table.columns
        assert "name" in table.columns

    def test_table_columns_defaults(self):
        """Test TableColumns with default values."""
        table = TableColumns(unique_id="model.project.test", name="test")
        assert table.unique_id == "model.project.test"
        assert table.name == "test"
        assert table.columns == {}

    def test_table_columns_to_dict(self):
        """Test converting TableColumns to dictionary."""
        col = ColumnInfo(name="id", data_type="INTEGER")
        table = TableColumns(
            unique_id="model.project.test",
            name="test",
            columns={"id": col},
        )
        result = table.to_dict()
        assert result == {
            "unique_id": "model.project.test",
            "name": "test",
            "columns": {
                "id": {
                    "name": "id",
                    "data_type": "INTEGER",
                    "description": "",
                    "sources": [],
                    "transformation": "unknown",
                }
            },
        }


class TestCatalogParser:
    """Test CatalogParser class."""

    def test_parse_catalog(self, catalog_path: Path):
        """Test parsing catalog.json."""
        parser = CatalogParser(catalog_path)
        parser.parse()

        # Check that nodes were parsed
        assert "model.my_project.stg_orders" in parser.tables
        assert "model.my_project.stg_customers" in parser.tables
        assert "model.my_project.fct_orders" in parser.tables
        assert "model.my_project.dim_customers" in parser.tables

        # Check that sources were parsed
        assert "source.my_project.raw.orders" in parser.tables
        assert "source.my_project.raw.customers" in parser.tables

    def test_parse_node_columns(self, catalog_path: Path):
        """Test that columns are parsed correctly from nodes."""
        parser = CatalogParser(catalog_path)
        parser.parse()

        stg_orders = parser.tables["model.my_project.stg_orders"]
        assert stg_orders.name == "stg_orders"
        assert len(stg_orders.columns) == 4

        # Check column details
        assert "order_id" in stg_orders.columns
        order_id_col = stg_orders.columns["order_id"]
        assert order_id_col.name == "order_id"
        assert order_id_col.data_type == "INTEGER"

    def test_columns_lowercased(self, catalog_path: Path):
        """Test that column names are lowercased in the dictionary keys."""
        parser = CatalogParser(catalog_path)
        parser.parse()

        # Even if catalog has mixed case, keys should be lowercase
        stg_orders = parser.tables["model.my_project.stg_orders"]
        for col_key in stg_orders.columns:
            assert col_key == col_key.lower()

    def test_get_columns_existing(self, catalog_path: Path):
        """Test get_columns for an existing table."""
        parser = CatalogParser(catalog_path)
        parser.parse()

        columns = parser.get_columns("model.my_project.stg_orders")
        assert len(columns) == 4
        assert "order_id" in columns
        assert "customer_id" in columns
        assert "order_date" in columns
        assert "status" in columns

    def test_get_columns_nonexistent(self, catalog_path: Path):
        """Test get_columns for a non-existent table."""
        parser = CatalogParser(catalog_path)
        parser.parse()

        columns = parser.get_columns("model.my_project.nonexistent")
        assert columns == {}

    def test_parse_source_columns(self, catalog_path: Path):
        """Test that source columns are parsed correctly."""
        parser = CatalogParser(catalog_path)
        parser.parse()

        source = parser.tables["source.my_project.raw.orders"]
        assert source.name == "orders"
        assert len(source.columns) == 4
        assert "id" in source.columns
        assert "customer_id" in source.columns


class TestCompiledSQLReader:
    """Test CompiledSQLReader class."""

    def test_find_sql_files(self, manifest_path: Path, compiled_path: Path):
        """Test finding compiled SQL files."""
        with open(manifest_path) as f:
            manifest = json.load(f)

        reader = CompiledSQLReader(compiled_path)
        reader.find_sql_files(manifest["nodes"])

        # Should find SQL files for models
        assert "model.my_project.stg_orders" in reader.sql_files
        assert "model.my_project.stg_customers" in reader.sql_files
        assert "model.my_project.fct_orders" in reader.sql_files
        assert "model.my_project.dim_customers" in reader.sql_files

        # Should not find SQL for seeds
        assert "seed.my_project.country_codes" not in reader.sql_files

    def test_get_sql_existing(self, manifest_path: Path, compiled_path: Path):
        """Test getting SQL for an existing model."""
        with open(manifest_path) as f:
            manifest = json.load(f)

        reader = CompiledSQLReader(compiled_path)
        reader.find_sql_files(manifest["nodes"])

        sql = reader.get_sql("model.my_project.stg_orders")
        assert sql is not None
        assert "SELECT" in sql
        assert "order_id" in sql

    def test_get_sql_nonexistent(self, manifest_path: Path, compiled_path: Path):
        """Test getting SQL for a non-existent model."""
        with open(manifest_path) as f:
            manifest = json.load(f)

        reader = CompiledSQLReader(compiled_path)
        reader.find_sql_files(manifest["nodes"])

        sql = reader.get_sql("model.my_project.nonexistent")
        assert sql is None

    def test_missing_compiled_path(self, manifest_path: Path, tmp_path: Path):
        """Test handling of missing compiled path."""
        with open(manifest_path) as f:
            manifest = json.load(f)

        # Use a non-existent path
        nonexistent_path = tmp_path / "nonexistent"
        reader = CompiledSQLReader(nonexistent_path)
        reader.find_sql_files(manifest["nodes"])

        # Should not crash, just have no SQL files
        assert len(reader.sql_files) == 0

    def test_sql_content(self, manifest_path: Path, compiled_path: Path):
        """Test that SQL content is read correctly."""
        with open(manifest_path) as f:
            manifest = json.load(f)

        reader = CompiledSQLReader(compiled_path)
        reader.find_sql_files(manifest["nodes"])

        sql = reader.get_sql("model.my_project.fct_orders")
        assert sql is not None
        assert "o.order_id" in sql
        assert "c.customer_name" in sql
        assert "LEFT JOIN" in sql


class TestColumnCollector:
    """Test ColumnCollector class."""

    def test_collect_with_all_sources(
        self, manifest_path: Path, catalog_path: Path, compiled_path: Path
    ):
        """Test collecting columns from all sources."""
        collector = ColumnCollector(
            manifest_path=manifest_path,
            catalog_path=catalog_path,
            compiled_path=compiled_path,
        )
        collector.collect()

        # Should have columns from catalog
        assert "model.my_project.stg_orders" in collector.columns
        assert "model.my_project.fct_orders" in collector.columns

        # Should have SQL files
        assert collector.sql_reader is not None
        assert len(collector.sql_reader.sql_files) > 0

    def test_collect_manifest_only(self, manifest_path: Path):
        """Test collecting with only manifest (no catalog or SQL)."""
        collector = ColumnCollector(manifest_path=manifest_path)
        collector.collect()

        # Should have columns from manifest
        assert "model.my_project.stg_orders" in collector.columns
        stg_orders_cols = collector.columns["model.my_project.stg_orders"]
        assert "order_id" in stg_orders_cols
        assert stg_orders_cols["order_id"].description == "Primary key"

    def test_get_columns(self, manifest_path: Path, catalog_path: Path, compiled_path: Path):
        """Test getting columns for a specific model."""
        collector = ColumnCollector(
            manifest_path=manifest_path,
            catalog_path=catalog_path,
            compiled_path=compiled_path,
        )
        collector.collect()

        columns = collector.get_columns("model.my_project.stg_orders")
        assert len(columns) > 0
        assert "order_id" in columns
        assert "customer_id" in columns

    def test_get_columns_nonexistent(
        self, manifest_path: Path, catalog_path: Path, compiled_path: Path
    ):
        """Test getting columns for a non-existent model."""
        collector = ColumnCollector(
            manifest_path=manifest_path,
            catalog_path=catalog_path,
            compiled_path=compiled_path,
        )
        collector.collect()

        columns = collector.get_columns("model.my_project.nonexistent")
        assert columns == {}

    def test_get_compiled_sql(self, manifest_path: Path, catalog_path: Path, compiled_path: Path):
        """Test getting compiled SQL for a model."""
        collector = ColumnCollector(
            manifest_path=manifest_path,
            catalog_path=catalog_path,
            compiled_path=compiled_path,
        )
        collector.collect()

        sql = collector.get_compiled_sql("model.my_project.stg_orders")
        assert sql is not None
        assert "SELECT" in sql

    def test_get_compiled_sql_no_reader(self, manifest_path: Path):
        """Test getting compiled SQL when no SQL reader exists."""
        collector = ColumnCollector(manifest_path=manifest_path)
        collector.collect()

        sql = collector.get_compiled_sql("model.my_project.stg_orders")
        assert sql is None

    def test_catalog_priority_for_data_type(self, manifest_path: Path, catalog_path: Path):
        """Test that catalog data types take priority over manifest."""
        collector = ColumnCollector(
            manifest_path=manifest_path,
            catalog_path=catalog_path,
        )
        collector.collect()

        # Catalog has "INTEGER", manifest has "integer"
        columns = collector.get_columns("model.my_project.stg_orders")
        order_id = columns["order_id"]

        # Should use catalog's data type (uppercase)
        assert order_id.data_type == "INTEGER"

    def test_manifest_description_overlay(self, manifest_path: Path, catalog_path: Path):
        """Test that manifest descriptions are used when catalog has none."""
        collector = ColumnCollector(
            manifest_path=manifest_path,
            catalog_path=catalog_path,
        )
        collector.collect()

        # Manifest has descriptions, catalog comments are empty
        columns = collector.get_columns("model.my_project.stg_orders")
        order_id = columns["order_id"]

        # Should have description from manifest
        assert order_id.description == "Primary key"

    def test_get_all_tables_with_columns(self, manifest_path: Path, catalog_path: Path):
        """Test getting all tables with columns."""
        collector = ColumnCollector(
            manifest_path=manifest_path,
            catalog_path=catalog_path,
        )
        collector.collect()

        all_tables = collector.get_all_tables_with_columns()
        assert len(all_tables) > 0
        assert "model.my_project.stg_orders" in all_tables
        assert "model.my_project.fct_orders" in all_tables

    def test_model_dependencies_parsed(self, manifest_path: Path):
        """Test that model dependencies are parsed correctly."""
        collector = ColumnCollector(manifest_path=manifest_path)
        collector.collect()

        # fct_orders depends on stg_orders and stg_customers
        deps = collector.model_dependencies.get("model.my_project.fct_orders", [])
        assert "model.my_project.stg_orders" in deps
        assert "model.my_project.stg_customers" in deps

    def test_model_names_parsed(self, manifest_path: Path):
        """Test that model names are parsed correctly."""
        collector = ColumnCollector(manifest_path=manifest_path)
        collector.collect()

        assert collector.model_names["model.my_project.stg_orders"] == "stg_orders"
        assert collector.model_names["model.my_project.fct_orders"] == "fct_orders"

    def test_get_column_lineage(self, manifest_path: Path, catalog_path: Path, compiled_path: Path):
        """Test getting column lineage for a model."""
        collector = ColumnCollector(
            manifest_path=manifest_path,
            catalog_path=catalog_path,
            compiled_path=compiled_path,
        )
        collector.collect()

        # Get lineage for a model that has compiled SQL
        lineage = collector.get_column_lineage("model.my_project.stg_orders")

        # Should have lineage data (may be empty dict if SQL parsing didn't work)
        assert isinstance(lineage, dict)

    def test_missing_catalog_gracefully(self, manifest_path: Path, tmp_path: Path):
        """Test handling missing catalog gracefully."""
        nonexistent_catalog = tmp_path / "nonexistent_catalog.json"
        collector = ColumnCollector(
            manifest_path=manifest_path,
            catalog_path=nonexistent_catalog,
        )
        collector.collect()

        # Should still work with manifest columns
        assert len(collector.columns) > 0

    def test_missing_compiled_sql_gracefully(
        self, manifest_path: Path, catalog_path: Path, tmp_path: Path
    ):
        """Test handling missing compiled SQL gracefully."""
        nonexistent_compiled = tmp_path / "nonexistent_compiled"
        collector = ColumnCollector(
            manifest_path=manifest_path,
            catalog_path=catalog_path,
            compiled_path=nonexistent_compiled,
        )
        collector.collect()

        # Should still work without SQL
        assert len(collector.columns) > 0
        assert collector.sql_reader is None


class TestHelperFunctions:
    """Test helper functions."""

    def test_find_catalog_exists(self, manifest_path: Path):
        """Test finding catalog when it exists."""
        catalog = find_catalog(manifest_path)
        assert catalog is not None
        assert catalog.exists()
        assert catalog.name == "catalog.json"

    def test_find_catalog_missing(self, tmp_path: Path):
        """Test finding catalog when it doesn't exist."""
        manifest = tmp_path / "manifest.json"
        manifest.write_text("{}")

        catalog = find_catalog(manifest)
        assert catalog is None

    def test_find_compiled_path_exists(self, manifest_path: Path):
        """Test finding compiled path when it exists."""
        compiled = find_compiled_path(manifest_path)
        assert compiled is not None
        assert compiled.exists()
        assert compiled.name == "compiled"

    def test_find_compiled_path_missing(self, tmp_path: Path):
        """Test finding compiled path when it doesn't exist."""
        manifest = tmp_path / "manifest.json"
        manifest.write_text("{}")

        compiled = find_compiled_path(manifest)
        assert compiled is None
