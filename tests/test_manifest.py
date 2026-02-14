"""Tests for manifest parsing."""

import json
from pathlib import Path

import pytest

from dbt_viz.manifest import ManifestParser, ModelInfo, find_manifest


class TestModelInfo:
    """Tests for ModelInfo dataclass."""

    def test_to_dict(self) -> None:
        """Test ModelInfo.to_dict() serialization."""
        model = ModelInfo(
            unique_id="model.my_project.test_model",
            name="test_model",
            resource_type="model",
            description="Test description",
            schema_name="staging",
            database="analytics",
            materialized="view",
            columns={
                "col1": {
                    "name": "col1",
                    "description": "Column 1",
                    "data_type": "integer",
                }
            },
            tags=["test", "staging"],
            file_path="models/test_model.sql",
            raw_sql="SELECT * FROM source",
            compiled_sql="SELECT * FROM production.source",
        )

        result = model.to_dict()

        assert result["unique_id"] == "model.my_project.test_model"
        assert result["name"] == "test_model"
        assert result["resource_type"] == "model"
        assert result["description"] == "Test description"
        assert result["schema"] == "staging"
        assert result["database"] == "analytics"
        assert result["materialized"] == "view"
        assert result["columns"] == {
            "col1": {
                "name": "col1",
                "description": "Column 1",
                "data_type": "integer",
            }
        }
        assert result["tags"] == ["test", "staging"]
        assert result["file_path"] == "models/test_model.sql"
        assert result["raw_sql"] == "SELECT * FROM source"
        assert result["compiled_sql"] == "SELECT * FROM production.source"

    def test_to_dict_defaults(self) -> None:
        """Test ModelInfo.to_dict() with default values."""
        model = ModelInfo(
            unique_id="model.test",
            name="test",
            resource_type="model",
        )

        result = model.to_dict()

        assert result["description"] == ""
        assert result["schema"] == ""
        assert result["database"] == ""
        assert result["materialized"] == ""
        assert result["columns"] == {}
        assert result["tags"] == []
        assert result["file_path"] == ""
        assert result["raw_sql"] == ""
        assert result["compiled_sql"] == ""


class TestManifestParser:
    """Tests for ManifestParser class."""

    def test_parse_nodes(self, manifest_parser: ManifestParser) -> None:
        """Test that parse() correctly parses model nodes."""
        # Should have 4 models + 1 seed = 5 nodes from nodes section
        model_nodes = [n for n in manifest_parser.nodes.values() if n.resource_type == "model"]
        seed_nodes = [n for n in manifest_parser.nodes.values() if n.resource_type == "seed"]

        assert len(model_nodes) == 4
        assert len(seed_nodes) == 1

    def test_parse_sources(self, manifest_parser: ManifestParser) -> None:
        """Test that parse() correctly parses source nodes."""
        source_nodes = [n for n in manifest_parser.nodes.values() if n.resource_type == "source"]

        assert len(source_nodes) == 2

    def test_parse_filters_unsupported_types(self, tmp_path: Path) -> None:
        """Test that parse() filters out unsupported resource types."""
        manifest_data = {
            "nodes": {
                "model.test.valid": {
                    "name": "valid",
                    "resource_type": "model",
                    "config": {},
                    "depends_on": {"nodes": []},
                },
                "test.test.invalid": {
                    "name": "invalid",
                    "resource_type": "test",
                    "config": {},
                    "depends_on": {"nodes": []},
                },
                "macro.test.also_invalid": {
                    "name": "also_invalid",
                    "resource_type": "macro",
                    "config": {},
                    "depends_on": {"nodes": []},
                },
            },
            "sources": {},
        }

        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data))

        parser = ManifestParser(manifest_path)
        parser.parse()

        # Should only have the model, not test or macro
        assert len(parser.nodes) == 1
        assert "model.test.valid" in parser.nodes
        assert "test.test.invalid" not in parser.nodes
        assert "macro.test.also_invalid" not in parser.nodes

    def test_parse_builds_edges(self, manifest_parser: ManifestParser) -> None:
        """Test that parse() builds edges from depends_on."""
        # fct_orders depends on stg_orders and stg_customers
        # Should have edges: stg_orders -> fct_orders, stg_customers -> fct_orders
        edges_to_fct_orders = [
            (src, tgt) for src, tgt in manifest_parser.edges if tgt == "model.my_project.fct_orders"
        ]

        assert len(edges_to_fct_orders) == 2
        assert ("model.my_project.stg_orders", "model.my_project.fct_orders") in edges_to_fct_orders
        assert (
            "model.my_project.stg_customers",
            "model.my_project.fct_orders",
        ) in edges_to_fct_orders

    def test_parse_populates_upstream_map(self, manifest_parser: ManifestParser) -> None:
        """Test that parse() populates upstream dependency map."""
        # fct_orders should have stg_orders and stg_customers as upstream
        upstream = manifest_parser._upstream["model.my_project.fct_orders"]

        assert len(upstream) == 2
        assert "model.my_project.stg_orders" in upstream
        assert "model.my_project.stg_customers" in upstream

    def test_parse_populates_downstream_map(self, manifest_parser: ManifestParser) -> None:
        """Test that parse() populates downstream dependent map."""
        # stg_customers should have fct_orders and dim_customers as downstream
        downstream = manifest_parser._downstream["model.my_project.stg_customers"]

        assert len(downstream) == 2
        assert "model.my_project.fct_orders" in downstream
        assert "model.my_project.dim_customers" in downstream

    def test_parse_node_attributes(self, manifest_parser: ManifestParser) -> None:
        """Test that node attributes are correctly parsed."""
        model = manifest_parser.nodes["model.my_project.stg_orders"]

        assert model.unique_id == "model.my_project.stg_orders"
        assert model.name == "stg_orders"
        assert model.resource_type == "model"
        assert model.description == "Staging table for raw orders data"
        assert model.schema_name == "staging"
        assert model.database == "analytics"
        assert model.materialized == "view"
        assert model.tags == ["staging", "orders"]
        assert model.file_path == "models/staging/stg_orders.sql"
        assert "SELECT" in model.raw_sql

    def test_parse_node_columns(self, manifest_parser: ManifestParser) -> None:
        """Test that node columns are correctly parsed."""
        model = manifest_parser.nodes["model.my_project.stg_orders"]

        assert "order_id" in model.columns
        assert model.columns["order_id"]["name"] == "order_id"
        assert model.columns["order_id"]["description"] == "Primary key"
        assert model.columns["order_id"]["data_type"] == "integer"

    def test_parse_source_attributes(self, manifest_parser: ManifestParser) -> None:
        """Test that source attributes are correctly parsed."""
        source = manifest_parser.nodes["source.my_project.raw.orders"]

        assert source.unique_id == "source.my_project.raw.orders"
        assert source.name == "orders"
        assert source.resource_type == "source"
        assert source.description == "Raw orders from the production database"
        assert source.schema_name == "raw"
        assert source.database == "production"
        assert source.tags == ["raw"]


class TestGetUpstream:
    """Tests for get_upstream() method."""

    def test_get_upstream_no_depth_limit(self, manifest_parser: ManifestParser) -> None:
        """Test get_upstream() with no depth limit (BFS traversal)."""
        # fct_orders -> stg_orders -> source.orders
        # fct_orders -> stg_customers -> source.customers
        upstream = manifest_parser.get_upstream("model.my_project.fct_orders")

        assert len(upstream) == 4
        assert "model.my_project.stg_orders" in upstream
        assert "model.my_project.stg_customers" in upstream
        assert "source.my_project.raw.orders" in upstream
        assert "source.my_project.raw.customers" in upstream

    def test_get_upstream_depth_1(self, manifest_parser: ManifestParser) -> None:
        """Test get_upstream() with depth=1."""
        upstream = manifest_parser.get_upstream("model.my_project.fct_orders", depth=1)

        # Should only get immediate upstream (depth 1)
        assert len(upstream) == 2
        assert "model.my_project.stg_orders" in upstream
        assert "model.my_project.stg_customers" in upstream
        # Should NOT include sources (depth 2)
        assert "source.my_project.raw.orders" not in upstream
        assert "source.my_project.raw.customers" not in upstream

    def test_get_upstream_depth_2(self, manifest_parser: ManifestParser) -> None:
        """Test get_upstream() with depth=2."""
        upstream = manifest_parser.get_upstream("model.my_project.fct_orders", depth=2)

        # Should get all upstream up to depth 2
        assert len(upstream) == 4
        assert "model.my_project.stg_orders" in upstream
        assert "model.my_project.stg_customers" in upstream
        assert "source.my_project.raw.orders" in upstream
        assert "source.my_project.raw.customers" in upstream

    def test_get_upstream_nonexistent_node(self, manifest_parser: ManifestParser) -> None:
        """Test get_upstream() with non-existent node returns empty set."""
        upstream = manifest_parser.get_upstream("model.nonexistent")

        assert upstream == set()


class TestGetDownstream:
    """Tests for get_downstream() method."""

    def test_get_downstream_no_depth_limit(self, manifest_parser: ManifestParser) -> None:
        """Test get_downstream() with no depth limit (BFS traversal)."""
        # stg_customers -> fct_orders
        # stg_customers -> dim_customers
        downstream = manifest_parser.get_downstream("model.my_project.stg_customers")

        assert len(downstream) == 2
        assert "model.my_project.fct_orders" in downstream
        assert "model.my_project.dim_customers" in downstream

    def test_get_downstream_depth_1(self, manifest_parser: ManifestParser) -> None:
        """Test get_downstream() with depth=1."""
        downstream = manifest_parser.get_downstream("model.my_project.stg_customers", depth=1)

        assert len(downstream) == 2
        assert "model.my_project.fct_orders" in downstream
        assert "model.my_project.dim_customers" in downstream

    def test_get_downstream_depth_2(self, manifest_parser: ManifestParser) -> None:
        """Test get_downstream() with depth=2."""
        # Source has downstream at multiple levels
        downstream = manifest_parser.get_downstream("source.my_project.raw.orders", depth=2)

        assert len(downstream) == 2
        assert "model.my_project.stg_orders" in downstream
        assert "model.my_project.fct_orders" in downstream

    def test_get_downstream_nonexistent_node(self, manifest_parser: ManifestParser) -> None:
        """Test get_downstream() with non-existent node returns empty set."""
        downstream = manifest_parser.get_downstream("model.nonexistent")

        assert downstream == set()


class TestGetSubgraph:
    """Tests for get_subgraph() method."""

    def test_get_subgraph_no_center_returns_entire_graph(
        self, manifest_parser: ManifestParser
    ) -> None:
        """Test get_subgraph() with no center node returns entire graph."""
        nodes, edges = manifest_parser.get_subgraph()

        # Should have all nodes: 4 models + 1 seed + 2 sources = 7
        assert len(nodes) == 7
        # Should have all edges
        assert len(edges) > 0

    def test_get_subgraph_center_by_unique_id(self, manifest_parser: ManifestParser) -> None:
        """Test get_subgraph() with center node by unique_id."""
        nodes, edges = manifest_parser.get_subgraph(center_node="model.my_project.fct_orders")

        # Should include fct_orders + all upstream + all downstream
        node_ids = {n["unique_id"] for n in nodes}
        assert "model.my_project.fct_orders" in node_ids
        assert "model.my_project.stg_orders" in node_ids
        assert "model.my_project.stg_customers" in node_ids
        assert "source.my_project.raw.orders" in node_ids
        assert "source.my_project.raw.customers" in node_ids

    def test_get_subgraph_center_by_name(self, manifest_parser: ManifestParser) -> None:
        """Test get_subgraph() with center node by name (resolves to unique_id)."""
        nodes, edges = manifest_parser.get_subgraph(center_node="fct_orders")

        # Should resolve name to unique_id and return subgraph
        node_ids = {n["unique_id"] for n in nodes}
        assert "model.my_project.fct_orders" in node_ids

    def test_get_subgraph_nonexistent_center_raises_error(
        self, manifest_parser: ManifestParser
    ) -> None:
        """Test get_subgraph() with non-existent center raises ValueError."""
        with pytest.raises(ValueError, match="Model 'nonexistent' not found"):
            manifest_parser.get_subgraph(center_node="nonexistent")

    def test_get_subgraph_with_upstream_depth(self, manifest_parser: ManifestParser) -> None:
        """Test get_subgraph() with upstream depth limit."""
        nodes, edges = manifest_parser.get_subgraph(
            center_node="model.my_project.fct_orders", upstream_depth=1
        )

        node_ids = {n["unique_id"] for n in nodes}
        # Should include center + immediate upstream only
        assert "model.my_project.fct_orders" in node_ids
        assert "model.my_project.stg_orders" in node_ids
        assert "model.my_project.stg_customers" in node_ids
        # Should NOT include sources (depth 2)
        assert "source.my_project.raw.orders" not in node_ids
        assert "source.my_project.raw.customers" not in node_ids

    def test_get_subgraph_with_downstream_depth(self, manifest_parser: ManifestParser) -> None:
        """Test get_subgraph() with downstream depth limit."""
        nodes, edges = manifest_parser.get_subgraph(
            center_node="source.my_project.raw.orders", downstream_depth=1
        )

        node_ids = {n["unique_id"] for n in nodes}
        # Should include center + immediate downstream only
        assert "source.my_project.raw.orders" in node_ids
        assert "model.my_project.stg_orders" in node_ids
        # Should NOT include fct_orders (depth 2)
        assert "model.my_project.fct_orders" not in node_ids

    def test_get_subgraph_edges_filtered(self, manifest_parser: ManifestParser) -> None:
        """Test get_subgraph() only includes edges between relevant nodes."""
        nodes, edges = manifest_parser.get_subgraph(
            center_node="model.my_project.stg_orders", upstream_depth=1, downstream_depth=1
        )

        node_ids = {n["unique_id"] for n in nodes}
        # All edges should be between nodes in the subgraph
        for edge in edges:
            assert edge["source"] in node_ids
            assert edge["target"] in node_ids


class TestGetModelByName:
    """Tests for get_model_by_name() method."""

    def test_get_model_by_name_found(self, manifest_parser: ManifestParser) -> None:
        """Test get_model_by_name() returns model when found."""
        model = manifest_parser.get_model_by_name("stg_orders")

        assert model is not None
        assert model.name == "stg_orders"
        assert model.unique_id == "model.my_project.stg_orders"

    def test_get_model_by_name_not_found(self, manifest_parser: ManifestParser) -> None:
        """Test get_model_by_name() returns None when not found."""
        model = manifest_parser.get_model_by_name("nonexistent")

        assert model is None


class TestFindManifest:
    """Tests for find_manifest() function."""

    def test_find_manifest_explicit_path_exists(self, tmp_manifest: Path) -> None:
        """Test find_manifest() with explicit path that exists."""
        result = find_manifest(manifest_path=tmp_manifest)

        assert result == tmp_manifest

    def test_find_manifest_explicit_path_not_exists(self, tmp_path: Path) -> None:
        """Test find_manifest() with explicit path that doesn't exist raises error."""
        nonexistent = tmp_path / "nonexistent.json"

        with pytest.raises(FileNotFoundError, match="Manifest not found at"):
            find_manifest(manifest_path=nonexistent)

    def test_find_manifest_in_target_directory(self, tmp_path: Path) -> None:
        """Test find_manifest() finds manifest in target/ subdirectory."""
        target_dir = tmp_path / "target"
        target_dir.mkdir()
        manifest = target_dir / "manifest.json"
        manifest.write_text("{}")

        result = find_manifest(start_path=tmp_path)

        assert result == manifest

    def test_find_manifest_walks_up_to_dbt_project(self, tmp_path: Path) -> None:
        """Test find_manifest() walks up directory tree to find dbt_project.yml."""
        # Create dbt project structure
        project_root = tmp_path / "my_project"
        project_root.mkdir()
        (project_root / "dbt_project.yml").write_text("name: my_project")
        target_dir = project_root / "target"
        target_dir.mkdir()
        manifest = target_dir / "manifest.json"
        manifest.write_text("{}")

        # Start from subdirectory
        subdir = project_root / "models" / "staging"
        subdir.mkdir(parents=True)

        result = find_manifest(start_path=subdir)

        assert result == manifest

    def test_find_manifest_dbt_project_no_manifest_raises_error(self, tmp_path: Path) -> None:
        """Test find_manifest() raises error when dbt_project.yml exists but no manifest."""
        project_root = tmp_path / "my_project"
        project_root.mkdir()
        (project_root / "dbt_project.yml").write_text("name: my_project")
        # No target/manifest.json

        with pytest.raises(FileNotFoundError, match="but target/manifest.json does not exist"):
            find_manifest(start_path=project_root)

    def test_find_manifest_not_found_raises_error(self, tmp_path: Path) -> None:
        """Test find_manifest() raises error when manifest cannot be found."""
        with pytest.raises(FileNotFoundError, match="Could not find manifest.json"):
            find_manifest(start_path=tmp_path)
