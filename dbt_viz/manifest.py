"""Manifest parsing and graph building for dbt projects."""

import json
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .columns import ColumnCollector, find_catalog, find_compiled_path


@dataclass
class ModelInfo:
    """Information about a dbt model/node."""

    unique_id: str
    name: str
    resource_type: str
    description: str = ""
    schema_name: str = ""
    database: str = ""
    materialized: str = ""
    columns: dict[str, dict[str, Any]] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    file_path: str = ""
    raw_sql: str = ""
    compiled_sql: str = ""
    current_sql: str = ""
    layer: str = ""
    source_system: str = ""
    depends_on: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "unique_id": self.unique_id,
            "name": self.name,
            "resource_type": self.resource_type,
            "description": self.description,
            "schema": self.schema_name,
            "database": self.database,
            "materialized": self.materialized,
            "columns": self.columns,
            "tags": self.tags,
            "file_path": self.file_path,
            "raw_sql": self.raw_sql,
            "compiled_sql": self.compiled_sql,
            "current_sql": self.current_sql,
            "layer": self.layer,
            "source_system": self.source_system,
        }


class ManifestParser:
    """Parse dbt manifest.json and build dependency graph."""

    SUPPORTED_RESOURCE_TYPES = {"model", "source", "seed", "snapshot"}

    def __init__(self, manifest_path: Path):
        self.manifest_path = manifest_path
        self.nodes: dict[str, ModelInfo] = {}
        self.edges: list[tuple[str, str]] = []  # (from, to) representing data flow
        self._downstream: dict[str, set[str]] = {}
        self._upstream: dict[str, set[str]] = {}
        self.column_collector: ColumnCollector | None = None

    def parse(self) -> None:
        """Parse the manifest file and build the graph."""
        with open(self.manifest_path) as f:
            manifest = json.load(f)

        # Parse nodes (models, seeds, snapshots)
        for unique_id, node_data in manifest.get("nodes", {}).items():
            resource_type = node_data.get("resource_type", "")
            if resource_type not in self.SUPPORTED_RESOURCE_TYPES:
                continue

            model = self._parse_node(unique_id, node_data, resource_type)
            self.nodes[unique_id] = model

        # Parse sources
        for unique_id, source_data in manifest.get("sources", {}).items():
            model = self._parse_source(unique_id, source_data)
            self.nodes[unique_id] = model

        # Build edges from depends_on
        self._build_edges()

    def _parse_node(self, unique_id: str, data: dict[str, Any], resource_type: str) -> ModelInfo:
        """Parse a node (model, seed, snapshot) from manifest."""
        config = data.get("config", {})
        depends_on_nodes = data.get("depends_on", {}).get("nodes", [])

        columns = {}
        for col_name, col_data in data.get("columns", {}).items():
            columns[col_name] = {
                "name": col_name,
                "description": col_data.get("description", ""),
                "data_type": col_data.get("data_type", ""),
            }

        original_file_path = data.get("original_file_path", "")
        current_sql = ""
        if original_file_path:
            project_root = self.manifest_path.parent.parent  # target/ -> project root
            sql_file = project_root / original_file_path
            if sql_file.exists():
                current_sql = sql_file.read_text()

        # Derive layer and source system from the file path.
        # models/staging/dwh/stg_dwh__foo.sql -> layer="staging", source_system="dwh"
        # seeds/cardata_backbone/seed_foo.csv  -> layer="seeds",   source_system="cardata_backbone"
        # snapshots/foo.sql                    -> layer="snapshots", source_system=""
        path_parts = Path(original_file_path).parts
        root_dir = path_parts[0] if path_parts else ""
        if root_dir == "models":
            layer = path_parts[1] if len(path_parts) > 1 else ""
            source_system = path_parts[2] if len(path_parts) > 2 else ""
        else:
            layer = root_dir
            # source_system is the second segment only when there is a proper subdirectory
            # (i.e. three or more parts: root / subdir / file)
            source_system = path_parts[1] if len(path_parts) > 2 else ""

        return ModelInfo(
            unique_id=unique_id,
            name=data.get("name", ""),
            resource_type=resource_type,
            description=data.get("description", ""),
            schema_name=data.get("schema", ""),
            database=data.get("database", ""),
            materialized=config.get("materialized", ""),
            columns=columns,
            tags=data.get("tags", []),
            file_path=original_file_path,
            raw_sql=data.get("raw_code", data.get("raw_sql", "")),
            current_sql=current_sql,
            layer=layer,
            source_system=source_system,
            depends_on=depends_on_nodes,
        )

    def _parse_source(self, unique_id: str, data: dict[str, Any]) -> ModelInfo:
        """Parse a source from manifest."""
        columns = {}
        for col_name, col_data in data.get("columns", {}).items():
            columns[col_name] = {
                "name": col_name,
                "description": col_data.get("description", ""),
                "data_type": col_data.get("data_type", ""),
            }

        return ModelInfo(
            unique_id=unique_id,
            name=data.get("name", ""),
            resource_type="source",
            description=data.get("description", ""),
            schema_name=data.get("schema", ""),
            database=data.get("database", ""),
            columns=columns,
            tags=data.get("tags", []),
            file_path=data.get("path", ""),
            layer="source",
            source_system=data.get("source_name", ""),
        )

    def _build_edges(self) -> None:
        """Build edge list and upstream/downstream maps from depends_on."""
        for unique_id in self.nodes:
            self._upstream[unique_id] = set()
            self._downstream[unique_id] = set()

        for unique_id, model in self.nodes.items():
            for dep_id in model.depends_on:
                if dep_id in self.nodes:
                    # Edge goes from dependency to dependent (data flow direction)
                    self.edges.append((dep_id, unique_id))
                    self._upstream[unique_id].add(dep_id)
                    self._downstream[dep_id].add(unique_id)

    def enrich_columns(self) -> None:
        """Enrich column information from catalog.json and SQL lineage."""
        catalog_path = find_catalog(self.manifest_path)
        compiled_path = find_compiled_path(self.manifest_path)

        self.column_collector = ColumnCollector(
            self.manifest_path,
            catalog_path,
            compiled_path,
        )
        self.column_collector.collect()

        # Update nodes with enriched column data including lineage
        for unique_id, model in self.nodes.items():
            enriched_cols = self.column_collector.get_columns(unique_id)
            if enriched_cols:
                # Merge enriched columns into model
                updated_columns = {}
                for col_name, col_info in enriched_cols.items():
                    updated_columns[col_name] = {
                        "name": col_info.name,
                        "data_type": col_info.data_type,
                        "description": col_info.description,
                        "sources": col_info.sources,
                        "transformation": col_info.transformation,
                    }
                model.columns = updated_columns

            # Add compiled SQL if available
            compiled_sql = self.column_collector.get_compiled_sql(unique_id)
            if compiled_sql:
                model.compiled_sql = compiled_sql

    def get_upstream(self, unique_id: str, depth: int | None = None) -> set[str]:
        """Get upstream dependencies up to specified depth."""
        return self._traverse(unique_id, self._upstream, depth)

    def get_downstream(self, unique_id: str, depth: int | None = None) -> set[str]:
        """Get downstream dependents up to specified depth."""
        return self._traverse(unique_id, self._downstream, depth)

    def _traverse(self, start_id: str, graph: dict[str, set[str]], depth: int | None) -> set[str]:
        """BFS traversal up to specified depth."""
        if start_id not in graph:
            return set()

        visited = set()
        queue: deque[tuple[str, int]] = deque([(start_id, 0)])

        while queue:
            node_id, current_depth = queue.popleft()

            # Skip if we've exceeded depth limit
            if depth is not None and current_depth >= depth:
                continue

            for neighbor in graph.get(node_id, set()):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, current_depth + 1))

        return visited

    def get_subgraph(
        self,
        center_node: str | None = None,
        upstream_depth: int | None = None,
        downstream_depth: int | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
        """
        Get nodes and edges for visualization.

        If center_node is specified, return subgraph around that node.
        Otherwise return entire graph.
        """
        if center_node is None:
            # Return entire graph
            nodes = [model.to_dict() for model in self.nodes.values()]
            edges = [{"source": src, "target": tgt} for src, tgt in self.edges]
            return nodes, edges

        # Find node by name if not found by unique_id
        node_id = center_node
        if node_id not in self.nodes:
            for uid, model in self.nodes.items():
                if model.name == center_node:
                    node_id = uid
                    break
            else:
                raise ValueError(f"Model '{center_node}' not found in manifest")

        # Get relevant nodes
        relevant_nodes = {node_id}
        relevant_nodes.update(self.get_upstream(node_id, upstream_depth))
        relevant_nodes.update(self.get_downstream(node_id, downstream_depth))

        nodes = [self.nodes[nid].to_dict() for nid in relevant_nodes]
        edges = [
            {"source": src, "target": tgt}
            for src, tgt in self.edges
            if src in relevant_nodes and tgt in relevant_nodes
        ]

        return nodes, edges

    def get_model_by_name(self, name: str) -> ModelInfo | None:
        """Find a model by name."""
        for model in self.nodes.values():
            if model.name == name:
                return model
        return None


def find_manifest(start_path: Path | None = None, manifest_path: Path | None = None) -> Path:
    """
    Find the manifest.json file.

    Args:
        start_path: Directory to start searching from (defaults to CWD)
        manifest_path: Explicit path to manifest.json

    Returns:
        Path to manifest.json

    Raises:
        FileNotFoundError: If manifest cannot be found
    """
    if manifest_path is not None:
        if manifest_path.exists():
            return manifest_path
        raise FileNotFoundError(f"Manifest not found at: {manifest_path}")

    if start_path is None:
        start_path = Path.cwd()

    # First check for target/manifest.json in current directory
    target_manifest = start_path / "target" / "manifest.json"
    if target_manifest.exists():
        return target_manifest

    # Walk up looking for dbt_project.yml
    current = start_path
    while current != current.parent:
        if (current / "dbt_project.yml").exists():
            manifest = current / "target" / "manifest.json"
            if manifest.exists():
                return manifest
            raise FileNotFoundError(
                f"Found dbt project at {current}, but target/manifest.json does not exist. "
                "Run 'dbt compile' or 'dbt run' first."
            )
        current = current.parent

    raise FileNotFoundError(
        "Could not find manifest.json. Either:\n"
        "  1. Run this command from a dbt project directory\n"
        "  2. Use --manifest to specify the path to manifest.json\n"
        "  3. Run 'dbt compile' to generate the manifest"
    )
