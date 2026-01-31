"""Column-level lineage data collection and parsing."""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ColumnInfo:
    """Information about a column."""

    name: str
    data_type: str = ""
    description: str = ""
    # Source columns that this column derives from
    # Format: ["model.project.table.column", ...]
    sources: list[str] = field(default_factory=list)
    # Type of transformation: passthrough, rename, derived, aggregated
    transformation: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "data_type": self.data_type,
            "description": self.description,
            "sources": self.sources,
            "transformation": self.transformation,
        }


@dataclass
class TableColumns:
    """Columns for a table/model."""

    unique_id: str
    name: str
    columns: dict[str, ColumnInfo] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "unique_id": self.unique_id,
            "name": self.name,
            "columns": {name: col.to_dict() for name, col in self.columns.items()},
        }


class CatalogParser:
    """Parse dbt catalog.json for column information with data types."""

    def __init__(self, catalog_path: Path):
        self.catalog_path = catalog_path
        self.tables: dict[str, TableColumns] = {}

    def parse(self) -> None:
        """Parse the catalog file."""
        with open(self.catalog_path) as f:
            catalog = json.load(f)

        # Parse nodes (models, seeds, snapshots)
        for unique_id, node_data in catalog.get("nodes", {}).items():
            self._parse_node(unique_id, node_data)

        # Parse sources
        for unique_id, source_data in catalog.get("sources", {}).items():
            self._parse_node(unique_id, source_data)

    def _parse_node(self, unique_id: str, data: dict[str, Any]) -> None:
        """Parse a node from the catalog."""
        metadata = data.get("metadata", {})
        columns_data = data.get("columns", {})

        table = TableColumns(
            unique_id=unique_id,
            name=metadata.get("name", ""),
        )

        for col_name, col_data in columns_data.items():
            table.columns[col_name.lower()] = ColumnInfo(
                name=col_data.get("name", col_name),
                data_type=col_data.get("type", ""),
                description=col_data.get("comment", ""),
            )

        self.tables[unique_id] = table

    def get_columns(self, unique_id: str) -> dict[str, ColumnInfo]:
        """Get columns for a specific table."""
        if unique_id in self.tables:
            return self.tables[unique_id].columns
        return {}


class CompiledSQLReader:
    """Read compiled SQL files from dbt's target/compiled directory."""

    def __init__(self, compiled_path: Path):
        self.compiled_path = compiled_path
        self.sql_files: dict[str, str] = {}  # unique_id -> sql content

    def find_sql_files(self, manifest_nodes: dict[str, Any]) -> None:
        """
        Find compiled SQL files for each model in the manifest.

        Args:
            manifest_nodes: Nodes from manifest.json to map file paths
        """
        if not self.compiled_path.exists():
            return

        for unique_id, node_data in manifest_nodes.items():
            resource_type = node_data.get("resource_type", "")
            if resource_type not in ("model", "snapshot"):
                continue

            # Get the compiled path from the node
            compiled_path = node_data.get("compiled_path")
            if compiled_path:
                full_path = self.compiled_path.parent.parent / compiled_path
                if full_path.exists():
                    self.sql_files[unique_id] = full_path.read_text()
                    continue

            # Fallback: try to find by original_file_path
            original_path = node_data.get("original_file_path", "")
            if original_path:
                # compiled files are in target/compiled/<project_name>/models/...
                # Try to find matching file
                for sql_file in self.compiled_path.rglob("*.sql"):
                    if sql_file.name == Path(original_path).name:
                        self.sql_files[unique_id] = sql_file.read_text()
                        break

    def get_sql(self, unique_id: str) -> str | None:
        """Get compiled SQL for a model."""
        return self.sql_files.get(unique_id)


class ColumnCollector:
    """
    Collect column information from multiple sources.

    Priority for data types:
    1. catalog.json (actual database types)
    2. manifest.json (documented types)
    """

    def __init__(
        self,
        manifest_path: Path,
        catalog_path: Path | None = None,
        compiled_path: Path | None = None,
    ):
        self.manifest_path = manifest_path
        self.catalog_path = catalog_path
        self.compiled_path = compiled_path

        self.manifest_columns: dict[str, dict[str, ColumnInfo]] = {}
        self.catalog_parser: CatalogParser | None = None
        self.sql_reader: CompiledSQLReader | None = None

        # Merged column info
        self.columns: dict[str, dict[str, ColumnInfo]] = {}

    def collect(self) -> None:
        """Collect column information from all sources."""
        # 1. Parse manifest for documented columns
        self._parse_manifest_columns()

        # 2. Parse catalog if available
        if self.catalog_path and self.catalog_path.exists():
            self.catalog_parser = CatalogParser(self.catalog_path)
            self.catalog_parser.parse()

        # 3. Read compiled SQL if available
        if self.compiled_path and self.compiled_path.exists():
            with open(self.manifest_path) as f:
                manifest = json.load(f)
            self.sql_reader = CompiledSQLReader(self.compiled_path)
            self.sql_reader.find_sql_files(manifest.get("nodes", {}))

        # 4. Merge all column information
        self._merge_columns()

    def _parse_manifest_columns(self) -> None:
        """Parse columns from manifest.json."""
        with open(self.manifest_path) as f:
            manifest = json.load(f)

        # Parse nodes
        for unique_id, node_data in manifest.get("nodes", {}).items():
            columns_data = node_data.get("columns", {})
            if columns_data:
                self.manifest_columns[unique_id] = {}
                for col_name, col_data in columns_data.items():
                    self.manifest_columns[unique_id][col_name.lower()] = ColumnInfo(
                        name=col_data.get("name", col_name),
                        data_type=col_data.get("data_type", ""),
                        description=col_data.get("description", ""),
                    )

        # Parse sources
        for unique_id, source_data in manifest.get("sources", {}).items():
            columns_data = source_data.get("columns", {})
            if columns_data:
                self.manifest_columns[unique_id] = {}
                for col_name, col_data in columns_data.items():
                    self.manifest_columns[unique_id][col_name.lower()] = ColumnInfo(
                        name=col_data.get("name", col_name),
                        data_type=col_data.get("data_type", ""),
                        description=col_data.get("description", ""),
                    )

    def _merge_columns(self) -> None:
        """Merge column information from all sources."""
        # Start with manifest columns
        for unique_id, cols in self.manifest_columns.items():
            self.columns[unique_id] = {}
            for col_name, col_info in cols.items():
                self.columns[unique_id][col_name] = ColumnInfo(
                    name=col_info.name,
                    data_type=col_info.data_type,
                    description=col_info.description,
                )

        # Overlay catalog data (has better type info)
        if self.catalog_parser:
            for unique_id, table in self.catalog_parser.tables.items():
                if unique_id not in self.columns:
                    self.columns[unique_id] = {}

                for col_name, col_info in table.columns.items():
                    if col_name in self.columns[unique_id]:
                        # Update type from catalog (more accurate)
                        if col_info.data_type:
                            self.columns[unique_id][col_name].data_type = col_info.data_type
                        # Keep description from manifest if catalog doesn't have one
                        if col_info.description and not self.columns[unique_id][col_name].description:
                            self.columns[unique_id][col_name].description = col_info.description
                    else:
                        # Add column from catalog that wasn't in manifest
                        self.columns[unique_id][col_name] = col_info

    def get_columns(self, unique_id: str) -> dict[str, ColumnInfo]:
        """Get merged columns for a model."""
        return self.columns.get(unique_id, {})

    def get_compiled_sql(self, unique_id: str) -> str | None:
        """Get compiled SQL for a model."""
        if self.sql_reader:
            return self.sql_reader.get_sql(unique_id)
        return None

    def get_all_tables_with_columns(self) -> dict[str, dict[str, ColumnInfo]]:
        """Get all tables with their columns."""
        return self.columns


def find_catalog(manifest_path: Path) -> Path | None:
    """Find catalog.json relative to manifest.json."""
    # catalog.json is typically in the same directory as manifest.json
    catalog_path = manifest_path.parent / "catalog.json"
    if catalog_path.exists():
        return catalog_path
    return None


def find_compiled_path(manifest_path: Path) -> Path | None:
    """Find compiled SQL directory relative to manifest.json."""
    # compiled SQL is in target/compiled/
    compiled_path = manifest_path.parent / "compiled"
    if compiled_path.exists():
        return compiled_path
    return None
