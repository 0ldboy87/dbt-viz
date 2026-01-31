"""SQL parsing for column-level lineage using sqlglot."""

from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage


@dataclass
class ColumnLineage:
    """Lineage information for a single column."""

    column_name: str
    source_columns: list[str] = field(default_factory=list)  # ["table.column", ...]
    transformation: str = "unknown"  # passthrough, rename, derived, aggregated
    expression: str = ""  # The SQL expression if derived

    def to_dict(self) -> dict[str, Any]:
        return {
            "column_name": self.column_name,
            "source_columns": self.source_columns,
            "transformation": self.transformation,
            "expression": self.expression,
        }


@dataclass
class TableLineage:
    """Column lineage for a table/model."""

    table_name: str
    columns: dict[str, ColumnLineage] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "columns": {name: col.to_dict() for name, col in self.columns.items()},
        }


class SQLLineageParser:
    """Parse SQL to extract column-level lineage."""

    def __init__(self, dialect: str = "snowflake"):
        """
        Initialize the parser.

        Args:
            dialect: SQL dialect (snowflake, postgres, bigquery, etc.)
        """
        self.dialect = dialect

    def parse_sql(self, sql: str, schema: dict[str, dict[str, str]] | None = None) -> TableLineage:
        """
        Parse SQL and extract column lineage.

        Args:
            sql: The SQL query to parse
            schema: Optional schema info {table_name: {column_name: type}}

        Returns:
            TableLineage with column-level lineage information
        """
        result = TableLineage(table_name="")

        try:
            # Parse the SQL
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            if parsed is None:
                return result

            # Get the output columns from SELECT
            select_columns = self._extract_select_columns(parsed)

            # Build table alias map
            table_aliases = self._build_table_alias_map(parsed)

            # For each output column, trace its lineage
            for col_name, col_expr in select_columns.items():
                lineage_info = self._trace_column_lineage(
                    col_name, col_expr, table_aliases, schema
                )
                result.columns[col_name.lower()] = lineage_info

        except Exception as e:
            # If parsing fails, return empty result
            # In production, you might want to log this
            pass

        return result

    def _extract_select_columns(self, parsed: exp.Expression) -> dict[str, exp.Expression]:
        """Extract column names and expressions from SELECT clause."""
        columns = {}

        # Find the main SELECT statement
        select = parsed.find(exp.Select)
        if select is None:
            return columns

        for expr in select.expressions:
            col_name = self._get_column_alias(expr)
            if col_name:
                columns[col_name] = expr

        return columns

    def _get_column_alias(self, expr: exp.Expression) -> str | None:
        """Get the output column name (alias or original name)."""
        if isinstance(expr, exp.Alias):
            return expr.alias
        elif isinstance(expr, exp.Column):
            return expr.name
        elif hasattr(expr, "alias") and expr.alias:
            return expr.alias
        elif hasattr(expr, "name"):
            return expr.name
        return None

    def _build_table_alias_map(self, parsed: exp.Expression) -> dict[str, str]:
        """Build a map of table aliases to actual table names."""
        aliases = {}

        # Find all table references
        for table in parsed.find_all(exp.Table):
            table_name = table.name
            if table.alias:
                aliases[table.alias] = table_name
            else:
                aliases[table_name] = table_name

        # Also check subqueries and CTEs
        for cte in parsed.find_all(exp.CTE):
            if cte.alias:
                aliases[cte.alias] = f"CTE:{cte.alias}"

        return aliases

    def _trace_column_lineage(
        self,
        col_name: str,
        col_expr: exp.Expression,
        table_aliases: dict[str, str],
        schema: dict[str, dict[str, str]] | None,
    ) -> ColumnLineage:
        """Trace lineage for a single column expression."""
        result = ColumnLineage(column_name=col_name)

        # Determine transformation type and source columns
        if isinstance(col_expr, exp.Alias):
            inner_expr = col_expr.this
            result = self._analyze_expression(col_name, inner_expr, table_aliases)
            # Check if it's a simple rename
            if isinstance(inner_expr, exp.Column):
                if inner_expr.name.lower() != col_name.lower():
                    result.transformation = "rename"
                else:
                    result.transformation = "passthrough"
        elif isinstance(col_expr, exp.Column):
            # Direct column reference
            result.transformation = "passthrough"
            source = self._resolve_column_source(col_expr, table_aliases)
            if source:
                result.source_columns = [source]
        else:
            # Complex expression
            result = self._analyze_expression(col_name, col_expr, table_aliases)

        return result

    def _analyze_expression(
        self,
        col_name: str,
        expr: exp.Expression,
        table_aliases: dict[str, str],
    ) -> ColumnLineage:
        """Analyze an expression to determine transformation type and sources."""
        result = ColumnLineage(column_name=col_name)

        # Get the SQL representation of the expression
        result.expression = expr.sql(dialect=self.dialect)

        # Find all column references in the expression
        source_columns = []
        for col in expr.find_all(exp.Column):
            source = self._resolve_column_source(col, table_aliases)
            if source and source not in source_columns:
                source_columns.append(source)

        result.source_columns = source_columns

        # Determine transformation type
        if self._is_aggregation(expr):
            result.transformation = "aggregated"
        elif len(source_columns) == 0:
            result.transformation = "literal"
        elif len(source_columns) == 1 and isinstance(expr, exp.Column):
            result.transformation = "passthrough"
        else:
            result.transformation = "derived"

        return result

    def _resolve_column_source(
        self, col: exp.Column, table_aliases: dict[str, str]
    ) -> str | None:
        """Resolve a column reference to table.column format."""
        col_name = col.name
        table_ref = col.table if col.table else None

        if table_ref:
            # Resolve alias to actual table name
            actual_table = table_aliases.get(table_ref, table_ref)
            return f"{actual_table}.{col_name}"
        elif len(table_aliases) == 1:
            # Only one table, assume it's from that
            table_name = list(table_aliases.values())[0]
            return f"{table_name}.{col_name}"
        else:
            # Ambiguous - just return the column name
            return col_name

    def _is_aggregation(self, expr: exp.Expression) -> bool:
        """Check if expression contains aggregation functions."""
        agg_functions = {
            exp.Count,
            exp.Sum,
            exp.Avg,
            exp.Min,
            exp.Max,
            exp.ArrayAgg,
            exp.GroupConcat,
        }

        for node in expr.walk():
            if type(node) in agg_functions:
                return True
            # Also check for generic function names
            if isinstance(node, exp.Anonymous):
                func_name = node.name.upper() if node.name else ""
                if func_name in ("COUNT", "SUM", "AVG", "MIN", "MAX", "ARRAY_AGG", "LISTAGG"):
                    return True

        return False


def parse_model_lineage(
    sql: str,
    model_name: str,
    upstream_tables: dict[str, str] | None = None,
    dialect: str = "snowflake",
) -> TableLineage:
    """
    Parse SQL for a model and extract column lineage.

    Args:
        sql: The compiled SQL for the model
        model_name: Name of the model being parsed
        upstream_tables: Map of table references to model unique_ids
        dialect: SQL dialect

    Returns:
        TableLineage with column-level lineage
    """
    parser = SQLLineageParser(dialect=dialect)
    result = parser.parse_sql(sql)
    result.table_name = model_name
    return result


def resolve_table_references(
    lineage: TableLineage,
    table_map: dict[str, str],
) -> TableLineage:
    """
    Resolve table names in lineage to model unique_ids.

    Args:
        lineage: The parsed lineage
        table_map: Map of {schema.table_name: unique_id} or {table_name: unique_id}

    Returns:
        Updated TableLineage with resolved references
    """
    for col in lineage.columns.values():
        resolved_sources = []
        for source in col.source_columns:
            parts = source.split(".")
            if len(parts) >= 2:
                # Try full match first (schema.table.column)
                table_ref = ".".join(parts[:-1])
                col_name = parts[-1]

                # Try to find matching table
                if table_ref in table_map:
                    resolved_sources.append(f"{table_map[table_ref]}.{col_name}")
                else:
                    # Try just table name
                    table_name = parts[-2] if len(parts) >= 2 else parts[0]
                    for key, uid in table_map.items():
                        if key.endswith(table_name) or key == table_name:
                            resolved_sources.append(f"{uid}.{col_name}")
                            break
                    else:
                        resolved_sources.append(source)
            else:
                resolved_sources.append(source)

        col.source_columns = resolved_sources

    return lineage
