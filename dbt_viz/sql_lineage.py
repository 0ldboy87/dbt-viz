"""SQL parsing for column-level lineage using sqlglot."""

import logging
from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)


@dataclass
class ColumnLineage:
    """Lineage information for a single column."""

    column_name: str
    source_columns: list[str] = field(default_factory=list)  # ["table.column", ...]
    transformation: str = "unknown"  # passthrough, rename, derived, aggregated, windowed, literal
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


# Type alias for column maps used by CTE/subquery trace-through.
# Maps output column name -> list of source references (e.g. ["table.col", ...])
ColumnMap = dict[str, list[str]]

# Maximum depth for CTE/subquery trace-through to prevent infinite recursion
MAX_CTE_TRACE_DEPTH = 20


class SQLLineageParser:
    """Parse SQL to extract column-level lineage."""

    def __init__(self, dialect: str = "snowflake"):
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
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            if parsed is None:
                return result

            # Build table alias map for the outermost scope
            table_aliases = self._build_table_alias_map(parsed)

            # Build column maps for CTEs and subqueries so we can trace through them
            cte_maps = self._build_cte_column_maps(parsed, table_aliases, schema)
            subquery_maps = self._build_subquery_column_maps(
                parsed, table_aliases, schema, cte_maps
            )

            if isinstance(parsed, exp.Union):
                # UNION at root level — merge sources from all branches
                union_cols = self._extract_union_columns(parsed, schema, cte_maps)
                for col_name, sources in union_cols.items():
                    result.columns[col_name.lower()] = ColumnLineage(
                        column_name=col_name,
                        source_columns=sources,
                        transformation="derived"
                        if len(sources) > 1
                        else ("passthrough" if sources else "literal"),
                    )
            else:
                # Regular SELECT (possibly with CTEs)
                select_columns = self._extract_select_columns(
                    parsed, table_aliases, schema, cte_maps
                )

                for col_name, col_data in select_columns.items():
                    if isinstance(col_data, list):
                        # Pre-resolved sources (from SELECT * expansion)
                        result.columns[col_name.lower()] = ColumnLineage(
                            column_name=col_name,
                            source_columns=col_data,
                            transformation="passthrough"
                            if len(col_data) == 1
                            else ("derived" if col_data else "unknown"),
                        )
                    else:
                        # Normal expression — trace lineage
                        lineage_info = self._trace_column_lineage(
                            col_name, col_data, table_aliases, schema, cte_maps, subquery_maps
                        )
                        result.columns[col_name.lower()] = lineage_info

        except Exception as e:
            # Broad exception catch: sqlglot may raise various exceptions for malformed SQL
            logger.warning("Failed to parse SQL for lineage: %s", e, exc_info=True)

        return result

    # -------------------------------------------------------------------------
    # SELECT column extraction
    # -------------------------------------------------------------------------

    def _extract_select_columns(
        self,
        parsed: exp.Expression,
        table_aliases: dict[str, str],
        schema: dict[str, dict[str, str]] | None,
        cte_maps: dict[str, ColumnMap],
    ) -> dict[str, exp.Expression | list[str]]:
        """Extract column names and expressions from SELECT clause.

        Returns a dict mapping column name to either:
        - exp.Expression (normal columns, to be traced later)
        - list[str] (pre-resolved sources from star expansion)
        """
        columns: dict[str, exp.Expression | list[str]] = {}

        select = parsed.find(exp.Select)
        if select is None:
            return columns

        for expr in select.expressions:
            # Handle SELECT *
            if isinstance(expr, exp.Star):
                star_cols = self._expand_star(None, table_aliases, schema, cte_maps)
                for star_col, sources in star_cols.items():
                    columns[star_col] = sources
                continue

            # Handle table.*
            if isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
                table_ref = expr.table
                star_cols = self._expand_star(table_ref, table_aliases, schema, cte_maps)
                for star_col, sources in star_cols.items():
                    columns[star_col] = sources
                continue

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

    # -------------------------------------------------------------------------
    # Table alias map
    # -------------------------------------------------------------------------

    def _build_table_alias_map(self, parsed: exp.Expression) -> dict[str, str]:
        """Build a map of table aliases to actual table names for the outermost scope."""
        aliases: dict[str, str] = {}

        # Collect CTE and subquery names first so we can recognize them in table refs
        cte_names: set[str] = set()
        for cte in parsed.find_all(exp.CTE):
            if cte.alias:
                cte_names.add(cte.alias)

        subquery_aliases: set[str] = set()
        for subquery in parsed.find_all(exp.Subquery):
            if subquery.alias:
                subquery_aliases.add(subquery.alias)

        # Map table references, using CTE:/SUBQUERY: prefix when appropriate
        for table in parsed.find_all(exp.Table):
            table_name = table.name
            key = table.alias if table.alias else table_name
            if table_name in cte_names:
                aliases[key] = f"CTE:{table_name}"
            elif table_name in subquery_aliases:
                aliases[key] = f"SUBQUERY:{table_name}"
            else:
                aliases[key] = table_name

        # Ensure CTE and subquery names are also registered directly
        for name in cte_names:
            if name not in aliases:
                aliases[name] = f"CTE:{name}"
        for name in subquery_aliases:
            if name not in aliases:
                aliases[name] = f"SUBQUERY:{name}"

        return aliases

    def _build_local_alias_map(
        self,
        node: exp.Expression,
        cte_maps: dict[str, ColumnMap],
    ) -> dict[str, str]:
        """Build alias map for a local scope (CTE body or subquery).

        Marks references to known CTEs as CTE:name so they can be traced through.
        """
        aliases: dict[str, str] = {}

        target = node
        if isinstance(target, exp.Union):
            target = target.left

        for table in target.find_all(exp.Table):
            table_name = table.name
            key = table.alias if table.alias else table_name
            if table_name in cte_maps:
                aliases[key] = f"CTE:{table_name}"
            else:
                aliases[key] = table_name

        for subquery in target.find_all(exp.Subquery):
            if subquery.alias:
                aliases[subquery.alias] = f"SUBQUERY:{subquery.alias}"

        return aliases

    # -------------------------------------------------------------------------
    # CTE trace-through
    # -------------------------------------------------------------------------

    def _build_cte_column_maps(
        self,
        parsed: exp.Expression,
        table_aliases: dict[str, str],
        schema: dict[str, dict[str, str]] | None,
    ) -> dict[str, ColumnMap]:
        """Build column maps for each CTE.

        CTEs are processed in definition order (which is dependency order),
        so a CTE can reference earlier CTEs.

        Returns: {cte_name: {output_col: [source_table.source_col, ...]}}
        """
        cte_maps: dict[str, ColumnMap] = {}

        with_clause = parsed.find(exp.With)
        if not with_clause:
            return cte_maps

        for cte in with_clause.expressions:
            cte_name = cte.alias
            inner = cte.this  # The CTE body (Select or Union)

            try:
                local_aliases = self._build_local_alias_map(inner, cte_maps)

                if isinstance(inner, exp.Union):
                    col_map = self._process_union_columns(inner, schema, cte_maps)
                else:
                    col_map = self._process_select_for_column_map(
                        inner, local_aliases, schema, cte_maps
                    )

                cte_maps[cte_name] = col_map
            except Exception as e:
                # Broad exception catch: defensive guard for malformed CTE definitions
                logger.debug("Failed to parse CTE '%s': %s", cte_name, e)
                cte_maps[cte_name] = {}

        return cte_maps

    # -------------------------------------------------------------------------
    # Subquery trace-through
    # -------------------------------------------------------------------------

    def _build_subquery_column_maps(
        self,
        parsed: exp.Expression,
        table_aliases: dict[str, str],
        schema: dict[str, dict[str, str]] | None,
        cte_maps: dict[str, ColumnMap],
    ) -> dict[str, ColumnMap]:
        """Build column maps for subqueries in the FROM clause."""
        subquery_maps: dict[str, ColumnMap] = {}

        select = parsed if isinstance(parsed, exp.Select) else parsed.find(exp.Select)
        if select is None:
            return subquery_maps

        for subquery in select.find_all(exp.Subquery):
            sq_alias = subquery.alias
            if not sq_alias:
                continue

            inner = subquery.this
            try:
                local_aliases = self._build_local_alias_map(inner, cte_maps)

                if isinstance(inner, exp.Union):
                    col_map = self._process_union_columns(inner, schema, cte_maps)
                else:
                    col_map = self._process_select_for_column_map(
                        inner, local_aliases, schema, cte_maps
                    )

                subquery_maps[sq_alias] = col_map
            except Exception as e:
                # Broad exception catch: defensive guard for malformed subquery definitions
                logger.debug("Failed to parse subquery '%s': %s", sq_alias, e)
                subquery_maps[sq_alias] = {}

        return subquery_maps

    # -------------------------------------------------------------------------
    # Shared helpers for building column maps (used by CTE + subquery)
    # -------------------------------------------------------------------------

    def _process_select_for_column_map(
        self,
        select: exp.Select,
        local_aliases: dict[str, str],
        schema: dict[str, dict[str, str]] | None,
        cte_maps: dict[str, ColumnMap],
    ) -> ColumnMap:
        """Process a SELECT's columns into a column map {col_name: [sources]}.

        Used for building CTE and subquery column maps.
        """
        col_map: ColumnMap = {}

        for expr in select.expressions:
            # Handle SELECT *
            if isinstance(expr, exp.Star):
                star_cols = self._expand_star(None, local_aliases, schema, cte_maps)
                col_map.update(star_cols)
                continue

            # Handle table.*
            if isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star):
                table_ref = expr.table
                star_cols = self._expand_star(table_ref, local_aliases, schema, cte_maps)
                col_map.update(star_cols)
                continue

            col_name = self._get_column_alias(expr)
            if not col_name:
                continue

            col_name_lower = col_name.lower()
            inner = expr.this if isinstance(expr, exp.Alias) else expr

            # Collect source columns from the expression
            sources: list[str] = []
            for col_ref in inner.find_all(exp.Column):
                if isinstance(col_ref.this, exp.Star):
                    continue
                source = self._resolve_column_source(col_ref, local_aliases)
                if source:
                    traced = self._trace_through_cte(source, cte_maps)
                    for s in traced:
                        if s not in sources:
                            sources.append(s)

            # If the expression is a simple column reference and find_all didn't
            # yield it (shouldn't happen, but guard against it)
            if (
                not sources
                and isinstance(inner, exp.Column)
                and not isinstance(inner.this, exp.Star)
            ):
                source = self._resolve_column_source(inner, local_aliases)
                if source:
                    traced = self._trace_through_cte(source, cte_maps)
                    sources.extend(traced)

            col_map[col_name_lower] = sources

        return col_map

    def _trace_through_cte(
        self,
        source: str,
        cte_maps: dict[str, ColumnMap],
        subquery_maps: dict[str, ColumnMap] | None = None,
        _depth: int = 0,
    ) -> list[str]:
        """Recursively resolve CTE/subquery references to original source columns.

        e.g. "CTE:my_cte.customer_id" -> ["stg_customers.customer_id"]
        """
        if _depth > MAX_CTE_TRACE_DEPTH:
            return [source]

        parts = source.split(".")
        if len(parts) < 2:
            return [source]

        table_part = ".".join(parts[:-1])
        col_name = parts[-1].lower()

        # Check CTE references
        if table_part.startswith("CTE:"):
            cte_name = table_part[4:]
            if cte_name in cte_maps and col_name in cte_maps[cte_name]:
                inner_sources = cte_maps[cte_name][col_name]
                if not inner_sources:
                    # Column exists in CTE but has no traceable sources (e.g. COUNT(*))
                    return []
                resolved: list[str] = []
                for inner_source in inner_sources:
                    resolved.extend(
                        self._trace_through_cte(inner_source, cte_maps, subquery_maps, _depth + 1)
                    )
                return resolved if resolved else [source]
            return [source]

        # Check subquery references
        if table_part.startswith("SUBQUERY:") and subquery_maps:
            sq_name = table_part[9:]
            if sq_name in subquery_maps and col_name in subquery_maps[sq_name]:
                inner_sources = subquery_maps[sq_name][col_name]
                if not inner_sources:
                    return []
                resolved = []
                for inner_source in inner_sources:
                    resolved.extend(
                        self._trace_through_cte(inner_source, cte_maps, subquery_maps, _depth + 1)
                    )
                return resolved if resolved else [source]
            return [source]

        return [source]

    # -------------------------------------------------------------------------
    # SELECT * expansion
    # -------------------------------------------------------------------------

    def _expand_star(
        self,
        table_ref: str | None,
        local_aliases: dict[str, str],
        schema: dict[str, dict[str, str]] | None,
        cte_maps: dict[str, ColumnMap],
    ) -> ColumnMap:
        """Expand SELECT * or table.* into individual columns.

        Args:
            table_ref: None for SELECT *, or the table alias/name for table.*
            local_aliases: Alias map for the current scope
            schema: Schema info {table_name: {column_name: type}}
            cte_maps: CTE column maps for resolution

        Returns: {col_name: [source_table.col_name, ...]}
        """
        result: ColumnMap = {}

        if table_ref:
            # table.* — expand columns from one specific table
            actual_table = local_aliases.get(table_ref, table_ref)
            columns = self._get_table_columns(actual_table, schema, cte_maps)
            for col_name in columns:
                source = f"{actual_table}.{col_name}"
                traced = self._trace_through_cte(source, cte_maps)
                result[col_name] = traced
        else:
            # SELECT * — expand columns from all tables in scope
            for _alias, actual_table in local_aliases.items():
                columns = self._get_table_columns(actual_table, schema, cte_maps)
                for col_name in columns:
                    if col_name not in result:  # first table wins for duplicates
                        source = f"{actual_table}.{col_name}"
                        traced = self._trace_through_cte(source, cte_maps)
                        result[col_name] = traced

        if not result:
            logger.debug(
                "Cannot expand %s: no schema info available", f"{table_ref}.*" if table_ref else "*"
            )

        return result

    def _get_table_columns(
        self,
        table_name: str,
        schema: dict[str, dict[str, str]] | None,
        cte_maps: dict[str, ColumnMap],
    ) -> list[str]:
        """Get column names for a table from CTE maps or schema info."""
        # Check CTE maps
        if table_name.startswith("CTE:"):
            cte_name = table_name[4:]
            if cte_name in cte_maps:
                return list(cte_maps[cte_name].keys())

        # Check schema info
        if schema:
            if table_name in schema:
                return list(schema[table_name].keys())
            for key in schema:
                if key.lower() == table_name.lower():
                    return list(schema[key].keys())

        return []

    # -------------------------------------------------------------------------
    # UNION support
    # -------------------------------------------------------------------------

    def _get_union_branches(self, node: exp.Expression) -> list[exp.Select]:
        """Recursively collect all SELECT branches from a UNION tree."""
        if isinstance(node, exp.Union):
            branches: list[exp.Select] = []
            branches.extend(self._get_union_branches(node.left))
            branches.extend(self._get_union_branches(node.right))
            return branches
        elif isinstance(node, exp.Select):
            return [node]
        return []

    def _extract_union_columns(
        self,
        union: exp.Union,
        schema: dict[str, dict[str, str]] | None,
        cte_maps: dict[str, ColumnMap],
    ) -> ColumnMap:
        """Process a UNION node. Returns {col_name: [merged_sources]}.

        Column names come from the first branch. Sources are merged from all branches.
        """
        branches = self._get_union_branches(union)
        if not branches:
            return {}

        # Use first branch for column names
        first_col_names: list[str] = []
        for expr in branches[0].expressions:
            col_name = self._get_column_alias(expr)
            if col_name:
                first_col_names.append(col_name.lower())

        result: ColumnMap = {name: [] for name in first_col_names}

        # Process each branch and merge sources
        for branch in branches:
            branch_aliases = self._build_local_alias_map(branch, cte_maps)

            for i, expr in enumerate(branch.expressions):
                if i >= len(first_col_names):
                    break
                col_name = first_col_names[i]

                inner = expr.this if isinstance(expr, exp.Alias) else expr

                for col_ref in inner.find_all(exp.Column):
                    if isinstance(col_ref.this, exp.Star):
                        continue
                    source = self._resolve_column_source(col_ref, branch_aliases)
                    if source:
                        traced = self._trace_through_cte(source, cte_maps)
                        for s in traced:
                            if s not in result[col_name]:
                                result[col_name].append(s)

                # Handle simple column reference
                if isinstance(inner, exp.Column) and not isinstance(inner.this, exp.Star):
                    source = self._resolve_column_source(inner, branch_aliases)
                    if source:
                        traced = self._trace_through_cte(source, cte_maps)
                        for s in traced:
                            if s not in result[col_name]:
                                result[col_name].append(s)

        return result

    def _process_union_columns(
        self,
        union: exp.Union,
        schema: dict[str, dict[str, str]] | None,
        cte_maps: dict[str, ColumnMap],
    ) -> ColumnMap:
        """Process a UNION for CTE/subquery column map building.

        Same as _extract_union_columns but used as an internal helper.
        """
        return self._extract_union_columns(union, schema, cte_maps)

    # -------------------------------------------------------------------------
    # Column lineage tracing
    # -------------------------------------------------------------------------

    def _trace_column_lineage(
        self,
        col_name: str,
        col_expr: exp.Expression,
        table_aliases: dict[str, str],
        schema: dict[str, dict[str, str]] | None,
        cte_maps: dict[str, ColumnMap] | None = None,
        subquery_maps: dict[str, ColumnMap] | None = None,
    ) -> ColumnLineage:
        """Trace lineage for a single column expression."""
        result = ColumnLineage(column_name=col_name)
        _cte_maps = cte_maps or {}
        _sq_maps = subquery_maps or {}

        if isinstance(col_expr, exp.Alias):
            inner_expr = col_expr.this
            result = self._analyze_expression(
                col_name, inner_expr, table_aliases, _cte_maps, _sq_maps
            )
            # Check if it's a simple rename vs passthrough
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
                traced = self._trace_through_cte(source, _cte_maps, _sq_maps)
                result.source_columns = traced
        else:
            # Complex expression
            result = self._analyze_expression(
                col_name, col_expr, table_aliases, _cte_maps, _sq_maps
            )

        return result

    def _analyze_expression(
        self,
        col_name: str,
        expr: exp.Expression,
        table_aliases: dict[str, str],
        cte_maps: dict[str, ColumnMap] | None = None,
        subquery_maps: dict[str, ColumnMap] | None = None,
    ) -> ColumnLineage:
        """Analyze an expression to determine transformation type and sources."""
        result = ColumnLineage(column_name=col_name)
        _cte_maps = cte_maps or {}
        _sq_maps = subquery_maps or {}

        # Get the SQL representation of the expression
        result.expression = expr.sql(dialect=self.dialect)

        # Find all column references in the expression
        source_columns: list[str] = []
        for col in expr.find_all(exp.Column):
            if isinstance(col.this, exp.Star):
                continue
            source = self._resolve_column_source(col, table_aliases)
            if source:
                traced = self._trace_through_cte(source, _cte_maps, _sq_maps)
                for s in traced:
                    if s not in source_columns:
                        source_columns.append(s)

        result.source_columns = source_columns

        # Determine transformation type (order matters: window before aggregation)
        if self._is_window_function(expr):
            result.transformation = "windowed"
        elif self._is_aggregation(expr):
            result.transformation = "aggregated"
        elif len(source_columns) == 0:
            result.transformation = "literal"
        elif len(source_columns) == 1 and isinstance(expr, exp.Column):
            result.transformation = "passthrough"
        else:
            result.transformation = "derived"

        return result

    # -------------------------------------------------------------------------
    # Column resolution
    # -------------------------------------------------------------------------

    def _resolve_column_source(self, col: exp.Column, table_aliases: dict[str, str]) -> str | None:
        """Resolve a column reference to table.column format."""
        col_name = col.name
        table_ref = col.table if col.table else None

        if table_ref:
            actual_table = table_aliases.get(table_ref, table_ref)
            return f"{actual_table}.{col_name}"
        elif len(table_aliases) == 1:
            table_name = list(table_aliases.values())[0]
            return f"{table_name}.{col_name}"
        else:
            return col_name

    # -------------------------------------------------------------------------
    # Expression classification
    # -------------------------------------------------------------------------

    def _is_window_function(self, expr: exp.Expression) -> bool:
        """Check if expression contains a window function."""
        return any(isinstance(node, exp.Window) for node in expr.walk())

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
                table_ref = ".".join(parts[:-1])
                col_name = parts[-1]

                if table_ref in table_map:
                    resolved_sources.append(f"{table_map[table_ref]}.{col_name}")
                else:
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
