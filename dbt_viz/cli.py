"""CLI entry point for dbt-viz."""

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .manifest import ManifestParser, find_manifest
from .server import VisualizationServer

app = typer.Typer(
    name="dbt-viz",
    help="Interactive visualization tool for dbt model lineage.",
    no_args_is_help=True,
)

console = Console()


def _get_parser(manifest: Path | None, enrich: bool = True) -> ManifestParser:
    """Get a ManifestParser instance."""
    manifest_path = find_manifest(manifest_path=manifest)
    parser = ManifestParser(manifest_path)
    parser.parse()
    if enrich:
        parser.enrich_columns()
    return parser


@app.command()
def lineage(
    model_name: Annotated[
        Optional[str],
        typer.Argument(help="Model to center the visualization on (optional)"),
    ] = None,
    manifest: Annotated[
        Optional[Path],
        typer.Option("--manifest", "-m", help="Path to manifest.json"),
    ] = None,
    port: Annotated[
        int,
        typer.Option("--port", "-p", help="Server port"),
    ] = 8080,
    upstream: Annotated[
        Optional[int],
        typer.Option("--upstream", "-u", help="Depth of upstream models to show"),
    ] = None,
    downstream: Annotated[
        Optional[int],
        typer.Option("--downstream", "-d", help="Depth of downstream models to show"),
    ] = None,
) -> None:
    """Open interactive lineage visualization in browser."""
    try:
        parser = _get_parser(manifest)

        # Validate model exists if specified
        center_node = None
        if model_name:
            model = parser.get_model_by_name(model_name)
            if model is None:
                # Try as unique_id
                if model_name not in parser.nodes:
                    console.print(f"[red]Error:[/red] Model '{model_name}' not found")
                    raise typer.Exit(1)
                center_node = model_name
            else:
                center_node = model.unique_id

        nodes, edges = parser.get_subgraph(
            center_node=center_node,
            upstream_depth=upstream,
            downstream_depth=downstream,
        )

        if not nodes:
            console.print("[yellow]Warning:[/yellow] No models found in manifest")
            raise typer.Exit(1)

        console.print(f"[green]Found {len(nodes)} nodes and {len(edges)} edges[/green]")

        server = VisualizationServer(port=port)
        server.start(nodes, edges, center_node=model_name)

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    except OSError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def info(
    model_name: Annotated[
        str,
        typer.Argument(help="Name of the model to display info for"),
    ],
    manifest: Annotated[
        Optional[Path],
        typer.Option("--manifest", "-m", help="Path to manifest.json"),
    ] = None,
) -> None:
    """Print model details to terminal."""
    try:
        parser = _get_parser(manifest)

        model = parser.get_model_by_name(model_name)
        if model is None:
            # Try as unique_id
            if model_name in parser.nodes:
                model = parser.nodes[model_name]
            else:
                console.print(f"[red]Error:[/red] Model '{model_name}' not found")
                raise typer.Exit(1)

        # Create header panel
        header = f"[bold]{model.name}[/bold]"
        if model.description:
            header += f"\n{model.description}"

        console.print(Panel(header, title=model.resource_type.upper(), border_style="blue"))

        # Basic info table
        info_table = Table(show_header=False, box=None, padding=(0, 2))
        info_table.add_column("Key", style="dim")
        info_table.add_column("Value")

        info_table.add_row("Database", model.database or "N/A")
        info_table.add_row("Schema", model.schema_name or "N/A")
        if model.materialized:
            info_table.add_row("Materialization", model.materialized)
        if model.file_path:
            info_table.add_row("File", model.file_path)
        if model.tags:
            info_table.add_row("Tags", ", ".join(model.tags))

        console.print(info_table)
        console.print()

        # Dependencies
        upstream = parser.get_upstream(model.unique_id, depth=1)
        downstream = parser.get_downstream(model.unique_id, depth=1)

        if upstream:
            console.print("[bold]Upstream Dependencies:[/bold]")
            for uid in upstream:
                node = parser.nodes[uid]
                console.print(f"  ← {node.name} [dim]({node.resource_type})[/dim]")
            console.print()

        if downstream:
            console.print("[bold]Downstream Dependents:[/bold]")
            for uid in downstream:
                node = parser.nodes[uid]
                console.print(f"  → {node.name} [dim]({node.resource_type})[/dim]")
            console.print()

        # Columns
        if model.columns:
            console.print("[bold]Columns:[/bold]")
            col_table = Table(show_header=True, header_style="bold", box=None)
            col_table.add_column("Name")
            col_table.add_column("Type", style="dim")
            col_table.add_column("Description")

            for col in model.columns.values():
                col_table.add_row(
                    col.get("name", ""),
                    col.get("data_type", ""),
                    col.get("description", ""),
                )

            console.print(col_table)
            console.print()

        # SQL preview
        if model.raw_sql:
            sql_preview = model.raw_sql[:500]
            if len(model.raw_sql) > 500:
                sql_preview += "\n..."
            console.print(Panel(sql_preview, title="SQL Preview", border_style="dim"))

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
