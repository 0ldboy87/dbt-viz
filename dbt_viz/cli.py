"""CLI entry point for dbt-viz."""

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from .manifest import ManifestParser, find_manifest
from .server import VisualizationServer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(name)s - %(levelname)s - %(message)s",
)

# SQL preview truncation limit
SQL_PREVIEW_MAX_CHARS = 500

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
        str | None,
        typer.Argument(help="Model to center the visualization on (optional)"),
    ] = None,
    manifest: Annotated[
        Path | None,
        typer.Option("--manifest", "-m", help="Path to manifest.json"),
    ] = None,
    port: Annotated[
        int,
        typer.Option("--port", "-p", help="Server port"),
    ] = 8080,
    upstream: Annotated[
        int | None,
        typer.Option("--upstream", "-u", help="Depth of upstream models to show"),
    ] = None,
    downstream: Annotated[
        int | None,
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
        raise typer.Exit(1) from e
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e
    except OSError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e


@app.command()
def info(
    model_name: Annotated[
        str,
        typer.Argument(help="Name of the model to display info for"),
    ],
    manifest: Annotated[
        Path | None,
        typer.Option("--manifest", "-m", help="Path to manifest.json"),
    ] = None,
) -> None:
    """Print model details to terminal."""
    try:
        parser = _get_parser(manifest, enrich=False)

        model = parser.get_model_by_name(model_name)
        if model is None:
            if model_name not in parser.nodes:
                console.print(f"[red]Error:[/red] Model '{model_name}' not found")
                raise typer.Exit(1)
            model = parser.nodes[model_name]

        console.print(f"[bold cyan]{model.name}[/bold cyan]")
        console.print(f"  [dim]ID:[/dim] {model.unique_id}")
        console.print(f"  [dim]Type:[/dim] {model.resource_type}")
        console.print(f"  [dim]Materialization:[/dim] {model.materialized}")
        console.print(f"  [dim]Database:[/dim] {model.database}")
        console.print(f"  [dim]Schema:[/dim] {model.schema_name}")
        if model.description:
            console.print(f"  [dim]Description:[/dim] {model.description}")
        if model.file_path:
            console.print(f"  [dim]Path:[/dim] {model.file_path}")
        if model.tags:
            console.print(f"  [dim]Tags:[/dim] {', '.join(model.tags)}")

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1) from e


if __name__ == "__main__":
    app()
