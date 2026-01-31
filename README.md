# dbt-viz

Interactive visualization tool for dbt model lineage.

## Features

- **Browser-based D3.js visualization** with force-directed graph layout
- Nodes colored by type (model, source, seed, snapshot)
- Directional edges showing data flow
- Zoom, pan, and drag controls
- Click nodes to view detailed information
- Search/filter by model name
- Highlight upstream/downstream dependencies on hover
- Terminal-based model info command

## Installation

```bash
pip install dbt-viz
```

Or install from source:

```bash
git clone https://github.com/your-username/dbt-viz.git
cd dbt-viz
pip install -e .
```

## Usage

### Lineage Visualization

Open an interactive graph visualization in your browser:

```bash
# Visualize entire project
dbt-viz lineage

# Center on a specific model
dbt-viz lineage my_model

# Limit depth of dependencies
dbt-viz lineage my_model --upstream 2 --downstream 1

# Specify manifest location
dbt-viz lineage --manifest /path/to/manifest.json

# Use a different port
dbt-viz lineage --port 3000
```

### Model Info

Print model details to the terminal:

```bash
dbt-viz info my_model
dbt-viz info my_model --manifest /path/to/manifest.json
```

## Commands

### `dbt-viz lineage [MODEL_NAME]`

Open interactive lineage visualization in browser.

**Arguments:**
- `MODEL_NAME` - (optional) Model to center the visualization on

**Options:**
- `--manifest, -m PATH` - Path to manifest.json
- `--port, -p PORT` - Server port (default: 8080)
- `--upstream, -u N` - Depth of upstream models to show
- `--downstream, -d N` - Depth of downstream models to show

### `dbt-viz info MODEL_NAME`

Print model details to terminal.

**Arguments:**
- `MODEL_NAME` - Name of the model to display info for

**Options:**
- `--manifest, -m PATH` - Path to manifest.json

## Manifest Discovery

The tool automatically finds your `manifest.json`:

1. If `--manifest` is provided, uses that path
2. Checks `target/manifest.json` in current directory
3. Walks up directory tree looking for `dbt_project.yml`
4. Uses `target/manifest.json` relative to project root

**Note:** Run `dbt compile` or `dbt run` to generate the manifest before using dbt-viz.

## Visualization Features

### Graph Interaction
- **Zoom:** Scroll or use +/- buttons
- **Pan:** Click and drag on background
- **Move nodes:** Click and drag nodes
- **Search:** Type in search box to filter nodes
- **Hover:** Highlights upstream/downstream dependencies
- **Click:** Opens details panel

### Details Panel
Shows model information including:
- Name and description
- Database and schema
- Materialization type
- File path
- Tags
- Column definitions
- SQL preview

## Requirements

- Python 3.9+
- A dbt project with `manifest.json` generated

## Dependencies

- `typer` - CLI framework
- `rich` - Terminal formatting
