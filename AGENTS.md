# Agent Guidelines for dbt-viz

## Project Overview

`dbt-viz` is a CLI tool that reads a dbt `manifest.json` and renders an interactive lineage graph in the browser. The stack is pure Python (backend + parsing) with a single-file vanilla JS/D3 frontend served from an embedded template.

## Running Things

### Tests
```bash
uv run pytest tests/ -v
```

### Running the tool locally (dev version)
Always use `uv run` from the project root to pick up the source rather than the globally installed binary:
```bash
uv run dbt-viz lineage --manifest /path/to/target/manifest.json
```

### Global install vs local dev version
There is a globally installed `dbt-viz` at `~/.local/share/uv/tools/dbt-viz`.

**Always install with the `-e` (editable) flag during development:**
```bash
uv tool install --reinstall -e .
```
`uv tool install .` (without `-e`) copies all files — including `index.html` — into the
tools directory at install time. Changes to the source are invisible until you reinstall.
With `-e .`, the installed package's `__file__` points back to the source directory, so:
- HTML/JS changes → visible on next browser hard-refresh (no reinstall needed)
- Python changes → visible after restarting the server (no reinstall needed)
- Entry point / `pyproject.toml` changes → require `uv tool install --reinstall -e .`

### Test project
Use `../hv-dig-analytics` (relative to this repo) for manual end-to-end testing against a real dbt project.

## Architecture

| File | Responsibility |
|---|---|
| `dbt_viz/manifest.py` | Parse `manifest.json`, build graph, expose `ModelInfo` dataclass |
| `dbt_viz/columns.py` | Enrich columns from `catalog.json` and compiled SQL lineage |
| `dbt_viz/cli.py` | Typer CLI entry points (`lineage`, `info`) |
| `dbt_viz/server.py` | Minimal HTTP server — serves `index.html` and `/data.json` |
| `dbt_viz/templates/index.html` | Entire frontend: D3 graph, node detail panel, SQL viewer |
| `tests/` | pytest suite; fixtures in `tests/fixtures/`, shared setup in `conftest.py` |

## Data Flow

```
manifest.json  ──► ManifestParser.parse()  ──► ModelInfo nodes
catalog.json   ──► enrich_columns()         ──► enriched columns + compiled_sql
disk .sql files ──► _parse_node()            ──► current_sql (raw file content)
                                                      │
                                    get_subgraph() → to_dict() → /data.json → JS
```

## Key Design Decisions

### Staleness detection
`raw_sql` (from `manifest.raw_code`) is the Jinja template as of the last `dbt compile`.
`current_sql` (read from `{project_root}/{original_file_path}` at startup) is what is on disk right now.
The browser compares these (after whitespace normalisation) to warn if a model has been edited since the last compile.
**Do not** compare `raw_sql` vs `compiled_sql` — compiled SQL always differs from Jinja templates by design.

### project_root resolution
`ManifestParser` derives the project root as `manifest_path.parent.parent` (i.e. the directory containing `target/`). This assumes the standard dbt layout where `manifest.json` lives at `{project_root}/target/manifest.json`.

### Static server
The server is fully static: data is parsed once at startup and served from memory. There is no file-watching or live-reload. A browser refresh re-fetches `/data.json` but gets the same snapshot.

## Conventions

- Python 3.11+, type hints throughout, dataclasses for data models.
- `ModelInfo.to_dict()` must be kept in sync with any new fields added to the dataclass — this is what reaches the JS frontend.
- Frontend JS accesses node fields directly off the JSON object (e.g. `node.current_sql`). Field names in `to_dict()` are the JS API.
- Tests use `tmp_path` (pytest built-in) and `tmp_manifest` (from `conftest.py`) for isolated filesystem tests.
- Run the full test suite before committing: `uv run pytest tests/ -v`.
