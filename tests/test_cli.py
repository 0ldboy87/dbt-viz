"""Tests for CLI commands."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from dbt_viz.cli import _get_parser, app

runner = CliRunner()


@pytest.fixture
def mock_server():  # type: ignore[misc,no-untyped-def]
    """Mock VisualizationServer to prevent actual server startup."""
    with patch("dbt_viz.cli.VisualizationServer") as mock:
        server_instance = MagicMock()
        mock.return_value = server_instance
        yield server_instance


class TestLineageCommand:
    """Tests for the lineage command."""

    def test_lineage_valid_manifest(self, manifest_path: Path, mock_server: MagicMock) -> None:
        """Test lineage command with valid manifest shows node count."""
        result = runner.invoke(app, ["lineage", "--manifest", str(manifest_path)])

        assert result.exit_code == 0
        assert "Found" in result.stdout
        assert "nodes" in result.stdout
        assert "edges" in result.stdout
        mock_server.start.assert_called_once()

    def test_lineage_with_manifest_flag(self, manifest_path: Path, mock_server: MagicMock) -> None:
        """Test lineage command with --manifest flag works."""
        result = runner.invoke(app, ["lineage", "-m", str(manifest_path)])

        assert result.exit_code == 0
        assert "Found" in result.stdout
        mock_server.start.assert_called_once()

    def test_lineage_nonexistent_model(self, manifest_path: Path, mock_server: MagicMock) -> None:
        """Test lineage command with non-existent model exits with error."""
        result = runner.invoke(
            app, ["lineage", "nonexistent_model", "--manifest", str(manifest_path)]
        )

        assert result.exit_code == 1
        assert "Error" in result.stdout
        assert "not found" in result.stdout
        mock_server.start.assert_not_called()

    def test_lineage_nonexistent_manifest(self, mock_server: MagicMock) -> None:
        """Test lineage command with non-existent manifest exits with error."""
        result = runner.invoke(app, ["lineage", "--manifest", "/nonexistent/manifest.json"])

        assert result.exit_code == 1
        assert "Error" in result.stdout
        mock_server.start.assert_not_called()

    def test_lineage_with_valid_model(self, manifest_path: Path, mock_server: MagicMock) -> None:
        """Test lineage command with valid model name."""
        from dbt_viz.manifest import ManifestParser

        parser = ManifestParser(manifest_path)
        parser.parse()

        models = [n for n in parser.nodes.values() if n.resource_type == "model"]
        if models:
            model_name = models[0].name
            result = runner.invoke(app, ["lineage", model_name, "--manifest", str(manifest_path)])

            assert result.exit_code == 0
            assert "Found" in result.stdout
            mock_server.start.assert_called_once()

    def test_lineage_with_depth_options(self, manifest_path: Path, mock_server: MagicMock) -> None:
        """Test lineage command with upstream/downstream depth options."""
        result = runner.invoke(
            app,
            [
                "lineage",
                "--manifest",
                str(manifest_path),
                "--upstream",
                "2",
                "--downstream",
                "1",
            ],
        )

        assert result.exit_code == 0
        assert "Found" in result.stdout
        mock_server.start.assert_called_once()

    def test_lineage_with_port_option(self, manifest_path: Path, mock_server: MagicMock) -> None:
        """Test lineage command with custom port."""
        result = runner.invoke(
            app,
            ["lineage", "--manifest", str(manifest_path), "--port", "9000"],
        )

        assert result.exit_code == 0
        with patch("dbt_viz.cli.VisualizationServer") as mock_server_class:
            server_instance = MagicMock()
            mock_server_class.return_value = server_instance

            runner.invoke(
                app,
                ["lineage", "--manifest", str(manifest_path), "--port", "9000"],
            )

            mock_server_class.assert_called_with(port=9000)


class TestInfoCommand:
    """Tests for the info command."""

    def test_info_valid_model(self, manifest_path: Path) -> None:
        """Test info command with valid model shows details."""
        from dbt_viz.manifest import ManifestParser

        parser = ManifestParser(manifest_path)
        parser.parse()

        models = [n for n in parser.nodes.values() if n.resource_type == "model"]
        if models:
            model_name = models[0].name
            result = runner.invoke(app, ["info", model_name, "--manifest", str(manifest_path)])

            assert result.exit_code == 0
            assert model_name in result.stdout or models[0].unique_id in result.stdout
            assert "Database" in result.stdout or "Schema" in result.stdout

    def test_info_nonexistent_model(self, manifest_path: Path) -> None:
        """Test info command with non-existent model exits with error."""
        result = runner.invoke(app, ["info", "nonexistent_model", "--manifest", str(manifest_path)])

        assert result.exit_code == 1
        assert "Error" in result.stdout
        assert "not found" in result.stdout

    def test_info_shows_database_schema(self, manifest_path: Path) -> None:
        """Test info command output includes database and schema."""
        from dbt_viz.manifest import ManifestParser

        parser = ManifestParser(manifest_path)
        parser.parse()

        models = [n for n in parser.nodes.values() if n.resource_type == "model"]
        if models:
            model_name = models[0].name
            result = runner.invoke(app, ["info", model_name, "--manifest", str(manifest_path)])

            assert result.exit_code == 0
            assert "Database" in result.stdout
            assert "Schema" in result.stdout

    def test_info_with_manifest_flag(self, manifest_path: Path) -> None:
        """Test info command with -m flag works."""
        from dbt_viz.manifest import ManifestParser

        parser = ManifestParser(manifest_path)
        parser.parse()

        models = [n for n in parser.nodes.values() if n.resource_type == "model"]
        if models:
            model_name = models[0].name
            result = runner.invoke(app, ["info", model_name, "-m", str(manifest_path)])

            assert result.exit_code == 0
            assert "Database" in result.stdout


class TestGetParserHelper:
    """Tests for the _get_parser helper function."""

    def test_get_parser_with_manifest_path(self, manifest_path: Path) -> None:
        """Test _get_parser with explicit manifest path."""
        parser = _get_parser(manifest_path)

        assert parser is not None
        assert len(parser.nodes) > 0

    def test_get_parser_with_enrich_true(self, manifest_path: Path) -> None:
        """Test _get_parser with enrich=True enriches columns."""
        with patch("dbt_viz.cli.ManifestParser") as mock_parser_class:
            mock_parser = MagicMock()
            mock_parser_class.return_value = mock_parser

            _get_parser(manifest_path, enrich=True)

            mock_parser.parse.assert_called_once()
            mock_parser.enrich_columns.assert_called_once()

    def test_get_parser_with_enrich_false(self, manifest_path: Path) -> None:
        """Test _get_parser with enrich=False skips column enrichment."""
        with patch("dbt_viz.cli.ManifestParser") as mock_parser_class:
            mock_parser = MagicMock()
            mock_parser_class.return_value = mock_parser

            _get_parser(manifest_path, enrich=False)

            mock_parser.parse.assert_called_once()
            mock_parser.enrich_columns.assert_not_called()

    def test_get_parser_calls_find_manifest(self) -> None:
        """Test _get_parser calls find_manifest when no path provided."""
        with patch("dbt_viz.cli.find_manifest") as mock_find:
            with patch("dbt_viz.cli.ManifestParser") as mock_parser_class:
                mock_find.return_value = Path("tests/fixtures/manifest.json")
                mock_parser = MagicMock()
                mock_parser_class.return_value = mock_parser

                _get_parser(None)

                mock_find.assert_called_once_with(manifest_path=None)


class TestHelpCommand:
    """Tests for help output."""

    def test_help_shows_both_commands(self) -> None:
        """Test --help shows both lineage and info commands."""
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "lineage" in result.stdout
        assert "info" in result.stdout

    def test_lineage_help(self) -> None:
        """Test lineage --help shows command options."""
        result = runner.invoke(app, ["lineage", "--help"])

        assert result.exit_code == 0
        assert "--manifest" in result.stdout
        assert "--port" in result.stdout
        assert "--upstream" in result.stdout
        assert "--downstream" in result.stdout

    def test_info_help(self) -> None:
        """Test info --help shows command options."""
        result = runner.invoke(app, ["info", "--help"])

        assert result.exit_code == 0
        assert "--manifest" in result.stdout
