"""Tests for server functionality."""

import json
from io import BytesIO
from unittest.mock import Mock, mock_open, patch

import pytest

from dbt_viz.server import VisualizationHandler, VisualizationServer


def create_mock_handler() -> VisualizationHandler:
    handler = object.__new__(VisualizationHandler)
    handler.send_response = Mock()  # type: ignore[method-assign]
    handler.send_header = Mock()  # type: ignore[method-assign]
    handler.end_headers = Mock()  # type: ignore[method-assign]
    handler.send_error = Mock()  # type: ignore[method-assign]
    handler.wfile = BytesIO()  # type: ignore[assignment]
    handler.path = "/"
    return handler


class TestVisualizationHandler:
    def test_send_json_data_returns_correct_json(self) -> None:
        handler = create_mock_handler()
        VisualizationHandler.graph_data = {
            "nodes": [{"id": "model1", "type": "model"}],
            "edges": [{"source": "model1", "target": "model2"}],
        }
        VisualizationHandler.center_node = "model1"

        handler.send_json_data()

        handler.send_response.assert_called_once_with(200)  # type: ignore[attr-defined]
        handler.send_header.assert_called_once_with("Content-type", "application/json")  # type: ignore[attr-defined]
        handler.end_headers.assert_called_once()  # type: ignore[attr-defined]

        written_data = handler.wfile.getvalue()  # type: ignore[attr-defined]
        data = json.loads(written_data.decode())
        assert data["nodes"] == [{"id": "model1", "type": "model"}]
        assert data["edges"] == [{"source": "model1", "target": "model2"}]
        assert data["centerNode"] == "model1"

    def test_send_json_data_handles_empty_graph(self) -> None:
        handler = create_mock_handler()
        VisualizationHandler.graph_data = {}
        VisualizationHandler.center_node = None

        handler.send_json_data()

        written_data = handler.wfile.getvalue()  # type: ignore[attr-defined]
        data = json.loads(written_data.decode())
        assert data["nodes"] == []
        assert data["edges"] == []
        assert data["centerNode"] is None

    def test_send_visualization_page_returns_html(self) -> None:
        handler = create_mock_handler()
        html_content = "<html><body>Test Visualization</body></html>"
        with patch("builtins.open", mock_open(read_data=html_content)):
            handler.send_visualization_page()

        handler.send_response.assert_called_once_with(200)  # type: ignore[attr-defined]
        handler.send_header.assert_any_call("Content-type", "text/html")  # type: ignore[attr-defined]
        handler.end_headers.assert_called_once()  # type: ignore[attr-defined]

        written_data = handler.wfile.getvalue()  # type: ignore[attr-defined]
        assert written_data.decode() == html_content

    def test_do_get_routes_to_visualization_page(self) -> None:
        handler = create_mock_handler()
        handler.send_visualization_page = Mock()

        handler.path = "/"
        handler.do_GET()
        handler.send_visualization_page.assert_called_once()

        handler.send_visualization_page.reset_mock()
        handler.path = "/index.html"
        handler.do_GET()
        handler.send_visualization_page.assert_called_once()

    def test_do_get_routes_to_json_data(self) -> None:
        handler = create_mock_handler()
        handler.send_json_data = Mock()

        handler.path = "/data.json"
        handler.do_GET()
        handler.send_json_data.assert_called_once()

    def test_do_get_returns_404_for_unknown_path(self) -> None:
        handler = create_mock_handler()

        handler.path = "/unknown/path"
        handler.do_GET()
        handler.send_error.assert_called_once_with(404, "Not Found")  # type: ignore[attr-defined]

    def test_log_message_suppresses_output(self) -> None:
        handler = create_mock_handler()
        with patch("sys.stderr") as mock_stderr:
            handler.log_message("GET %s", "/test")
            mock_stderr.write.assert_not_called()


class TestVisualizationServer:
    """Tests for VisualizationServer."""

    def test_constructor_sets_port(self) -> None:
        """Test that constructor sets the port correctly."""
        server = VisualizationServer(port=3000)
        assert server.port == 3000
        assert server.server is None
        assert server.thread is None

    def test_constructor_uses_default_port(self) -> None:
        """Test that constructor uses default port 8080."""
        server = VisualizationServer()
        assert server.port == 8080

    def test_stop_handles_none_server(self) -> None:
        """Test that stop() handles None server gracefully."""
        server = VisualizationServer()
        # Should not raise any exceptions
        server.stop()
        assert server.server is None

    def test_stop_shuts_down_server(self) -> None:
        """Test that stop() properly shuts down the server."""
        server = VisualizationServer()

        # Create mock server
        mock_tcp_server = Mock()
        server.server = mock_tcp_server

        # Call stop
        server.stop()

        # Verify shutdown and close were called
        mock_tcp_server.shutdown.assert_called_once()
        mock_tcp_server.server_close.assert_called_once()
        assert server.server is None

    def test_start_sets_graph_data_on_handler(self) -> None:
        """Test that start() sets graph data on handler class."""
        server = VisualizationServer(port=8888)

        nodes = [{"id": "model1", "type": "model"}]
        edges = [{"source": "model1", "target": "model2"}]
        center_node = "model1"

        # Mock TCPServer to prevent actual server creation
        with patch("socketserver.TCPServer") as mock_tcp_server:
            mock_server_instance = Mock()
            mock_tcp_server.return_value = mock_server_instance

            # Mock serve_forever to prevent blocking
            mock_server_instance.serve_forever.side_effect = KeyboardInterrupt()

            # Mock print to suppress output
            with patch("builtins.print"):
                try:
                    server.start(
                        nodes=nodes,
                        edges=edges,
                        center_node=center_node,
                        open_browser=False,
                    )
                except KeyboardInterrupt:
                    pass

        # Verify graph data was set on handler class
        assert VisualizationHandler.graph_data == {"nodes": nodes, "edges": edges}
        assert VisualizationHandler.center_node == center_node

    def test_start_raises_error_on_port_in_use(self) -> None:
        """Test that start() raises OSError when port is in use."""
        server = VisualizationServer(port=8888)

        nodes = [{"id": "model1"}]
        edges = []

        # Mock TCPServer to raise OSError
        with patch("socketserver.TCPServer") as mock_tcp_server:
            mock_tcp_server.side_effect = OSError("Address already in use")

            with pytest.raises(OSError, match="Port 8888 is already in use"):
                server.start(nodes=nodes, edges=edges, open_browser=False)

    def test_start_does_not_open_browser_when_disabled(self) -> None:
        """Test that start() does not open browser when open_browser=False."""
        server = VisualizationServer(port=8888)

        nodes = [{"id": "model1"}]
        edges = []

        with patch("socketserver.TCPServer") as mock_tcp_server:
            mock_server_instance = Mock()
            mock_tcp_server.return_value = mock_server_instance
            mock_server_instance.serve_forever.side_effect = KeyboardInterrupt()

            with patch("threading.Timer") as mock_timer:
                with patch("builtins.print"):
                    try:
                        server.start(nodes=nodes, edges=edges, open_browser=False)
                    except KeyboardInterrupt:
                        pass

                # Verify Timer was not called (browser not opened)
                mock_timer.assert_not_called()

    def test_start_opens_browser_when_enabled(self) -> None:
        """Test that start() opens browser when open_browser=True."""
        server = VisualizationServer(port=8888)

        nodes = [{"id": "model1"}]
        edges = []

        with patch("socketserver.TCPServer") as mock_tcp_server:
            mock_server_instance = Mock()
            mock_tcp_server.return_value = mock_server_instance
            mock_server_instance.serve_forever.side_effect = KeyboardInterrupt()

            with patch("threading.Timer") as mock_timer:
                with patch("builtins.print"):
                    try:
                        server.start(nodes=nodes, edges=edges, open_browser=True)
                    except KeyboardInterrupt:
                        pass

                # Verify Timer was called to open browser
                mock_timer.assert_called_once()
                assert mock_timer.call_args[0][0] == 0.5  # 0.5 second delay
