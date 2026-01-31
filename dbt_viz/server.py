"""Local HTTP server for visualization."""

import http.server
import json
import socketserver
import threading
import webbrowser
from pathlib import Path
from typing import Any


class VisualizationHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP handler that serves the visualization page."""

    graph_data: dict[str, Any] = {}
    center_node: str | None = None

    def do_GET(self) -> None:
        """Handle GET requests."""
        if self.path == "/" or self.path == "/index.html":
            self.send_visualization_page()
        elif self.path == "/data.json":
            self.send_json_data()
        else:
            self.send_error(404, "Not Found")

    def send_visualization_page(self) -> None:
        """Send the HTML visualization page."""
        template_path = Path(__file__).parent / "templates" / "index.html"
        with open(template_path) as f:
            html_content = f.read()

        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(html_content.encode())

    def send_json_data(self) -> None:
        """Send graph data as JSON."""
        data = {
            "nodes": self.graph_data.get("nodes", []),
            "edges": self.graph_data.get("edges", []),
            "centerNode": self.center_node,
        }

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default logging."""
        pass


class VisualizationServer:
    """Server for serving the visualization."""

    def __init__(self, port: int = 8080):
        self.port = port
        self.server: socketserver.TCPServer | None = None
        self.thread: threading.Thread | None = None

    def start(
        self,
        nodes: list[dict[str, Any]],
        edges: list[dict[str, str]],
        center_node: str | None = None,
        open_browser: bool = True,
    ) -> None:
        """Start the server and optionally open browser."""
        # Set graph data on handler class
        VisualizationHandler.graph_data = {"nodes": nodes, "edges": edges}
        VisualizationHandler.center_node = center_node

        # Allow address reuse
        socketserver.TCPServer.allow_reuse_address = True

        try:
            self.server = socketserver.TCPServer(("", self.port), VisualizationHandler)
        except OSError as e:
            if "Address already in use" in str(e):
                raise OSError(
                    f"Port {self.port} is already in use. "
                    f"Use --port to specify a different port."
                ) from e
            raise

        url = f"http://localhost:{self.port}"

        if open_browser:
            # Open browser in a separate thread to not block
            threading.Timer(0.5, lambda: webbrowser.open(url)).start()

        print(f"Serving visualization at {url}")
        print("Press Ctrl+C to stop")

        try:
            self.server.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server...")
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop the server."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.server = None
