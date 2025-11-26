"""
HTTP server for GroupCache peer-to-peer communication.

This server runs in the background and handles cache requests from other peers.
Protocol: GET /_groupcache/{group}/{key} returns JSON {"value": <cached_value>}
"""

import logging
from typing import Callable, Any
from aiohttp import web

logger = logging.getLogger(__name__)


class GroupCacheHTTPServer:
    """HTTP server that handles groupcache peer requests"""

    def __init__(self, base_path: str = "/_groupcache/", self_url: str | None = None):
        self.base_path = base_path
        self.self_url = self_url
        self.app = web.Application()
        self.runner = None
        self.site = None

        # This will be set by the cluster when integrated
        self.get_handler: Callable[[str, str], Any] | None = None

        # Setup routes
        self._setup_routes()

    def _setup_routes(self):
        """Setup HTTP routes for the server"""
        # Route pattern: /_groupcache/{group}/{key}
        self.app.router.add_get(
            self.base_path + "{group}/{key:.*}", self._handle_cache_request
        )

    async def _handle_cache_request(self, request: web.Request) -> web.Response:
        """Handle GET request for a cache key"""
        group_name = request.match_info.get("group")
        key = request.match_info.get("key")

        if not group_name or not key:
            return web.json_response({"error": "Invalid request format"}, status=400)

        # Log the request
        logger.debug(f"Peer request: group={group_name}, key={key}")

        # Use the handler to get the value
        if not self.get_handler:
            return web.json_response(
                {"error": "Server not properly configured"}, status=500
            )

        try:
            # The handler should:
            # 1. Check if the group exists
            # 2. Load from mainCache if present
            # 3. Load from source if not in cache (and we own the key)
            # 4. Never request from another peer (to avoid loops)
            value = await self.get_handler(group_name, key)

            if value is not None:
                # Successfully got the value
                return web.json_response({"value": value})
            else:
                # Key not found or we don't own it
                return web.json_response({"error": "Key not found"}, status=404)

        except Exception as e:
            logger.error(f"Error handling peer request: {e}")
            return web.json_response({"error": str(e)}, status=500)

    def set_get_handler(self, handler: Callable[[str, str], Any]):
        """Set the handler function that retrieves values for peer requests"""
        self.get_handler = handler

    async def start(self, host: str | None = None, port: int | None = None):
        """Start the HTTP server"""
        # Auto-parse host and port from self_url if not provided
        if (host is None or port is None) and self.self_url:
            from urllib.parse import urlparse

            parsed = urlparse(self.self_url)
            host = host or parsed.hostname or "0.0.0.0"
            port = port or parsed.port or 8080
        else:
            host = host or "0.0.0.0"
            port = port or 8080

        self.runner = web.AppRunner(self.app)
        await self.runner.setup()

        self.site = web.TCPSite(self.runner, host, port)
        await self.site.start()

        logger.info(f"GroupCache HTTP server started on {host}:{port}")

    async def stop(self):
        """Stop the HTTP server gracefully"""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()

        logger.info("GroupCache HTTP server stopped")
