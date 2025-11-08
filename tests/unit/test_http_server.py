import pytest
from unittest.mock import AsyncMock, Mock, patch
from aiohttp import web
from groupcache.http_server import GroupCacheHTTPServer


@pytest.mark.asyncio
async def test_http_server_initialization():
    """Test HTTP server initialization"""
    server = GroupCacheHTTPServer()
    assert server.base_path == "/_groupcache/"
    assert server.self_url is None
    assert server.app is not None
    assert server.get_handler is None
    assert server.site is None


@pytest.mark.asyncio
async def test_http_server_with_custom_config():
    """Test HTTP server with custom configuration"""
    server = GroupCacheHTTPServer(
        base_path="/custom/", self_url="http://localhost:9999"
    )
    assert server.base_path == "/custom/"
    assert server.self_url == "http://localhost:9999"


@pytest.mark.asyncio
async def test_http_server_set_handler():
    """Test setting request handler"""
    server = GroupCacheHTTPServer()

    async def test_handler(group: str, key: str):
        return f"value_for_{group}_{key}"

    server.set_get_handler(test_handler)
    assert server.get_handler is test_handler


@pytest.mark.asyncio
@patch("aiohttp.web.TCPSite")
@patch("aiohttp.web.AppRunner")
async def test_http_server_start_stop(mock_app_runner, mock_tcp_site):
    """Test starting and stopping HTTP server"""
    server = GroupCacheHTTPServer(self_url="http://localhost:8080")

    # Mock the runner and site
    mock_runner = AsyncMock()
    mock_site = AsyncMock()
    mock_app_runner.return_value = mock_runner
    mock_tcp_site.return_value = mock_site

    async def dummy_handler(group: str, key: str):
        return f"test_value_{key}"

    server.set_get_handler(dummy_handler)

    # Start server
    await server.start()

    # Verify runner setup and site start were called
    mock_app_runner.assert_called_once_with(server.app)
    mock_runner.setup.assert_called_once()
    mock_tcp_site.assert_called_once_with(mock_runner, "localhost", 8080)
    mock_site.start.assert_called_once()
    assert server.site is mock_site

    # Stop server
    await server.stop()

    # Verify cleanup
    mock_site.stop.assert_called_once()
    mock_runner.cleanup.assert_called_once()


@pytest.mark.asyncio
@patch("aiohttp.web.TCPSite")
@patch("aiohttp.web.AppRunner")
async def test_http_server_start_with_host_port_override(
    mock_app_runner, mock_tcp_site
):
    """Test starting server with explicit host and port"""
    server = GroupCacheHTTPServer()

    mock_runner = AsyncMock()
    mock_site = AsyncMock()
    mock_app_runner.return_value = mock_runner
    mock_tcp_site.return_value = mock_site

    await server.start(host="0.0.0.0", port=9000)

    mock_tcp_site.assert_called_once_with(mock_runner, "0.0.0.0", 9000)


@pytest.mark.asyncio
async def test_http_server_handle_cache_request_success():
    """Test successful cache request handling"""
    server = GroupCacheHTTPServer()

    async def test_handler(group: str, key: str):
        return {"id": "123", "name": "Alice"}

    server.set_get_handler(test_handler)

    # Mock request
    request = Mock()
    request.match_info = {"group": "users", "key": "123"}

    response = await server._handle_cache_request(request)

    assert isinstance(response, web.Response)
    assert response.status == 200
    # Parse the response body
    import json

    response_data = json.loads(response.text)
    assert response_data == {"value": {"id": "123", "name": "Alice"}}


@pytest.mark.asyncio
async def test_http_server_handle_cache_request_not_found():
    """Test cache request handling when value not found"""
    server = GroupCacheHTTPServer()

    async def test_handler(group: str, key: str):
        return None

    server.set_get_handler(test_handler)

    request = Mock()
    request.match_info = {"group": "missing", "key": "key"}

    response = await server._handle_cache_request(request)

    assert response.status == 404


@pytest.mark.asyncio
async def test_http_server_handle_cache_request_invalid_format():
    """Test cache request handling with invalid request format"""
    server = GroupCacheHTTPServer()

    # Test missing group
    request = Mock()
    request.match_info = {"group": None, "key": "key"}

    response = await server._handle_cache_request(request)
    assert response.status == 400

    # Test missing key
    request.match_info = {"group": "test", "key": None}

    response = await server._handle_cache_request(request)
    assert response.status == 400


@pytest.mark.asyncio
async def test_http_server_handle_cache_request_no_handler():
    """Test cache request handling when no handler is set"""
    server = GroupCacheHTTPServer()

    request = Mock()
    request.match_info = {"group": "test", "key": "key"}

    response = await server._handle_cache_request(request)

    assert response.status == 500
    import json

    response_data = json.loads(response.text)
    assert "not properly configured" in response_data["error"]


@pytest.mark.asyncio
async def test_http_server_handle_cache_request_handler_exception():
    """Test cache request handling when handler raises exception"""
    server = GroupCacheHTTPServer()

    async def error_handler(group: str, key: str):
        raise ValueError("Test error")

    server.set_get_handler(error_handler)

    request = Mock()
    request.match_info = {"group": "test", "key": "error"}

    response = await server._handle_cache_request(request)

    assert response.status == 500
    import json

    response_data = json.loads(response.text)
    assert "Test error" in response_data["error"]


@pytest.mark.asyncio
async def test_http_server_route_setup():
    """Test that routes are properly configured"""
    server = GroupCacheHTTPServer(base_path="/custom/")

    # Check that routes were added (aiohttp adds both GET and HEAD routes)
    routes = [route for route in server.app.router.routes()]
    assert len(routes) == 2  # GET and HEAD routes

    # Check for GET route
    get_routes = [route for route in routes if route.method == "GET"]
    assert len(get_routes) == 1

    route = get_routes[0]
    # The route pattern should include the base path
    assert "/custom/" in str(route._resource)


@pytest.mark.asyncio
async def test_http_server_url_parsing():
    """Test URL parsing for host and port extraction"""
    # Test with full URL
    server = GroupCacheHTTPServer(self_url="http://example.com:9090")

    with (
        patch("aiohttp.web.TCPSite") as mock_tcp_site,
        patch("aiohttp.web.AppRunner") as mock_app_runner,
    ):
        mock_runner = AsyncMock()
        mock_site = AsyncMock()
        mock_app_runner.return_value = mock_runner
        mock_tcp_site.return_value = mock_site

        await server.start()

        mock_tcp_site.assert_called_once_with(mock_runner, "example.com", 9090)


@pytest.mark.asyncio
async def test_http_server_url_parsing_defaults():
    """Test URL parsing with default values"""
    server = GroupCacheHTTPServer(self_url="http://localhost")

    with (
        patch("aiohttp.web.TCPSite") as mock_tcp_site,
        patch("aiohttp.web.AppRunner") as mock_app_runner,
    ):
        mock_runner = AsyncMock()
        mock_site = AsyncMock()
        mock_app_runner.return_value = mock_runner
        mock_tcp_site.return_value = mock_site

        await server.start()

        # Should use default port 8080 when not specified
        mock_tcp_site.assert_called_once_with(mock_runner, "localhost", 8080)


@pytest.mark.asyncio
async def test_http_server_response_value_formats():
    """Test different value formats in responses"""
    server = GroupCacheHTTPServer()

    test_cases = [
        ("string_value", {"value": "string_value"}),
        ({"dict": "value"}, {"value": {"dict": "value"}}),
        ([1, 2, 3], {"value": [1, 2, 3]}),
        (123, {"value": 123}),
        (True, {"value": True}),
    ]

    for test_value, expected_response in test_cases:

        async def test_handler(group: str, key: str):
            return test_value

        server.set_get_handler(test_handler)

        request = Mock()
        request.match_info = {"group": "test", "key": "key"}

        response = await server._handle_cache_request(request)

        assert response.status == 200
        import json

        response_data = json.loads(response.text)
        assert response_data == expected_response


@pytest.mark.asyncio
@patch("aiohttp.web.TCPSite")
@patch("aiohttp.web.AppRunner")
async def test_http_server_stop_without_start(mock_app_runner, mock_tcp_site):
    """Test stopping server that was never started"""
    server = GroupCacheHTTPServer()

    # Should not error when stopping without starting
    await server.stop()

    # No calls should have been made
    mock_app_runner.assert_not_called()
    mock_tcp_site.assert_not_called()


@pytest.mark.asyncio
async def test_http_server_handler_must_be_async():
    """Test that server expects async handler functions"""
    server = GroupCacheHTTPServer()

    def sync_handler(group: str, key: str):
        return f"sync_{group}_{key}"

    server.set_get_handler(sync_handler)

    request = Mock()
    request.match_info = {"group": "test", "key": "key"}

    # Sync handler should cause an error because server uses 'await'
    response = await server._handle_cache_request(request)

    assert response.status == 500
    import json

    response_data = json.loads(response.text)
    assert "can't be used in 'await' expression" in response_data["error"]
