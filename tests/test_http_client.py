import pytest
import aiohttp
from unittest.mock import AsyncMock, patch
from groupcache.http_client import GroupCacheHTTPClient


@pytest.mark.asyncio
async def test_http_client_initialization():
    """Test HTTP client initialization"""
    client = GroupCacheHTTPClient()
    assert client.base_path == "/_groupcache/"
    assert client._session is None
    assert client.timeout.total == 5.0
    assert client.max_retries == 1

    # Test custom initialization
    client2 = GroupCacheHTTPClient(base_path="/custom/", timeout=10.0, max_retries=3)
    assert client2.base_path == "/custom/"
    assert client2.timeout.total == 10.0
    assert client2.max_retries == 3


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_successful_get(mock_get):
    """Test successful GET request"""
    client = GroupCacheHTTPClient()

    # Mock the response context manager
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(
        return_value={"value": {"id": "123", "name": "Alice"}}
    )

    mock_context = AsyncMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_response)
    mock_context.__aexit__ = AsyncMock(return_value=None)
    mock_get.return_value = mock_context

    result = await client.get("http://localhost:8090", "users", "123")

    assert result == {"id": "123", "name": "Alice"}
    mock_get.assert_called_once_with("http://localhost:8090/_groupcache/users/123")
    await client.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_not_found(mock_get):
    """Test client handling of 404 responses"""
    client = GroupCacheHTTPClient()

    mock_response = AsyncMock()
    mock_response.status = 404

    mock_context = AsyncMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_response)
    mock_context.__aexit__ = AsyncMock(return_value=None)
    mock_get.return_value = mock_context

    result = await client.get("http://localhost:8090", "missing", "key")

    assert result is None
    mock_get.assert_called_once_with("http://localhost:8090/_groupcache/missing/key")
    await client.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_server_error(mock_get):
    """Test client handling of server errors"""
    client = GroupCacheHTTPClient()

    mock_response = AsyncMock()
    mock_response.status = 500
    mock_response.text = AsyncMock(return_value="Internal Server Error")

    mock_context = AsyncMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_response)
    mock_context.__aexit__ = AsyncMock(return_value=None)
    mock_get.return_value = mock_context

    result = await client.get("http://localhost:8090", "error", "key")

    assert result is None
    # Client retries on server errors (initial + 1 retry = 2 calls)
    assert mock_get.call_count == 2
    mock_get.assert_called_with("http://localhost:8090/_groupcache/error/key")
    await client.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_network_error(mock_get):
    """Test client handling of network errors"""
    client = GroupCacheHTTPClient()

    mock_get.side_effect = aiohttp.ClientConnectionError("Connection failed")

    result = await client.get("http://localhost:8090", "test", "key")

    assert result is None
    # Client retries on network errors (initial + 1 retry = 2 calls)
    assert mock_get.call_count == 2
    mock_get.assert_called_with("http://localhost:8090/_groupcache/test/key")
    await client.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_json_decode_error(mock_get):
    """Test client handling of malformed JSON responses"""
    client = GroupCacheHTTPClient()

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(side_effect=Exception("JSON decode error"))

    mock_context = AsyncMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_response)
    mock_context.__aexit__ = AsyncMock(return_value=None)
    mock_get.return_value = mock_context

    result = await client.get("http://localhost:8090", "test", "key")

    assert result is None
    await client.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_retries(mock_get):
    """Test client retry logic"""
    client = GroupCacheHTTPClient(max_retries=2)

    # First call fails with 500, second call succeeds
    mock_response_fail = AsyncMock()
    mock_response_fail.status = 500
    mock_response_fail.text = AsyncMock(return_value="Server Error")

    mock_response_success = AsyncMock()
    mock_response_success.status = 200
    mock_response_success.json = AsyncMock(return_value={"value": "success"})

    # Create context managers for each call
    ctx1 = AsyncMock()
    ctx1.__aenter__ = AsyncMock(return_value=mock_response_fail)
    ctx1.__aexit__ = AsyncMock(return_value=None)

    ctx2 = AsyncMock()
    ctx2.__aenter__ = AsyncMock(return_value=mock_response_success)
    ctx2.__aexit__ = AsyncMock(return_value=None)

    mock_get.side_effect = [ctx1, ctx2]

    result = await client.get("http://localhost:8090", "test", "key")

    assert result == "success"
    assert mock_get.call_count == 2
    await client.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_max_retries_exceeded(mock_get):
    """Test client behavior when max retries is exceeded"""
    client = GroupCacheHTTPClient(max_retries=1)

    mock_response = AsyncMock()
    mock_response.status = 500
    mock_response.text = AsyncMock(return_value="Server Error")

    mock_context = AsyncMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_response)
    mock_context.__aexit__ = AsyncMock(return_value=None)
    mock_get.return_value = mock_context

    result = await client.get("http://localhost:8090", "test", "key")

    assert result is None
    assert mock_get.call_count == 2  # Initial + 1 retry
    await client.close()


@pytest.mark.asyncio
async def test_http_client_ensure_session():
    """Test session creation and management"""
    client = GroupCacheHTTPClient()

    # Initially no session
    assert client._session is None

    # First call creates session
    session1 = await client._ensure_session()
    assert session1 is not None
    assert client._session is session1
    assert isinstance(session1, aiohttp.ClientSession)

    # Second call returns same session
    session2 = await client._ensure_session()
    assert session2 is session1

    await client.close()


@pytest.mark.asyncio
async def test_http_client_ensure_session_closed():
    """Test session recreation when session is closed"""
    client = GroupCacheHTTPClient()

    # Create and close first session
    session1 = await client._ensure_session()
    await session1.close()

    # Ensure session should create a new one
    session2 = await client._ensure_session()
    assert session2 is not session1
    assert not session2.closed

    await client.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_url_construction(mock_get):
    """Test URL construction with different base paths"""
    client = GroupCacheHTTPClient()

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"value": "test"})

    mock_context = AsyncMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_response)
    mock_context.__aexit__ = AsyncMock(return_value=None)
    mock_get.return_value = mock_context

    await client.get("http://localhost:8080", "test_group", "test_key")

    mock_get.assert_called_once_with(
        "http://localhost:8080/_groupcache/test_group/test_key"
    )
    await client.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_custom_base_path(mock_get):
    """Test client with custom base path"""
    client = GroupCacheHTTPClient(base_path="/api/cache/")

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"value": "test"})

    mock_context = AsyncMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_response)
    mock_context.__aexit__ = AsyncMock(return_value=None)
    mock_get.return_value = mock_context

    await client.get("http://localhost:8080", "test", "key")

    mock_get.assert_called_once_with("http://localhost:8080/api/cache/test/key")
    await client.close()


@pytest.mark.asyncio
async def test_http_client_close():
    """Test client cleanup"""
    client = GroupCacheHTTPClient()

    # Create a session
    session = await client._ensure_session()
    assert not session.closed

    # Close should close the session
    await client.close()
    assert session.closed


@pytest.mark.asyncio
async def test_http_client_close_without_session():
    """Test closing client without creating session"""
    client = GroupCacheHTTPClient()

    # Should not error when closing without session
    await client.close()
    assert client._session is None


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_response_value_extraction(mock_get):
    """Test extracting value from response JSON"""
    client = GroupCacheHTTPClient()

    test_cases = [
        # (response_data, expected_result)
        ({"value": "simple_string"}, "simple_string"),
        ({"value": {"nested": "object"}}, {"nested": "object"}),
        ({"value": None}, None),
        ({"value": 123}, 123),
        ({"value": [1, 2, 3]}, [1, 2, 3]),
        ({"other": "field"}, None),  # No 'value' key
        ({}, None),  # Empty response
    ]

    for i, (response_data, expected_result) in enumerate(test_cases):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=response_data)

        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        mock_get.return_value = mock_context

        result = await client.get("http://test", "group", f"key{i}")
        assert result == expected_result

        # Reset mock for next iteration
        mock_get.reset_mock()

    await client.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_concurrent_requests(mock_get):
    """Test client handling multiple concurrent requests"""
    client = GroupCacheHTTPClient()

    # Create responses for 3 concurrent requests
    contexts = []
    for i in range(3):
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"value": f"value_{i}"})

        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        contexts.append(mock_context)

    mock_get.side_effect = contexts

    # Make concurrent requests
    import asyncio

    tasks = [client.get("http://test", "group", f"key{i}") for i in range(3)]
    results = await asyncio.gather(*tasks)

    # All should succeed with expected values
    assert len(results) == 3
    for i, result in enumerate(results):
        assert result == f"value_{i}"

    # Session should have been called 3 times
    assert mock_get.call_count == 3
    await client.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_unexpected_exception(mock_get):
    """Test client handling of unexpected exceptions"""
    client = GroupCacheHTTPClient()

    # Simulate an unexpected exception during request
    mock_get.side_effect = ValueError("Unexpected error")

    result = await client.get("http://localhost:8090", "test", "key")

    assert result is None
    # Should not retry on unexpected exceptions
    mock_get.assert_called_once()
    await client.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_timeout_configuration(mock_get):
    """Test client timeout configuration"""
    client = GroupCacheHTTPClient(timeout=15.0)

    # Verify timeout is properly configured
    session = await client._ensure_session()
    assert session.timeout.total == 15.0

    await client.close()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test_http_client_max_retries_configuration(mock_get):
    """Test client max_retries configuration"""
    client = GroupCacheHTTPClient(max_retries=5)
    assert client.max_retries == 5

    # Test that it actually uses the configured max_retries
    mock_response = AsyncMock()
    mock_response.status = 500
    mock_response.text = AsyncMock(return_value="Server Error")

    mock_context = AsyncMock()
    mock_context.__aenter__ = AsyncMock(return_value=mock_response)
    mock_context.__aexit__ = AsyncMock(return_value=None)
    mock_get.return_value = mock_context

    result = await client.get("http://localhost:8090", "test", "key")

    assert result is None
    assert mock_get.call_count == 6  # Initial + 5 retries
    await client.close()
