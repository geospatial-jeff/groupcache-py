import pytest
from unittest.mock import AsyncMock, Mock, patch
from groupcache.groupcache import GroupCacheCluster, GroupCacheGroup


def dummy_loader(key: str) -> str:
    """Simple test loader"""
    return f"value_for_{key}"


def test_cluster_initialization():
    """Test GroupCacheCluster initialization"""
    cluster = GroupCacheCluster("localhost:8081")

    assert cluster.self_url == "localhost:8081"
    assert cluster.groups == {}
    assert isinstance(cluster.consistent_hash, object)
    assert hasattr(cluster, "peer_client")
    assert len(cluster.consistent_hash.get_nodes()) == 0


def test_cluster_set_peers():
    """Test setting cluster peers"""
    cluster = GroupCacheCluster("localhost:8081")

    # Test with empty peer list
    cluster.set_peers([])
    nodes = cluster.consistent_hash.get_nodes()
    assert len(nodes) == 1
    assert "localhost:8081" in nodes

    # Test with peer list
    peers = ["localhost:8082", "localhost:8083"]
    cluster.set_peers(peers)
    nodes = cluster.consistent_hash.get_nodes()
    assert len(nodes) == 3
    assert "localhost:8081" in nodes
    assert "localhost:8082" in nodes
    assert "localhost:8083" in nodes

    # Test updating peers
    new_peers = ["localhost:8084", "localhost:8085"]
    cluster.set_peers(new_peers)
    nodes = cluster.consistent_hash.get_nodes()
    assert len(nodes) == 3
    assert "localhost:8081" in nodes
    assert "localhost:8084" in nodes
    assert "localhost:8085" in nodes
    # Old peers should be removed
    assert "localhost:8082" not in nodes
    assert "localhost:8083" not in nodes


def test_cluster_create_group():
    """Test creating cache groups"""
    cluster = GroupCacheCluster("localhost:8081")

    # Test creating first group
    group = cluster.create_group("users", dummy_loader, max_size=1000)
    assert isinstance(group, GroupCacheGroup)
    assert group.name == "users"
    assert group.cluster is cluster
    assert "users" in cluster.groups
    assert cluster.groups["users"] is group

    # Test creating second group
    products_group = cluster.create_group("products", dummy_loader, max_size=2000)
    assert products_group.name == "products"
    assert "products" in cluster.groups
    assert len(cluster.groups) == 2

    # Test creating group with duplicate name should raise error
    with pytest.raises(ValueError, match="already exists.*get_group"):
        cluster.create_group("users", dummy_loader)


def test_cluster_get_group():
    """Test getting existing cache groups"""
    cluster = GroupCacheCluster("localhost:8081")

    # Test getting non-existent group should raise error
    with pytest.raises(ValueError, match="does not exist.*create_group"):
        cluster.get_group("nonexistent")

    # Create group and test getting it
    original_group = cluster.create_group("test_group", dummy_loader)
    retrieved_group = cluster.get_group("test_group")

    assert retrieved_group is original_group
    assert retrieved_group.name == "test_group"


def test_cluster_get_stats():
    """Test getting cluster statistics"""
    cluster = GroupCacheCluster("localhost:8081")

    # Test stats with no peers or groups
    stats = cluster.get_stats()
    assert stats["peers"] == 0
    assert stats["groups"] == 0
    assert stats["self_url"] == "localhost:8081"

    # Add peers and test stats
    cluster.set_peers(["localhost:8082", "localhost:8083"])
    stats = cluster.get_stats()
    assert stats["peers"] == 3  # self + 2 peers
    assert stats["groups"] == 0

    # Add groups and test stats
    cluster.create_group("users", dummy_loader)
    cluster.create_group("products", dummy_loader)
    stats = cluster.get_stats()
    assert stats["peers"] == 3
    assert stats["groups"] == 2
    assert "group_users" in stats
    assert "group_products" in stats

    # Verify group stats are included
    assert isinstance(stats["group_users"], dict)
    assert isinstance(stats["group_products"], dict)


def test_cluster_multiple_operations():
    """Test cluster with multiple operations"""
    cluster = GroupCacheCluster("node1:8081")

    # Set up cluster
    cluster.set_peers(["node2:8082", "node3:8083"])
    users_group = cluster.create_group("users", dummy_loader, max_size=1000)
    products_group = cluster.create_group("products", dummy_loader, max_size=500)

    # Verify everything is set up correctly
    assert len(cluster.consistent_hash.get_nodes()) == 3
    assert len(cluster.groups) == 2

    # Verify we can get groups
    assert cluster.get_group("users") is users_group
    assert cluster.get_group("products") is products_group

    # Verify stats include everything
    stats = cluster.get_stats()
    assert stats["peers"] == 3
    assert stats["groups"] == 2
    assert stats["self_url"] == "node1:8081"
    assert "group_users" in stats
    assert "group_products" in stats


def test_cluster_peer_operations():
    """Test cluster peer management operations"""
    cluster = GroupCacheCluster("main:8081")

    # Start with no peers
    assert len(cluster.consistent_hash.get_nodes()) == 0

    # Add peers multiple times
    cluster.set_peers(["peer1:8082"])
    assert len(cluster.consistent_hash.get_nodes()) == 2  # main + peer1

    cluster.set_peers(["peer1:8082", "peer2:8083"])
    assert len(cluster.consistent_hash.get_nodes()) == 3  # main + peer1 + peer2

    # Remove all peers by setting empty list
    cluster.set_peers([])
    nodes = cluster.consistent_hash.get_nodes()
    assert len(nodes) == 1
    assert "main:8081" in nodes


def test_cluster_group_isolation():
    """Test that groups are properly isolated"""
    cluster = GroupCacheCluster("localhost:8081")

    # Create groups with different settings
    group1 = cluster.create_group("group1", dummy_loader, max_size=100)
    group2 = cluster.create_group("group2", dummy_loader, max_size=200)

    # Verify they have different configurations
    assert group1.main_cache.max_size == 100
    assert group2.main_cache.max_size == 200

    # Verify they reference the same cluster
    assert group1.cluster is cluster
    assert group2.cluster is cluster

    # Verify they have separate singleflight instances
    assert group1.singleflight is not group2.singleflight

    # Verify they have separate cache instances
    assert group1.main_cache is not group2.main_cache
    assert group1.hot_cache is not group2.hot_cache


def test_cluster_default_max_size():
    """Test cluster with default max_size for groups"""
    cluster = GroupCacheCluster("localhost:8081")

    # Create group without specifying max_size
    group = cluster.create_group("default_group", dummy_loader)

    # Should use default value of 10000
    assert group.main_cache.max_size == 10000
    assert group.hot_cache.max_size == 1000  # 10% of main cache


# HTTP Unit Tests


@pytest.mark.asyncio
@patch("groupcache.groupcache.GroupCacheHTTPServer")
@patch("groupcache.groupcache.GroupCacheHTTPClient")
async def test_cluster_http_components_initialization(
    mock_client_class, mock_server_class
):
    """Test cluster initializes HTTP components with correct parameters"""
    mock_server = Mock()
    mock_client = Mock()
    mock_server_class.return_value = mock_server
    mock_client_class.return_value = mock_client

    cluster = GroupCacheCluster(self_url="http://localhost:8080", base_path="/custom/")

    # Verify HTTP server was created with correct parameters
    mock_server_class.assert_called_once_with("/custom/", "http://localhost:8080")
    assert cluster.http_server is mock_server

    # Verify HTTP client was created with correct parameters
    mock_client_class.assert_called_once_with("/custom/")
    assert cluster.peer_client is mock_client


@pytest.mark.asyncio
async def test_cluster_start_http_server_sets_handler():
    """Test starting HTTP server sets the correct handler"""
    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    # Mock the server start method
    cluster.http_server.start = AsyncMock()

    await cluster.start_http_server()

    # Verify handler was set and server was started
    assert cluster.http_server.get_handler == cluster._handle_peer_request
    cluster.http_server.start.assert_called_once()
    assert cluster._server_started


@pytest.mark.asyncio
async def test_cluster_start_http_server_idempotent():
    """Test starting HTTP server multiple times is safe"""
    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    # Mock the server start method
    cluster.http_server.start = AsyncMock()
    cluster._server_started = True

    await cluster.start_http_server()

    # Should not call start again
    cluster.http_server.start.assert_not_called()


@pytest.mark.asyncio
async def test_cluster_close_calls_cleanup():
    """Test cluster close calls cleanup on components"""
    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    # Mock the cleanup methods
    cluster.peer_client.close = AsyncMock()
    cluster.http_server.stop = AsyncMock()
    cluster._server_started = True

    await cluster.close()

    # Verify cleanup was called
    cluster.peer_client.close.assert_called_once()
    cluster.http_server.stop.assert_called_once()
    assert not cluster._server_started


@pytest.mark.asyncio
async def test_cluster_handle_peer_request_unknown_group():
    """Test peer request handler returns None for unknown groups"""
    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    result = await cluster._handle_peer_request("unknown_group", "test_key")

    assert result is None


@pytest.mark.asyncio
async def test_cluster_handle_peer_request_cache_hit():
    """Test peer request handler serves from cache"""
    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    # Create group and populate cache
    group = cluster.create_group("test_group", dummy_loader)
    group.main_cache.set("test_key", "cached_value")

    result = await cluster._handle_peer_request("test_group", "test_key")

    assert result == "cached_value"
    assert group.main_cache_hits == 1


@pytest.mark.asyncio
async def test_cluster_handle_peer_request_cache_miss_loads():
    """Test peer request handler loads from source on cache miss"""
    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    # Mock loader
    mock_loader = Mock(return_value="loaded_value")
    group = cluster.create_group("test_group", mock_loader)

    result = await cluster._handle_peer_request("test_group", "test_key")

    assert result == "loaded_value"
    assert group.source_loads == 1
    mock_loader.assert_called_once_with("test_key")
    # Should cache the result
    assert group.main_cache.get("test_key") == "loaded_value"


@pytest.mark.asyncio
async def test_cluster_handle_peer_request_async_loader():
    """Test peer request handler works with async loaders"""
    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    # Mock async loader
    mock_loader = AsyncMock(return_value="async_value")
    group = cluster.create_group("test_group", mock_loader)

    result = await cluster._handle_peer_request("test_group", "test_key")

    assert result == "async_value"
    assert group.source_loads == 1
    mock_loader.assert_called_once_with("test_key")


@pytest.mark.asyncio
async def test_cluster_handle_peer_request_loader_exception():
    """Test peer request handler handles loader exceptions"""
    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    # Mock failing loader
    mock_loader = Mock(side_effect=ValueError("Load failed"))
    group = cluster.create_group("test_group", mock_loader)

    result = await cluster._handle_peer_request("test_group", "test_key")

    assert result is None
    assert group.source_loads == 1
    mock_loader.assert_called_once_with("test_key")


@pytest.mark.asyncio
@patch("groupcache.groupcache.ConsistentHash.get_node")
async def test_group_uses_peer_client_for_remote_keys(mock_get_node):
    """Test group uses HTTP client for keys owned by peers"""
    cluster = GroupCacheCluster(self_url="http://localhost:8080")
    cluster.set_peers(["http://peer1:8080"])

    # Mock consistent hash to return peer URL
    mock_get_node.return_value = "http://peer1:8080"

    # Mock the HTTP client
    cluster.peer_client.get = AsyncMock(return_value="peer_value")

    group = cluster.create_group("test_group", dummy_loader)

    result = await group.get("remote_key")

    assert result == "peer_value"
    assert group.peer_requests == 1
    assert group.peer_hits == 1
    cluster.peer_client.get.assert_called_once_with(
        "http://peer1:8080", "test_group", "remote_key"
    )


@pytest.mark.asyncio
@patch("groupcache.groupcache.ConsistentHash.get_node")
async def test_group_uses_local_loader_for_owned_keys(mock_get_node):
    """Test group uses local loader for keys it owns"""
    cluster = GroupCacheCluster(self_url="http://localhost:8080")
    cluster.set_peers(["http://peer1:8080"])

    # Mock consistent hash to return self URL
    mock_get_node.return_value = "http://localhost:8080"

    # Mock the HTTP client (should not be called)
    cluster.peer_client.get = AsyncMock()

    # Mock loader
    mock_loader = Mock(return_value="local_value")
    group = cluster.create_group("test_group", mock_loader)

    result = await group.get("local_key")

    assert result == "local_value"
    assert group.peer_requests == 0  # No peer requests
    assert group.source_loads == 1
    cluster.peer_client.get.assert_not_called()
    mock_loader.assert_called_once_with("local_key")


@pytest.mark.asyncio
@patch("groupcache.groupcache.ConsistentHash.get_node")
async def test_group_fallback_on_peer_failure(mock_get_node):
    """Test group falls back to local loader when peer request fails"""
    cluster = GroupCacheCluster(self_url="http://localhost:8080")
    cluster.set_peers(["http://peer1:8080"])

    # Mock consistent hash to return peer URL
    mock_get_node.return_value = "http://peer1:8080"

    # Mock HTTP client to fail
    cluster.peer_client.get = AsyncMock(side_effect=Exception("Network error"))

    # Mock loader for fallback
    mock_loader = Mock(return_value="fallback_value")
    group = cluster.create_group("test_group", mock_loader)

    result = await group.get("failing_key")

    assert result == "fallback_value"
    assert group.peer_requests == 1
    assert group.peer_hits == 0
    assert group.source_loads == 1
    cluster.peer_client.get.assert_called_once_with(
        "http://peer1:8080", "test_group", "failing_key"
    )
    mock_loader.assert_called_once_with("failing_key")


@pytest.mark.asyncio
@patch("groupcache.groupcache.ConsistentHash.get_node")
async def test_group_hot_cache_population(mock_get_node):
    """Test group populates hot cache for peer values"""
    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    # Mock consistent hash to return peer URL
    mock_get_node.return_value = "http://peer1:8080"

    # Mock HTTP client
    cluster.peer_client.get = AsyncMock(return_value="peer_value")

    # Mock random to always trigger hot cache population (10% chance normally)
    with patch("random.randint", return_value=1):  # 1 out of 10 triggers population
        group = cluster.create_group("test_group", dummy_loader)

        result = await group.get("hot_key")

        assert result == "peer_value"
        assert group.peer_hits == 1
        # Should be in hot cache now
        assert group.hot_cache.get("hot_key") == "peer_value"


@pytest.mark.asyncio
@patch("groupcache.groupcache.ConsistentHash.get_node")
async def test_group_peer_not_found_returns_none(mock_get_node):
    """Test group handles peer returning None"""
    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    # Mock consistent hash to return peer URL
    mock_get_node.return_value = "http://peer1:8080"

    # Mock HTTP client to return None
    cluster.peer_client.get = AsyncMock(return_value=None)

    # Mock loader to also return None
    mock_loader = Mock(return_value=None)
    group = cluster.create_group("test_group", mock_loader)

    result = await group.get("missing_key")

    assert result is None
    assert group.peer_requests == 1
    assert group.peer_hits == 0
    cluster.peer_client.get.assert_called_once_with(
        "http://peer1:8080", "test_group", "missing_key"
    )
    mock_loader.assert_called_once_with("missing_key")
