import asyncio
import pytest
from groupcache.groupcache import GroupCacheCluster


def create_test_cluster(self_url="localhost:8081"):
    """Helper to create a cluster for testing"""
    cluster = GroupCacheCluster(self_url)
    return cluster


@pytest.mark.asyncio
async def test_group_initialization():
    """Test GroupCacheGroup initialization"""
    cluster = create_test_cluster()
    group = cluster.create_group("test_group", max_size=1000)

    assert group.name == "test_group"
    assert group.cluster is cluster
    assert group.main_cache.max_size == 1000
    assert group.hot_cache.max_size == 100  # 10% of main cache
    assert hasattr(group, "singleflight")

    # Check initial metrics
    assert group.peer_requests == 0
    assert group.peer_hits == 0
    assert group.main_cache_hits == 0
    assert group.hot_cache_hits == 0


@pytest.mark.asyncio
async def test_group_lookup_cache():
    """Test _lookup_cache method"""
    cluster = create_test_cluster()
    group = cluster.create_group("test_group")

    # Empty cache should return None
    assert group._lookup_cache("key1") is None
    assert group.main_cache_hits == 0
    assert group.hot_cache_hits == 0

    # Add to main cache
    group.main_cache.set("key1", "value1")
    assert group._lookup_cache("key1") == "value1"
    assert group.main_cache_hits == 1
    assert group.hot_cache_hits == 0

    # Add to hot cache
    group.hot_cache.set("key2", "value2")
    assert group._lookup_cache("key2") == "value2"
    assert group.main_cache_hits == 1
    assert group.hot_cache_hits == 1

    # Main cache takes precedence over hot cache
    group.main_cache.set("key3", "main_value")
    group.hot_cache.set("key3", "hot_value")
    assert group._lookup_cache("key3") == "main_value"
    assert group.main_cache_hits == 2
    assert group.hot_cache_hits == 1


@pytest.mark.asyncio
async def test_group_get_cache_hit():
    """Test get() with cache hits"""
    cluster = create_test_cluster()
    group = cluster.create_group("test_group")

    # Pre-populate main cache
    group.main_cache.set("key1", "value1")

    # Get should return cached value
    value = await group.get("key1")
    assert value == "value1"
    assert group.main_cache_hits == 1
    assert group.hot_cache_hits == 0
    assert group.peer_requests == 0

    # Pre-populate hot cache
    group.hot_cache.set("key2", "value2")

    # Get should return hot cached value
    value = await group.get("key2")
    assert value == "value2"
    assert group.main_cache_hits == 1
    assert group.hot_cache_hits == 1
    assert group.peer_requests == 0


@pytest.mark.asyncio
async def test_group_get_cache_miss():
    """Test get() with cache misses"""
    cluster = create_test_cluster()
    group = cluster.create_group("test_group")

    # Get non-existent key should return None
    value = await group.get("missing_key")
    assert value is None
    assert group.main_cache_hits == 0
    assert group.hot_cache_hits == 0

    # For local ownership (no peers), should not make peer requests
    assert group.peer_requests == 0


@pytest.mark.asyncio
async def test_group_set_local_ownership():
    """Test set() for locally owned keys"""
    cluster = create_test_cluster()
    group = cluster.create_group("test_group")

    # With no peers, we own all keys
    await group.set("key1", "value1")

    # Should be in main cache (we're authoritative)
    assert group.main_cache.get("key1") == "value1"
    assert group.hot_cache.get("key1") is None

    # Verify via get
    value = await group.get("key1")
    assert value == "value1"
    assert group.main_cache_hits == 1


@pytest.mark.asyncio
async def test_group_set_remote_ownership():
    """Test set() for remotely owned keys"""
    cluster = create_test_cluster("localhost:8081")
    cluster.set_peers(["localhost:8082", "localhost:8083"])
    group = cluster.create_group("test_group")

    # Find a key that we don't own
    test_key = None
    for i in range(100):
        key = f"key_{i}"
        owner = cluster.consistent_hash.get_node(key)
        if owner != cluster.self_url:
            test_key = key
            break

    assert test_key is not None, "Could not find a key owned by remote peer"

    # Set a remotely owned key
    await group.set(test_key, "remote_value")

    # Should be in hot cache (we're not authoritative)
    assert group.hot_cache.get(test_key) == "remote_value"
    assert group.main_cache.get(test_key) is None


@pytest.mark.asyncio
async def test_group_get_stats():
    """Test get_stats() method"""
    cluster = create_test_cluster()
    group = cluster.create_group("test_group", max_size=100)

    # Initial stats
    stats = group.get_stats()
    assert stats["main_cache"]["size"] == 0
    assert stats["hot_cache"]["size"] == 0
    assert stats["main_cache_hits"] == 0
    assert stats["hot_cache_hits"] == 0
    assert stats["peer_requests"] == 0
    assert stats["peer_hits"] == 0
    assert stats["peer_hit_rate"] == 0
    assert stats["total_cache_hits"] == 0

    # Generate some activity
    group.main_cache.set("key1", "value1")
    group.hot_cache.set("key2", "value2")
    await group.get("key1")  # main cache hit
    await group.get("key2")  # hot cache hit
    await group.get("key3")  # miss

    stats = group.get_stats()
    assert stats["main_cache"]["size"] == 1
    assert stats["hot_cache"]["size"] == 1
    assert stats["main_cache_hits"] == 1
    assert stats["hot_cache_hits"] == 1
    assert stats["total_cache_hits"] == 2


@pytest.mark.asyncio
async def test_group_concurrent_gets():
    """Test concurrent get() calls for same key"""
    cluster = create_test_cluster()
    group = cluster.create_group("test_group")

    call_count = 0

    # Mock the peer client to track calls
    class CountingPeerClient:
        async def get(self, peer_url, group_name, key):
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.05)  # Simulate network delay
            return f"value_for_{key}"

    # Use a key that would go to a peer
    cluster.set_peers(["localhost:8082"])
    cluster.peer_client = CountingPeerClient()  # type: ignore

    # Find a key owned by the peer
    test_key = None
    for i in range(100):
        key = f"key_{i}"
        owner = cluster.consistent_hash.get_node(key)
        if owner != cluster.self_url:
            test_key = key
            break

    # Launch multiple concurrent gets
    assert test_key is not None, "Could not find peer-owned key"
    tasks = [group.get(test_key) for _ in range(5)]
    results = await asyncio.gather(*tasks)

    # All should get same result
    assert all(r == f"value_for_{test_key}" for r in results)

    # Peer should only be called once (singleflight deduplication)
    assert call_count == 1
    assert group.peer_requests == 1


@pytest.mark.asyncio
async def test_group_peer_request_failure():
    """Test behavior when peer request fails"""
    cluster = create_test_cluster("localhost:8081")
    cluster.set_peers(["localhost:8082"])
    group = cluster.create_group("test_group")

    # Mock failing peer client
    class FailingPeerClient:
        async def get(self, peer_url, group_name, key):
            raise Exception("Network error")

    cluster.peer_client = FailingPeerClient()  # type: ignore

    # Find a key owned by peer
    test_key = None
    for i in range(100):
        key = f"key_{i}"
        owner = cluster.consistent_hash.get_node(key)
        if owner != cluster.self_url:
            test_key = key
            break

    # Get should return None when peer fails
    assert test_key is not None, "Could not find peer-owned key"
    value = await group.get(test_key)
    assert value is None
    assert group.peer_requests == 1
    assert group.peer_hits == 0


@pytest.mark.asyncio
async def test_group_cache_isolation():
    """Test that different groups have isolated caches"""
    cluster = create_test_cluster()
    group1 = cluster.create_group("group1")
    group2 = cluster.create_group("group2")

    # Set same key in different groups
    await group1.set("shared_key", "value1")
    await group2.set("shared_key", "value2")

    # Each group should have its own value
    assert await group1.get("shared_key") == "value1"
    assert await group2.get("shared_key") == "value2"

    # Stats should be independent
    assert group1.main_cache_hits == 1
    assert group2.main_cache_hits == 1


@pytest.mark.asyncio
async def test_group_populate_cache():
    """Test _populate_cache helper method"""
    cluster = create_test_cluster()
    group = cluster.create_group("test_group")

    # Populate main cache
    group._populate_cache("key1", "value1", group.main_cache)
    assert group.main_cache.get("key1") == "value1"
    assert group.main_cache.size() == 1

    # Populate hot cache
    group._populate_cache("key2", "value2", group.hot_cache)
    assert group.hot_cache.get("key2") == "value2"
    assert group.hot_cache.size() == 1


@pytest.mark.asyncio
async def test_group_ownership_changes():
    """Test behavior when peer ownership changes"""
    cluster = create_test_cluster("localhost:8081")
    group = cluster.create_group("test_group")

    # Initially no peers - we own everything
    await group.set("key1", "value1")
    assert group.main_cache.get("key1") == "value1"
    assert group.hot_cache.get("key1") is None

    # Add peers - ownership might change
    cluster.set_peers(["localhost:8082", "localhost:8083"])

    # Check where key1 is owned now
    owner = cluster.consistent_hash.get_node("key1")

    # Set it again
    await group.set("key1", "value1_updated")

    if owner == cluster.self_url:
        # Still owned locally
        assert group.main_cache.get("key1") == "value1_updated"
    else:
        # Now owned remotely
        assert group.hot_cache.get("key1") == "value1_updated"
