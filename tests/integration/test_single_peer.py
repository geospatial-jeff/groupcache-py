"""
Integration test for basic GroupCache workflow.

Tests the simplest use case:
1. Cache miss - load from source
2. Cache hit - serve from cache
With a single peer configuration.
"""

import pytest
import asyncio
from groupcache.groupcache import GroupCacheCluster


@pytest.mark.asyncio
async def test_single_peer_cache_miss_hit_workflow():
    """Test basic cache miss -> load -> cache hit workflow with single peer"""

    # Create a simple loader that tracks calls
    load_calls = []

    def user_loader(key: str) -> str:
        load_calls.append(key)
        return f"user_data_for_{key}"

    # Create a single cluster (no peers, just local)
    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    try:
        # Create a cache group
        users_group = cluster.create_group("users", user_loader, max_size=100)

        # Test 1: Cache miss - should load from source
        result1 = await users_group.get("user123")

        assert result1 == "user_data_for_user123"
        assert len(load_calls) == 1
        assert load_calls[0] == "user123"
        assert users_group.source_loads == 1
        assert users_group.main_cache_hits == 0

        # Test 2: Cache hit - should serve from cache (no additional load)
        result2 = await users_group.get("user123")

        assert result2 == "user_data_for_user123"
        assert len(load_calls) == 1  # No additional loads
        assert users_group.source_loads == 1  # Still 1
        assert users_group.main_cache_hits == 1  # Now 1 hit

        # Test 3: Different key - should load from source again
        result3 = await users_group.get("user456")

        assert result3 == "user_data_for_user456"
        assert len(load_calls) == 2
        assert load_calls[1] == "user456"
        assert users_group.source_loads == 2
        assert users_group.main_cache_hits == 1

        # Test 4: Second key cache hit
        result4 = await users_group.get("user456")

        assert result4 == "user_data_for_user456"
        assert len(load_calls) == 2  # No additional loads
        assert users_group.source_loads == 2
        assert users_group.main_cache_hits == 2

        # Verify cache contents
        assert users_group.main_cache.get("user123") == "user_data_for_user123"
        assert users_group.main_cache.get("user456") == "user_data_for_user456"
        assert users_group.main_cache.size() == 2

        # Verify stats
        stats = users_group.get_stats()
        assert stats["source_loads"] == 2
        assert stats["main_cache_hits"] == 2
        assert stats["peer_requests"] == 0  # No peer requests in single node
        assert stats["main_cache"]["size"] == 2

    finally:
        # Cleanup
        await cluster.close()


@pytest.mark.asyncio
async def test_single_peer_async_loader():
    """Test basic workflow with async loader"""

    # Async loader that tracks calls
    load_calls = []

    async def async_user_loader(key: str) -> str:
        # Simulate some async work
        await asyncio.sleep(0.01)
        load_calls.append(key)
        return f"async_user_{key}"

    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    try:
        users_group = cluster.create_group(
            "async_users", async_user_loader, max_size=50
        )

        # Cache miss - should load from async source
        result1 = await users_group.get("async123")

        assert result1 == "async_user_async123"
        assert len(load_calls) == 1
        assert users_group.source_loads == 1

        # Cache hit - should serve from cache
        result2 = await users_group.get("async123")

        assert result2 == "async_user_async123"
        assert len(load_calls) == 1  # No additional loads
        assert users_group.source_loads == 1
        assert users_group.main_cache_hits == 1

    finally:
        await cluster.close()


@pytest.mark.asyncio
async def test_single_peer_loader_returns_none():
    """Test workflow when loader returns None (not found)"""

    def selective_loader(key: str) -> str | None:
        if key.startswith("valid_"):
            return f"data_for_{key}"
        return None  # Not found

    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    try:
        group = cluster.create_group("selective", selective_loader)

        # Test valid key
        result1 = await group.get("valid_user")
        assert result1 == "data_for_valid_user"
        assert group.source_loads == 1

        # Test invalid key (returns None)
        result2 = await group.get("invalid_user")
        assert result2 is None
        assert group.source_loads == 2

        # Cache hit for valid key
        result3 = await group.get("valid_user")
        assert result3 == "data_for_valid_user"
        assert group.source_loads == 2  # No additional load
        assert group.main_cache_hits == 1

        # None values are not cached, so this will load again
        result4 = await group.get("invalid_user")
        assert result4 is None
        assert group.source_loads == 3  # Another load attempt

    finally:
        await cluster.close()


@pytest.mark.asyncio
async def test_single_peer_concurrent_requests():
    """Test concurrent requests for same key (singleflight behavior)"""

    load_calls = []

    async def slow_loader(key: str) -> str:
        # Simulate slow loading
        await asyncio.sleep(0.1)
        load_calls.append(key)
        return f"slow_data_{key}"

    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    try:
        group = cluster.create_group("slow", slow_loader)

        # Start multiple concurrent requests for the same key
        tasks = [
            group.get("slow_key"),
            group.get("slow_key"),
            group.get("slow_key"),
        ]

        results = await asyncio.gather(*tasks)

        # All should get the same result
        assert all(result == "slow_data_slow_key" for result in results)

        # But loader should only be called once (singleflight)
        assert len(load_calls) == 1
        assert group.source_loads == 1

        # Subsequent request should hit cache
        result = await group.get("slow_key")
        assert result == "slow_data_slow_key"
        assert len(load_calls) == 1  # Still only one load
        assert group.main_cache_hits == 1

    finally:
        await cluster.close()


@pytest.mark.asyncio
async def test_single_peer_cache_eviction():
    """Test cache eviction when max_size is reached"""

    def number_loader(key: str) -> str:
        return f"number_{key}"

    cluster = GroupCacheCluster(self_url="http://localhost:8080")

    try:
        # Small cache size to trigger eviction
        group = cluster.create_group("numbers", number_loader, max_size=2)

        # Fill cache to capacity
        await group.get("1")
        await group.get("2")

        assert group.main_cache.size() == 2
        assert group.main_cache.get("1") == "number_1"
        assert group.main_cache.get("2") == "number_2"

        # Add third item - should evict first item
        await group.get("3")

        assert group.main_cache.size() == 2
        assert group.main_cache.get("1") is None  # Evicted
        assert group.main_cache.get("2") == "number_2"
        assert group.main_cache.get("3") == "number_3"

        # Access "1" again - should reload
        original_loads = group.source_loads
        result = await group.get("1")

        assert result == "number_1"
        assert group.source_loads == original_loads + 1  # Had to reload

    finally:
        await cluster.close()
