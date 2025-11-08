import asyncio
import pytest
import groupcache.groupcache as gc
from groupcache.groupcache import (
    configure_cluster,
    get_cluster,
    cached,
    _make_cache_key,
)


def setup_function():
    """Reset global cluster before each test"""
    gc._global_cluster = None


def test_make_cache_key_basic():
    """Test basic cache key generation"""
    # Simple args
    key1 = _make_cache_key("func", "arg1", "arg2")
    key2 = _make_cache_key("func", "arg1", "arg2")
    assert key1 == key2  # Same args should produce same key

    # Different args
    key3 = _make_cache_key("func", "arg1", "arg3")
    assert key1 != key3

    # Different function names
    key4 = _make_cache_key("other_func", "arg1", "arg2")
    assert key1 != key4


def test_make_cache_key_with_kwargs():
    """Test cache key generation with keyword arguments"""
    key1 = _make_cache_key("func", "arg1", foo="bar", baz="qux")
    key2 = _make_cache_key("func", "arg1", foo="bar", baz="qux")
    assert key1 == key2

    # Different kwargs values
    key4 = _make_cache_key("func", "arg1", foo="different", baz="qux")
    assert key1 != key4


def test_make_cache_key_with_hashable_args():
    """Test cache key generation with hashable arguments"""
    # Tuples (hashable)
    key1 = _make_cache_key("func", (1, 2, 3), "str_arg")
    key2 = _make_cache_key("func", (1, 2, 3), "str_arg")
    assert key1 == key2

    # Different tuple values
    key3 = _make_cache_key("func", (1, 2, 4), "str_arg")
    assert key1 != key3


@pytest.mark.asyncio
async def test_configure_cluster():
    """Test cluster configuration"""
    # Configure without peers
    cluster = await configure_cluster("localhost:8081", auto_start_server=False)
    assert cluster is not None
    assert cluster.self_url == "localhost:8081"
    assert get_cluster() is cluster
    assert len(cluster.consistent_hash.get_nodes()) == 1  # Only self is added

    # Configure with peers
    cluster2 = await configure_cluster(
        "localhost:8081", ["localhost:8082", "localhost:8083"], auto_start_server=False
    )
    assert cluster2 is get_cluster()  # Should return same instance
    assert len(cluster2.consistent_hash.get_nodes()) == 3


def test_get_cluster_before_configure():
    """Test getting cluster before configuration"""
    assert get_cluster() is None


@pytest.mark.asyncio
async def test_cached_decorator_basic():
    """Test basic @cached decorator functionality"""
    # Configure cluster - let decorator create group
    await configure_cluster("localhost:8081", auto_start_server=False)

    call_count = 0

    @cached(group="test_group")
    async def expensive_function(x: int) -> int:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)  # Simulate expensive operation
        return x * 2

    # First call - should execute function
    result1 = await expensive_function(5)
    assert result1 == 10
    assert call_count == 1

    # Second call - should use cache
    result2 = await expensive_function(5)
    assert result2 == 10
    assert call_count == 1  # Should not increment

    # Different argument - should execute function
    result3 = await expensive_function(7)
    assert result3 == 14
    assert call_count == 2


@pytest.mark.asyncio
async def test_cached_decorator_with_kwargs():
    """Test @cached decorator with keyword arguments"""
    await configure_cluster("localhost:8081", auto_start_server=False)

    call_count = 0

    @cached(group="test_group")
    async def greet(name: str, greeting: str = "Hello") -> str:
        nonlocal call_count
        call_count += 1
        return f"{greeting}, {name}!"

    # Call with kwargs
    result1 = await greet("Alice", greeting="Hi")
    assert result1 == "Hi, Alice!"
    assert call_count == 1

    # Same call - should use cache
    result2 = await greet("Alice", greeting="Hi")
    assert result2 == "Hi, Alice!"
    assert call_count == 1

    # Same call again - should use cache
    result3 = await greet("Alice", greeting="Hi")
    assert result3 == "Hi, Alice!"
    assert call_count == 1

    # Different kwargs value - should execute function
    result4 = await greet("Alice", greeting="Hey")
    assert result4 == "Hey, Alice!"
    assert call_count == 2


@pytest.mark.asyncio
async def test_cached_decorator_multiple_functions():
    """Test @cached decorator on multiple functions"""
    await configure_cluster("localhost:8081", auto_start_server=False)

    add_count = 0
    mult_count = 0

    @cached(group="add_group")
    async def add(x: int, y: int) -> int:
        nonlocal add_count
        add_count += 1
        return x + y

    @cached(group="multiply_group")
    async def multiply(x: int, y: int) -> int:
        nonlocal mult_count
        mult_count += 1
        return x * y

    # Test both functions
    assert await add(2, 3) == 5
    assert add_count == 1

    assert await multiply(2, 3) == 6
    assert mult_count == 1

    # Cache should work for each function independently
    assert await add(2, 3) == 5
    assert add_count == 1  # Should use cache

    assert await multiply(2, 3) == 6
    assert mult_count == 1  # Should use cache

    # Different args
    assert await add(3, 4) == 7
    assert add_count == 2


@pytest.mark.asyncio
async def test_cached_decorator_none_values():
    """Test @cached decorator with None return values"""
    await configure_cluster("localhost:8081", auto_start_server=False)

    call_count = 0

    @cached(group="test_group")
    async def maybe_none(x: int):
        nonlocal call_count
        call_count += 1
        if x > 0:
            return x
        return None

    # Function returns None - should not cache
    result1 = await maybe_none(0)
    assert result1 is None
    assert call_count == 1

    # Call again - should execute again (None not cached)
    result2 = await maybe_none(0)
    assert result2 is None
    assert call_count == 2

    # Non-None value should be cached
    result3 = await maybe_none(5)
    assert result3 == 5
    assert call_count == 3

    result4 = await maybe_none(5)
    assert result4 == 5
    assert call_count == 3  # Should use cache


@pytest.mark.asyncio
async def test_cached_decorator_no_cluster_configured():
    """Test @cached decorator error when cluster not configured"""
    # Don't configure cluster

    @cached(group="test_group")
    async def some_function():
        return "value"

    with pytest.raises(RuntimeError, match="GroupCache cluster not configured"):
        await some_function()


@pytest.mark.asyncio
async def test_cached_decorator_auto_creates_group():
    """Test @cached decorator auto-creates group when it doesn't exist"""
    cluster = await configure_cluster("localhost:8081", auto_start_server=False)
    # Verify group doesn't exist initially
    assert "auto_created_group" not in cluster.groups

    @cached(group="auto_created_group")
    async def some_function():
        return "value"

    # First call should auto-create the group
    result = await some_function()
    assert result == "value"
    assert "auto_created_group" in cluster.groups


@pytest.mark.asyncio
async def test_cached_decorator_concurrent_calls():
    """Test @cached decorator with concurrent calls"""
    await configure_cluster("localhost:8081", auto_start_server=False)

    call_count = 0

    @cached(group="test_group")
    async def slow_function(x: int) -> int:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)  # Simulate slow operation
        return x * 2

    # Launch multiple concurrent calls for same key
    tasks = [slow_function(10) for _ in range(5)]
    results = await asyncio.gather(*tasks)

    # All should get same result
    assert all(r == 20 for r in results)

    # Function should only be called once due to singleflight
    assert call_count == 1


@pytest.mark.asyncio
async def test_cached_decorator_with_cluster_peers():
    """Test @cached decorator with peer configuration"""
    cluster = await configure_cluster(
        "localhost:8081", ["localhost:8082", "localhost:8083"], auto_start_server=False
    )

    call_count = 0

    @cached(group="distributed")
    async def distributed_function(key: str) -> str:
        nonlocal call_count
        call_count += 1
        return f"result_{key}"

    # Find a key we own locally
    local_key = None
    for i in range(100):
        test_key = f"key_{i}"
        owner = cluster.consistent_hash.get_node(test_key)
        if owner == cluster.self_url:
            local_key = test_key
            break

    if local_key:
        # Call with local key
        result = await distributed_function(local_key)
        assert result == f"result_{local_key}"
        assert call_count == 1

        # Should be in main cache (we own this key)
        group = cluster.get_group("distributed")
        cache_key = _make_cache_key("distributed_function", local_key)

        # Check that the result was cached somewhere
        main_cached = group.main_cache.get(cache_key)
        hot_cached = group.hot_cache.get(cache_key)

        # Should be in main cache since we own the key
        assert (
            main_cached == f"result_{local_key}" or hot_cached == f"result_{local_key}"
        )


@pytest.mark.asyncio
async def test_cached_decorator_function_metadata():
    """Test that @cached preserves function metadata"""
    await configure_cluster("localhost:8081", auto_start_server=False)

    @cached(group="test_group")
    async def documented_function(x: int) -> int:
        """This function has documentation."""
        return x * 2

    # Check metadata is preserved
    assert documented_function.__name__ == "documented_function"
    assert documented_function.__doc__ == "This function has documentation."

    # Function should still work
    result = await documented_function(5)
    assert result == 10


@pytest.mark.asyncio
async def test_configure_cluster_updates_existing():
    """Test that configure_cluster updates existing cluster"""
    # First configuration
    cluster1 = await configure_cluster("localhost:8081", auto_start_server=False)
    cluster1.create_group("group1", lambda key: f"value_{key}")

    # Second configuration - should replace cluster
    cluster2 = await configure_cluster(
        "localhost:9999", ["peer1", "peer2"], auto_start_server=False
    )

    # Should be different instance
    assert cluster2 is not cluster1
    assert cluster2.self_url == "localhost:9999"
    assert get_cluster() is cluster2

    # Old groups should not exist in new cluster
    with pytest.raises(ValueError, match="does not exist"):
        cluster2.get_group("group1")
