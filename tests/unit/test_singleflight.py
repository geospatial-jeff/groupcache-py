import asyncio
import pytest
from groupcache.groupcache import ChannelSingleFlight


@pytest.mark.asyncio
async def test_singleflight_initialization():
    """Test ChannelSingleFlight initialization"""
    sf = ChannelSingleFlight()
    assert sf._calls == {}


@pytest.mark.asyncio
async def test_singleflight_single_call():
    """Test single function call"""
    sf = ChannelSingleFlight()

    call_count = 0

    async def test_function():
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)  # Small delay
        return "result"

    result = await sf.do("key1", test_function)

    assert result == "result"
    assert call_count == 1
    assert "key1" not in sf._calls  # Should be cleaned up


@pytest.mark.asyncio
async def test_singleflight_concurrent_same_key():
    """Test that concurrent calls with same key only execute once"""
    sf = ChannelSingleFlight()

    call_count = 0

    async def test_function():
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.05)  # Longer delay to ensure concurrency
        return f"result_{call_count}"

    # Start multiple concurrent calls with same key
    tasks = [asyncio.create_task(sf.do("same_key", test_function)) for _ in range(5)]

    results = await asyncio.gather(*tasks)

    # Function should only be called once
    assert call_count == 1

    # All tasks should get the same result
    assert all(result == "result_1" for result in results)

    # Key should be cleaned up
    assert "same_key" not in sf._calls


@pytest.mark.asyncio
async def test_singleflight_different_keys():
    """Test that different keys execute independently"""
    sf = ChannelSingleFlight()

    call_counts = {"key1": 0, "key2": 0, "key3": 0}

    async def make_function(key):
        async def test_function():
            call_counts[key] += 1
            await asyncio.sleep(0.01)
            return f"result_{key}"

        return test_function

    # Start concurrent calls with different keys
    tasks = [
        asyncio.create_task(sf.do("key1", await make_function("key1"))),
        asyncio.create_task(sf.do("key2", await make_function("key2"))),
        asyncio.create_task(sf.do("key3", await make_function("key3"))),
    ]

    results = await asyncio.gather(*tasks)

    # Each function should be called once
    assert all(count == 1 for count in call_counts.values())

    # Each should get its own result
    assert "result_key1" in results
    assert "result_key2" in results
    assert "result_key3" in results

    # All keys should be cleaned up
    assert len(sf._calls) == 0


@pytest.mark.asyncio
async def test_singleflight_exception_handling():
    """Test that exceptions are properly propagated"""
    sf = ChannelSingleFlight()

    call_count = 0

    async def failing_function():
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        raise ValueError("Test error")

    # Start multiple concurrent calls that will fail
    tasks = [
        asyncio.create_task(sf.do("error_key", failing_function)) for _ in range(3)
    ]

    # All should raise the same exception
    for task in tasks:
        with pytest.raises(ValueError, match="Test error"):
            await task

    # Function should only be called once
    assert call_count == 1

    # Key should be cleaned up even after exception
    assert "error_key" not in sf._calls


@pytest.mark.asyncio
async def test_singleflight_sequential_calls():
    """Test sequential calls with same key execute separately"""
    sf = ChannelSingleFlight()

    call_count = 0

    async def test_function():
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return f"result_{call_count}"

    # First call
    result1 = await sf.do("sequential_key", test_function)
    assert result1 == "result_1"
    assert call_count == 1

    # Second call (after first completes)
    result2 = await sf.do("sequential_key", test_function)
    assert result2 == "result_2"
    assert call_count == 2

    # Both should be different results
    assert result1 != result2


@pytest.mark.asyncio
async def test_singleflight_mixed_concurrent_sequential():
    """Test mix of concurrent and sequential calls"""
    sf = ChannelSingleFlight()

    call_count = 0

    async def test_function():
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.03)
        return f"result_{call_count}"

    # First batch of concurrent calls
    tasks1 = [asyncio.create_task(sf.do("mixed_key", test_function)) for _ in range(3)]
    results1 = await asyncio.gather(*tasks1)

    # Should all be the same (first batch)
    assert all(result == "result_1" for result in results1)
    assert call_count == 1

    # Second batch of concurrent calls (after first batch completes)
    tasks2 = [asyncio.create_task(sf.do("mixed_key", test_function)) for _ in range(2)]
    results2 = await asyncio.gather(*tasks2)

    # Should all be the same (second batch)
    assert all(result == "result_2" for result in results2)
    assert call_count == 2

    # Results from different batches should be different
    assert results1[0] != results2[0]


@pytest.mark.asyncio
async def test_singleflight_return_types():
    """Test that different return types are handled correctly"""
    sf = ChannelSingleFlight()

    # Test different return types
    test_cases = [
        ("string", lambda: "hello"),
        ("int", lambda: 42),
        ("list", lambda: [1, 2, 3]),
        ("dict", lambda: {"key": "value"}),
        ("none", lambda: None),
        ("bool", lambda: True),
    ]

    for key, func in test_cases:

        async def async_func():
            return func()

        result = await sf.do(key, async_func)
        assert result == func()


@pytest.mark.asyncio
async def test_singleflight_cleanup_after_exception():
    """Test that cleanup happens even when exception occurs"""
    sf = ChannelSingleFlight()

    async def failing_function():
        await asyncio.sleep(0.01)
        raise RuntimeError("Cleanup test")

    # Should raise exception
    with pytest.raises(RuntimeError, match="Cleanup test"):
        await sf.do("cleanup_key", failing_function)

    # Key should be cleaned up
    assert "cleanup_key" not in sf._calls

    # Subsequent call with same key should work
    async def working_function():
        return "success"

    result = await sf.do("cleanup_key", working_function)
    assert result == "success"


@pytest.mark.asyncio
async def test_singleflight_high_concurrency():
    """Test high concurrency scenario"""
    sf = ChannelSingleFlight()

    call_count = 0

    async def expensive_function():
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.02)  # Simulate expensive operation
        return "expensive_result"

    # Create many concurrent tasks
    num_tasks = 50
    tasks = [
        asyncio.create_task(sf.do("expensive_key", expensive_function))
        for _ in range(num_tasks)
    ]

    results = await asyncio.gather(*tasks)

    # Function should only be called once despite high concurrency
    assert call_count == 1

    # All tasks should get the same result
    assert all(result == "expensive_result" for result in results)
    assert len(results) == num_tasks

    # Key should be cleaned up
    assert "expensive_key" not in sf._calls
