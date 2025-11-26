from groupcache.groupcache import LRUCache


def test_lru_cache_basic_get_set():
    """Test basic get/set operations"""
    cache = LRUCache(max_size=3)

    # Test set and get
    cache.set("key1", "value1")
    assert cache.get("key1") == "value1"
    assert cache.size() == 1

    # Test get non-existent key
    assert cache.get("key2") is None

    # Test multiple sets
    cache.set("key2", "value2")
    cache.set("key3", "value3")
    assert cache.get("key1") == "value1"
    assert cache.get("key2") == "value2"
    assert cache.get("key3") == "value3"
    assert cache.size() == 3


def test_lru_cache_update_existing_key():
    """Test updating existing keys"""
    cache = LRUCache(max_size=3)

    cache.set("key1", "value1")
    cache.set("key2", "value2")

    # Update existing key
    cache.set("key1", "new_value1")
    assert cache.get("key1") == "new_value1"
    assert cache.size() == 2  # Size should not increase


def test_lru_cache_eviction():
    """Test LRU eviction when cache is full"""
    cache = LRUCache(max_size=3)

    # Fill cache
    cache.set("key1", "value1")
    cache.set("key2", "value2")
    cache.set("key3", "value3")

    # Add one more - should evict key1 (least recently used)
    cache.set("key4", "value4")
    assert cache.get("key1") is None  # Evicted
    assert cache.get("key2") == "value2"
    assert cache.get("key3") == "value3"
    assert cache.get("key4") == "value4"
    assert cache.size() == 3


def test_lru_cache_access_order():
    """Test that accessing items updates their recency"""
    cache = LRUCache(max_size=3)

    cache.set("key1", "value1")
    cache.set("key2", "value2")
    cache.set("key3", "value3")

    # Access key1, making it most recently used
    assert cache.get("key1") == "value1"

    # Add new item - should evict key2 (now least recently used)
    cache.set("key4", "value4")
    assert cache.get("key1") == "value1"  # Still present
    assert cache.get("key2") is None  # Evicted
    assert cache.get("key3") == "value3"
    assert cache.get("key4") == "value4"


def test_lru_cache_stats():
    """Test cache statistics tracking"""
    cache = LRUCache(max_size=3)

    # Initial stats
    stats = cache.stats()
    assert stats["hits"] == 0
    assert stats["misses"] == 0
    assert stats["size"] == 0
    assert stats["max_size"] == 3

    # Generate some hits and misses
    cache.set("key1", "value1")
    cache.get("key1")  # Hit
    cache.get("key2")  # Miss
    cache.get("key1")  # Hit

    stats = cache.stats()
    assert stats["hits"] == 2
    assert stats["misses"] == 1
    assert stats["size"] == 1


def test_lru_cache_empty_cache():
    """Test operations on empty cache"""
    cache = LRUCache(max_size=10)

    assert cache.size() == 0
    assert cache.get("any_key") is None

    stats = cache.stats()
    assert stats["hits"] == 0
    assert stats["misses"] == 1
    assert stats["size"] == 0


def test_lru_cache_single_item():
    """Test cache with max_size=1"""
    cache = LRUCache(max_size=1)

    cache.set("key1", "value1")
    assert cache.get("key1") == "value1"

    # Adding another should evict the only item
    cache.set("key2", "value2")
    assert cache.get("key1") is None
    assert cache.get("key2") == "value2"
    assert cache.size() == 1


def test_lru_cache_eviction_order():
    """Test that eviction follows strict LRU order"""
    cache = LRUCache(max_size=5)

    # Fill the cache completely
    for i in range(5):
        cache.set(f"key{i}", f"value{i}")

    assert cache.size() == 5

    # Access keys in specific order: 0, 2, 4, 1, 3
    # This makes key0 least recently used, then 2, 4, 1, 3 (most recent)
    assert cache.get("key0") == "value0"
    assert cache.get("key2") == "value2"
    assert cache.get("key4") == "value4"
    assert cache.get("key1") == "value1"
    assert cache.get("key3") == "value3"

    # Add new item - should evict key0 (least recently used)
    cache.set("key5", "value5")
    assert cache.get("key0") is None
    assert cache.size() == 5

    # Add another - should evict key2 (next least recently used)
    cache.set("key6", "value6")
    assert cache.get("key2") is None
    assert cache.size() == 5

    # Verify remaining keys are still present
    assert cache.get("key4") == "value4"
    assert cache.get("key1") == "value1"
    assert cache.get("key3") == "value3"
    assert cache.get("key5") == "value5"
    assert cache.get("key6") == "value6"


def test_lru_cache_eviction_with_updates():
    """Test eviction behavior when updating existing keys"""
    cache = LRUCache(max_size=3)

    # Fill cache
    cache.set("a", 1)
    cache.set("b", 2)
    cache.set("c", 3)

    # Update 'a' - moves it to end (most recent)
    cache.set("a", 10)

    # Add new item - should evict 'b' (least recently used)
    cache.set("d", 4)
    assert cache.get("b") is None
    assert cache.get("a") == 10
    assert cache.get("c") == 3
    assert cache.get("d") == 4
    assert cache.size() == 3

    # Update all existing keys
    cache.set("a", 100)
    cache.set("c", 300)
    cache.set("d", 400)

    # Size should remain the same
    assert cache.size() == 3

    # All values should be updated
    assert cache.get("a") == 100
    assert cache.get("c") == 300
    assert cache.get("d") == 400


def test_lru_cache_unlimited_size():
    """Test cache with unlimited size (max_size=None)"""
    cache = LRUCache(max_size=None)

    # Add many items - none should be evicted
    for i in range(1000):
        cache.set(f"key{i}", f"value{i}")

    assert cache.size() == 1000

    # Verify all items are still present
    for i in range(1000):
        assert cache.get(f"key{i}") == f"value{i}"

    # Stats should show all hits
    stats = cache.stats()
    assert stats["hits"] == 1000
    assert stats["misses"] == 0
    assert stats["size"] == 1000
    assert stats["max_size"] is None

    # Update some items
    for i in range(10):
        cache.set(f"key{i}", f"new_value{i}")

    # Size should remain the same
    assert cache.size() == 1000

    # Updated values should be reflected
    for i in range(10):
        assert cache.get(f"key{i}") == f"new_value{i}"


def test_lru_cache_zero_size():
    """Test cache with max_size=0 (edge case)"""
    cache = LRUCache(max_size=0)

    # Should immediately evict anything added
    cache.set("key1", "value1")
    assert cache.get("key1") is None
    assert cache.size() == 0

    # Try multiple adds
    for i in range(10):
        cache.set(f"key{i}", f"value{i}")

    assert cache.size() == 0

    # All gets should miss
    for i in range(10):
        assert cache.get(f"key{i}") is None

    stats = cache.stats()
    assert stats["hits"] == 0
    assert stats["misses"] == 11
    assert stats["size"] == 0


def test_lru_cache_get_set_same_key_repeatedly():
    """Test repeated operations on the same key"""
    cache = LRUCache(max_size=3)

    # Set and get same key multiple times
    for i in range(10):
        cache.set("key", i)
        assert cache.get("key") == i

    # Should only have one entry
    assert cache.size() == 1

    # Add other keys
    cache.set("key2", "value2")
    cache.set("key3", "value3")

    # Original key should still be there (was recently accessed)
    assert cache.get("key") == 9

    # Stats should show many hits
    stats = cache.stats()
    assert stats["hits"] == 11  # 10 from loop + 1 from final get


def test_lru_cache_values_are_preserved():
    """Test that various value types are preserved correctly"""
    cache = LRUCache(max_size=10)

    # Test different value types
    test_values = [
        ("int", 42),
        ("float", 3.14),
        ("str", "hello world"),
        ("list", [1, 2, 3]),
        ("dict", {"a": 1, "b": 2}),
        ("tuple", (1, 2, 3)),
        ("none", None),
        ("bool", True),
    ]

    # Set all values
    for key, value in test_values:
        cache.set(key, value)

    # Verify all values are preserved exactly
    for key, expected_value in test_values:
        actual_value = cache.get(key)
        assert actual_value == expected_value
        # For mutable types, verify it's the same object
        if key in ["list", "dict"]:
            assert actual_value is expected_value
