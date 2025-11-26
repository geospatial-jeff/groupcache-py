from groupcache.groupcache import ConsistentHash


def test_consistent_hash_initialization():
    """Test ConsistentHash initialization"""
    # Default replicas
    ch = ConsistentHash()
    assert ch.replicas == 150
    assert ch.ring == {}
    assert ch.sorted_keys == []

    # Custom replicas
    ch_custom = ConsistentHash(replicas=50)
    assert ch_custom.replicas == 50
    assert ch_custom.ring == {}
    assert ch_custom.sorted_keys == []


def test_consistent_hash_add_node():
    """Test adding nodes to the ring"""
    ch = ConsistentHash(replicas=3)

    # Add first node
    ch.add_node("node1")
    assert len(ch.ring) == 3  # Should have 3 replicas
    assert len(ch.sorted_keys) == 3
    assert "node1" in ch.get_nodes()
    assert len(ch.get_nodes()) == 1

    # Add second node
    ch.add_node("node2")
    assert len(ch.ring) == 6  # Should have 6 replicas total
    assert len(ch.sorted_keys) == 6
    nodes = ch.get_nodes()
    assert "node1" in nodes
    assert "node2" in nodes
    assert len(nodes) == 2


def test_consistent_hash_remove_node():
    """Test removing nodes from the ring"""
    ch = ConsistentHash(replicas=3)

    # Add nodes
    ch.add_node("node1")
    ch.add_node("node2")
    ch.add_node("node3")

    assert len(ch.ring) == 9
    assert len(ch.get_nodes()) == 3

    # Remove one node
    ch.remove_node("node2")
    assert len(ch.ring) == 6
    assert len(ch.sorted_keys) == 6
    nodes = ch.get_nodes()
    assert "node1" in nodes
    assert "node2" not in nodes
    assert "node3" in nodes
    assert len(nodes) == 2

    # Remove all nodes
    ch.remove_node("node1")
    ch.remove_node("node3")
    assert len(ch.ring) == 0
    assert len(ch.sorted_keys) == 0
    assert len(ch.get_nodes()) == 0


def test_consistent_hash_remove_nonexistent_node():
    """Test removing a node that doesn't exist"""
    ch = ConsistentHash(replicas=3)
    ch.add_node("node1")

    initial_size = len(ch.ring)

    # Remove non-existent node - should not crash
    ch.remove_node("nonexistent")

    # Ring should be unchanged
    assert len(ch.ring) == initial_size
    assert "node1" in ch.get_nodes()


def test_consistent_hash_get_node_empty_ring():
    """Test getting node from empty ring"""
    ch = ConsistentHash()

    assert ch.get_node("any_key") is None
    assert ch.get_node("") is None


def test_consistent_hash_get_node_single_node():
    """Test getting node with single node in ring"""
    ch = ConsistentHash(replicas=5)
    ch.add_node("only_node")

    # All keys should map to the only node
    assert ch.get_node("key1") == "only_node"
    assert ch.get_node("key2") == "only_node"
    assert ch.get_node("different_key") == "only_node"
    assert ch.get_node("") == "only_node"


def test_consistent_hash_get_node_multiple_nodes():
    """Test getting node with multiple nodes"""
    ch = ConsistentHash(replicas=5)
    ch.add_node("node1")
    ch.add_node("node2")
    ch.add_node("node3")

    # Test that keys consistently map to nodes
    node1 = ch.get_node("test_key")
    node2 = ch.get_node("test_key")
    assert node1 == node2  # Same key should always map to same node

    # Test different keys
    results = {}
    for i in range(100):
        key = f"key_{i}"
        node = ch.get_node(key)
        assert node in ["node1", "node2", "node3"]
        results[key] = node

    # Verify consistency - same keys should map to same nodes
    for i in range(100):
        key = f"key_{i}"
        assert ch.get_node(key) == results[key]


def test_consistent_hash_get_nodes():
    """Test getting list of all nodes"""
    ch = ConsistentHash(replicas=3)

    # Empty ring
    assert ch.get_nodes() == []

    # Add nodes
    ch.add_node("node1")
    nodes = ch.get_nodes()
    assert len(nodes) == 1
    assert "node1" in nodes

    ch.add_node("node2")
    ch.add_node("node3")
    nodes = ch.get_nodes()
    assert len(nodes) == 3
    assert "node1" in nodes
    assert "node2" in nodes
    assert "node3" in nodes

    # Add duplicate node - should not increase count
    ch.add_node("node1")
    nodes = ch.get_nodes()
    assert len(nodes) == 3  # Still 3 unique nodes


def test_consistent_hash_hash_function():
    """Test the internal hash function"""
    ch = ConsistentHash()

    # Test that hash function is deterministic
    hash1 = ch._hash("test_string")
    hash2 = ch._hash("test_string")
    assert hash1 == hash2

    # Test that different strings produce different hashes
    hash_a = ch._hash("string_a")
    hash_b = ch._hash("string_b")
    assert hash_a != hash_b

    # Test that hashes are integers
    assert isinstance(hash1, int)
    assert isinstance(hash_a, int)
    assert isinstance(hash_b, int)


def test_consistent_hash_ring_wraparound():
    """Test that ring properly wraps around"""
    ch = ConsistentHash(replicas=1)  # Single replica for predictable testing
    ch.add_node("node1")
    ch.add_node("node2")

    # Test with a key - should consistently map to one of the nodes
    node = ch.get_node("test_key")
    assert node in ["node1", "node2"]

    # Test that same key always maps to same node
    node2 = ch.get_node("test_key")
    assert node == node2

    # Test multiple keys map to available nodes
    for i in range(10):
        test_node = ch.get_node(f"key_{i}")
        assert test_node in ["node1", "node2"]


def test_consistent_hash_distribution():
    """Test that keys are reasonably distributed across nodes"""
    ch = ConsistentHash(replicas=50)  # More replicas for better distribution

    # Add several nodes
    nodes = ["node1", "node2", "node3", "node4", "node5"]
    for node in nodes:
        ch.add_node(node)

    # Test many keys and track distribution
    distribution = {}
    num_keys = 1000

    for i in range(num_keys):
        key = f"test_key_{i}"
        node = ch.get_node(key)
        distribution[node] = distribution.get(node, 0) + 1

    # Verify all nodes get some keys
    assert len(distribution) == len(nodes)
    for node in nodes:
        assert node in distribution
        assert distribution[node] > 0

    # Check that distribution is somewhat balanced
    # With 1000 keys and 5 nodes, expect ~200 keys per node
    # Allow for variance but ensure no node gets < 10% or > 40% of keys
    for node, count in distribution.items():
        assert count >= num_keys * 0.1  # At least 10%
        assert count <= num_keys * 0.4  # At most 40%


def test_consistent_hash_node_addition_stability():
    """Test that adding nodes doesn't remapping most keys"""
    ch = ConsistentHash(replicas=10)

    # Start with 2 nodes
    ch.add_node("node1")
    ch.add_node("node2")

    # Record initial key mappings
    initial_mappings = {}
    test_keys = [f"key_{i}" for i in range(100)]

    for key in test_keys:
        initial_mappings[key] = ch.get_node(key)

    # Add a third node
    ch.add_node("node3")

    # Check how many keys were remapped
    remapped_count = 0
    for key in test_keys:
        new_node = ch.get_node(key)
        if new_node != initial_mappings[key]:
            remapped_count += 1

    # In consistent hashing, adding 1 node to 2 nodes should
    # remap approximately 1/3 of keys (not exactly, but roughly)
    # Allow for some variance: between 20% and 50%
    remap_percentage = remapped_count / len(test_keys)
    assert 0.2 <= remap_percentage <= 0.5


def test_consistent_hash_node_removal_stability():
    """Test that removing nodes doesn't remap too many keys"""
    ch = ConsistentHash(replicas=10)

    # Start with 3 nodes
    ch.add_node("node1")
    ch.add_node("node2")
    ch.add_node("node3")

    # Record initial key mappings
    initial_mappings = {}
    test_keys = [f"key_{i}" for i in range(100)]

    for key in test_keys:
        initial_mappings[key] = ch.get_node(key)

    # Remove one node
    ch.remove_node("node2")

    # Check mappings - keys that were on node2 should move,
    # but keys on node1 and node3 should stay
    node2_keys = [k for k, v in initial_mappings.items() if v == "node2"]
    other_keys = [k for k, v in initial_mappings.items() if v != "node2"]

    # All node2 keys should be remapped
    for key in node2_keys:
        new_node = ch.get_node(key)
        assert new_node in ["node1", "node3"]
        assert new_node != "node2"

    # Most other keys should remain unchanged
    unchanged_count = 0
    for key in other_keys:
        if ch.get_node(key) == initial_mappings[key]:
            unchanged_count += 1

    # Most keys not on removed node should stay put
    if other_keys:  # Only test if there were keys on other nodes
        stability_percentage = unchanged_count / len(other_keys)
        assert stability_percentage >= 0.8  # At least 80% should stay


def test_consistent_hash_empty_key():
    """Test behavior with empty and edge case keys"""
    ch = ConsistentHash(replicas=5)
    ch.add_node("node1")
    ch.add_node("node2")

    # Test empty string
    assert ch.get_node("") in ["node1", "node2"]

    # Test whitespace
    assert ch.get_node(" ") in ["node1", "node2"]
    assert ch.get_node("\t") in ["node1", "node2"]
    assert ch.get_node("\n") in ["node1", "node2"]

    # Test special characters
    assert ch.get_node("@#$%^&*()") in ["node1", "node2"]
    assert ch.get_node("unicode: 你好") in ["node1", "node2"]

    # Test very long key
    long_key = "x" * 10000
    assert ch.get_node(long_key) in ["node1", "node2"]


def test_consistent_hash_identical_node_names():
    """Test adding the same node multiple times"""
    ch = ConsistentHash(replicas=3)

    # Add same node multiple times
    ch.add_node("node1")
    ch.add_node("node1")  # Duplicate
    ch.add_node("node1")  # Another duplicate

    # Should still only have replicas for one instance
    assert len(ch.ring) == 3
    assert len(ch.get_nodes()) == 1
    assert ch.get_nodes()[0] == "node1"

    # Keys should still map consistently
    assert ch.get_node("test") == "node1"


def test_consistent_hash_sorted_keys_ordering():
    """Test that sorted_keys maintains proper ordering"""
    ch = ConsistentHash(replicas=3)

    # Add nodes and verify sorted_keys is always sorted
    ch.add_node("node1")
    assert ch.sorted_keys == sorted(ch.sorted_keys)

    ch.add_node("node2")
    assert ch.sorted_keys == sorted(ch.sorted_keys)
    assert len(ch.sorted_keys) == 6

    ch.add_node("node3")
    assert ch.sorted_keys == sorted(ch.sorted_keys)
    assert len(ch.sorted_keys) == 9

    # Remove nodes and verify ordering is maintained
    ch.remove_node("node2")
    assert ch.sorted_keys == sorted(ch.sorted_keys)
    assert len(ch.sorted_keys) == 6

    # Verify all keys in sorted_keys exist in ring
    for key in ch.sorted_keys:
        assert key in ch.ring


def test_consistent_hash_zero_replicas():
    """Test edge case with zero replicas"""
    ch = ConsistentHash(replicas=0)

    # Adding nodes with 0 replicas should result in empty ring
    ch.add_node("node1")
    assert len(ch.ring) == 0
    assert len(ch.sorted_keys) == 0
    assert len(ch.get_nodes()) == 0

    # Getting nodes should return None
    assert ch.get_node("any_key") is None
