import pytest
from groupcache.groupcache import GroupCacheCluster, GroupCacheGroup


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
    group = cluster.create_group("users", max_size=1000)
    assert isinstance(group, GroupCacheGroup)
    assert group.name == "users"
    assert group.cluster is cluster
    assert "users" in cluster.groups
    assert cluster.groups["users"] is group

    # Test creating second group
    products_group = cluster.create_group("products", max_size=2000)
    assert products_group.name == "products"
    assert "products" in cluster.groups
    assert len(cluster.groups) == 2

    # Test creating group with duplicate name should raise error
    with pytest.raises(ValueError, match="already exists.*get_group"):
        cluster.create_group("users")


def test_cluster_get_group():
    """Test getting existing cache groups"""
    cluster = GroupCacheCluster("localhost:8081")

    # Test getting non-existent group should raise error
    with pytest.raises(ValueError, match="does not exist.*create_group"):
        cluster.get_group("nonexistent")

    # Create group and test getting it
    original_group = cluster.create_group("test_group")
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
    cluster.create_group("users")
    cluster.create_group("products")
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
    users_group = cluster.create_group("users", max_size=1000)
    products_group = cluster.create_group("products", max_size=500)

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
    group1 = cluster.create_group("group1", max_size=100)
    group2 = cluster.create_group("group2", max_size=200)

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
    group = cluster.create_group("default_group")

    # Should use default value of 10000
    assert group.main_cache.max_size == 10000
    assert group.hot_cache.max_size == 1000  # 10% of main cache
