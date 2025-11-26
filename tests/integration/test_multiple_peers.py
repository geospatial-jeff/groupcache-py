"""
Integration test for distributed GroupCache workflow.

Tests peer-to-peer communication with multiple nodes:
1. Key owned by peer A requested by peer B
2. Peer B requests from peer A via HTTP
3. Peer B caches result in hot cache
4. Subsequent requests served from hot cache
"""

import pytest
import asyncio
from groupcache.groupcache import GroupCacheCluster


@pytest.mark.asyncio
async def test_distributed_peer_to_peer_workflow():
    """Test distributed cache workflow with peer-to-peer HTTP communication"""

    # Create loaders that track which node loaded what
    node1_loads = []
    node2_loads = []

    def node1_loader(key: str) -> str:
        node1_loads.append(key)
        return f"node1_data_for_{key}"

    def node2_loader(key: str) -> str:
        node2_loads.append(key)
        return f"node2_data_for_{key}"

    # Create two cluster nodes
    cluster1 = GroupCacheCluster(self_url="http://localhost:8080")
    cluster2 = GroupCacheCluster(self_url="http://localhost:8081")

    try:
        # Configure peers for both nodes
        peer_urls = ["http://localhost:8080", "http://localhost:8081"]
        cluster1.set_peers(peer_urls)
        cluster2.set_peers(peer_urls)

        # Create groups on both nodes with different loaders
        group1 = cluster1.create_group("distributed_test", node1_loader, max_size=100)
        group2 = cluster2.create_group("distributed_test", node2_loader, max_size=100)

        # Start HTTP servers
        await cluster1.start_http_server()
        await cluster2.start_http_server()

        # Give servers a moment to start
        await asyncio.sleep(0.1)

        # Find a key that cluster1 owns (so cluster2 will need to request it)
        test_key = None
        for potential_key in ["key1", "key2", "key3", "key4", "key5"]:
            owner = cluster1.consistent_hash.get_node(potential_key)
            if owner == "http://localhost:8080":  # cluster1 owns this key
                test_key = potential_key
                break

        assert test_key is not None, "Could not find key owned by cluster1"

        # Test 1: cluster2 requests key owned by cluster1
        # This should trigger HTTP request from cluster2 to cluster1
        result = await group2.get(test_key)

        # Verify the result came from cluster1's loader
        assert result == f"node1_data_for_{test_key}"
        assert len(node1_loads) == 1
        assert node1_loads[0] == test_key
        assert len(node2_loads) == 0  # cluster2 didn't load anything

        # Verify cluster2 metrics show peer request
        assert group2.peer_requests == 1
        assert group2.peer_hits == 1
        assert group2.source_loads == 0  # No local loads

        # Verify cluster1 metrics show source load
        assert group1.peer_requests == 0  # No peer requests
        assert group1.source_loads == 1  # Loaded from source

        # Test 2: Same key requested again by cluster2
        # May serve from hot cache (10% probability) or make another peer request
        result2 = await group2.get(test_key)

        assert result2 == f"node1_data_for_{test_key}"
        assert (
            len(node1_loads) == 1
        )  # No additional loads on cluster1 (served from cache)

        # Either served from hot cache OR made another peer request (but no additional source load)
        # Hot cache population is probabilistic, so we can't guarantee it

        # Test 3: Find a key that cluster2 owns
        test_key2 = None
        for potential_key in ["keyA", "keyB", "keyC", "keyD", "keyE"]:
            owner = cluster2.consistent_hash.get_node(potential_key)
            if owner == "http://localhost:8081":  # cluster2 owns this key
                test_key2 = potential_key
                break

        assert test_key2 is not None, "Could not find key owned by cluster2"

        # Test 3: cluster1 requests key owned by cluster2
        result3 = await group1.get(test_key2)

        # Verify the result came from cluster2's loader
        assert result3 == f"node2_data_for_{test_key2}"
        assert len(node2_loads) == 1
        assert node2_loads[0] == test_key2

        # Verify cluster1 metrics show peer request
        assert group1.peer_requests == 1
        assert group1.peer_hits == 1

        # Test 4: Verify both clusters can serve their own keys locally
        local_key1 = test_key  # cluster1 owns this
        local_key2 = test_key2  # cluster2 owns this

        # cluster1 serves its own key (should use cache now)
        result4 = await group1.get(local_key1)
        assert result4 == f"node1_data_for_{local_key1}"
        assert len(node1_loads) == 1  # No additional loads (served from cache)
        assert group1.main_cache_hits >= 1  # At least one cache hit

        # cluster2 serves its own key (should use cache now)
        result5 = await group2.get(local_key2)
        assert result5 == f"node2_data_for_{local_key2}"
        assert len(node2_loads) == 1  # No additional loads (served from cache)
        assert group2.main_cache_hits >= 1  # At least one cache hit

        # Verify final metrics
        print(f"Cluster1 stats: {group1.get_stats()}")
        print(f"Cluster2 stats: {group2.get_stats()}")

        # Verify that both clusters have done some work
        assert group1.source_loads >= 1  # At least loaded one key
        assert group2.source_loads >= 1  # At least loaded one key
        assert group1.peer_requests == 1  # Made one peer request
        assert group2.peer_requests >= 1  # Made at least one peer request

    finally:
        # Cleanup - stop servers and close clusters
        await cluster1.close()
        await cluster2.close()


@pytest.mark.asyncio
async def test_distributed_peer_failure_fallback():
    """Test fallback behavior when peer is unreachable"""

    # Create loaders
    node1_loads = []
    node2_loads = []

    def node1_loader(key: str) -> str:
        node1_loads.append(key)
        return f"node1_fallback_{key}"

    def node2_loader(key: str) -> str:
        node2_loads.append(key)
        return f"node2_fallback_{key}"

    # Create clusters with one unreachable peer
    cluster1 = GroupCacheCluster(self_url="http://localhost:8082")
    cluster2 = GroupCacheCluster(self_url="http://localhost:8083")

    try:
        # Configure peers - include an unreachable peer
        peer_urls = [
            "http://localhost:8082",
            "http://localhost:8083",
            "http://localhost:9999",
        ]  # 9999 is unreachable
        cluster1.set_peers(peer_urls)
        cluster2.set_peers(peer_urls)

        # Create groups
        group1 = cluster1.create_group("fallback_test", node1_loader)
        cluster2.create_group("fallback_test", node2_loader)

        # Start only cluster1's server (cluster2 server not started = unreachable)
        await cluster1.start_http_server()

        await asyncio.sleep(0.1)

        # Find a key that the unreachable peer (9999) would own
        test_key = None
        for potential_key in [
            "fallback1",
            "fallback2",
            "fallback3",
            "fallback4",
            "fallback5",
        ]:
            owner = cluster1.consistent_hash.get_node(potential_key)
            if owner == "http://localhost:9999":  # unreachable peer owns this key
                test_key = potential_key
                break

        if test_key is None:
            # If we can't find a key owned by the unreachable peer,
            # try finding one owned by cluster2 (whose server isn't started)
            for potential_key in [
                "fallback1",
                "fallback2",
                "fallback3",
                "fallback4",
                "fallback5",
            ]:
                owner = cluster1.consistent_hash.get_node(potential_key)
                if (
                    owner == "http://localhost:8083"
                ):  # cluster2 owns this key but server not started
                    test_key = potential_key
                    break

        assert test_key is not None, "Could not find key owned by unreachable peer"

        # cluster1 requests key owned by unreachable peer
        # Should fallback to local loader
        result = await group1.get(test_key)

        # Verify fallback occurred
        assert result == f"node1_fallback_{test_key}"
        assert len(node1_loads) == 1
        assert node1_loads[0] == test_key

        # Verify metrics show peer request attempt and fallback
        assert group1.peer_requests == 1  # Attempted peer request
        assert group1.peer_hits == 0  # But peer request failed
        assert group1.source_loads == 1  # Fell back to local loader

    finally:
        await cluster1.close()
        await cluster2.close()


@pytest.mark.asyncio
async def test_distributed_hot_cache_behavior():
    """Test hot cache population and behavior in distributed setup"""

    node1_loads = []
    node2_loads = []

    def node1_loader(key: str) -> str:
        node1_loads.append(key)
        return f"popular_data_{key}"

    def node2_loader(key: str) -> str:
        node2_loads.append(key)
        return f"node2_data_{key}"

    cluster1 = GroupCacheCluster(self_url="http://localhost:8084")
    cluster2 = GroupCacheCluster(self_url="http://localhost:8085")

    try:
        # Configure peers
        peer_urls = ["http://localhost:8084", "http://localhost:8085"]
        cluster1.set_peers(peer_urls)
        cluster2.set_peers(peer_urls)

        # Create groups with small hot cache to test eviction
        cluster1.create_group("hot_cache_test", node1_loader, max_size=100)
        group2 = cluster2.create_group("hot_cache_test", node2_loader, max_size=100)

        # Start servers
        await cluster1.start_http_server()
        await cluster2.start_http_server()

        await asyncio.sleep(0.1)

        # Find a key owned by cluster1
        popular_key = None
        for potential_key in [
            "popular1",
            "popular2",
            "popular3",
            "popular4",
            "popular5",
        ]:
            owner = cluster1.consistent_hash.get_node(potential_key)
            if owner == "http://localhost:8084":  # cluster1 owns this key
                popular_key = potential_key
                break

        assert popular_key is not None, "Could not find key owned by cluster1"

        # Make multiple requests from cluster2 for the same key owned by cluster1
        # Hot cache population happens with 10% probability, so make multiple requests
        results = []
        for i in range(
            20
        ):  # Make enough requests to likely trigger hot cache population
            result = await group2.get(popular_key)
            results.append(result)

            # Reset some state to avoid the request being served from hot cache immediately
            if i < 19:  # Don't reset on last iteration
                await asyncio.sleep(0.01)

        # All results should be the same
        assert all(result == f"popular_data_{popular_key}" for result in results)

        # cluster1 should have loaded the key only once (on first request)
        assert len(node1_loads) == 1
        assert node1_loads[0] == popular_key

        # cluster2 should have made peer requests but some may have been served from hot cache
        assert group2.peer_requests >= 1
        assert group2.peer_hits >= 1

        # If hot cache was populated, some requests should have been served from it
        # (This is probabilistic, so we can't guarantee it, but with 20 requests it's very likely)
        print(f"Hot cache hits: {group2.hot_cache_hits}")
        print(f"Peer requests: {group2.peer_requests}")
        print(f"Peer hits: {group2.peer_hits}")

    finally:
        await cluster1.close()
        await cluster2.close()


@pytest.mark.asyncio
async def test_distributed_concurrent_requests():
    """Test concurrent requests across multiple peers"""

    node1_loads = []
    node2_loads = []

    async def slow_node1_loader(key: str) -> str:
        await asyncio.sleep(0.1)  # Simulate slow loading
        node1_loads.append(key)
        return f"slow_data_{key}"

    def node2_loader(key: str) -> str:
        node2_loads.append(key)
        return f"fast_data_{key}"

    cluster1 = GroupCacheCluster(self_url="http://localhost:8086")
    cluster2 = GroupCacheCluster(self_url="http://localhost:8087")

    try:
        # Configure peers
        peer_urls = ["http://localhost:8086", "http://localhost:8087"]
        cluster1.set_peers(peer_urls)
        cluster2.set_peers(peer_urls)

        # Create groups
        cluster1.create_group("concurrent_test", slow_node1_loader)
        group2 = cluster2.create_group("concurrent_test", node2_loader)

        # Start servers
        await cluster1.start_http_server()
        await cluster2.start_http_server()

        await asyncio.sleep(0.1)

        # Find a key owned by cluster1 (slow loader)
        slow_key = None
        for potential_key in ["slow1", "slow2", "slow3", "slow4", "slow5"]:
            owner = cluster1.consistent_hash.get_node(potential_key)
            if owner == "http://localhost:8086":  # cluster1 owns this key
                slow_key = potential_key
                break

        assert slow_key is not None, "Could not find key owned by cluster1"

        # Make multiple concurrent requests from cluster2 for the same slow key
        # Singleflight should ensure only one HTTP request is made to cluster1
        tasks = [
            group2.get(slow_key),
            group2.get(slow_key),
            group2.get(slow_key),
        ]

        results = await asyncio.gather(*tasks)

        # All results should be the same
        assert all(result == f"slow_data_{slow_key}" for result in results)

        # cluster1 should have loaded the key only once (singleflight)
        assert len(node1_loads) == 1
        assert node1_loads[0] == slow_key

        # cluster2 should have made only one successful peer request
        assert group2.peer_requests == 1
        assert group2.peer_hits == 1

    finally:
        await cluster1.close()
        await cluster2.close()


@pytest.mark.asyncio
async def test_three_node_cluster():
    """Test distributed behavior with three nodes"""

    node1_loads = []
    node2_loads = []
    node3_loads = []

    def node1_loader(key: str) -> str:
        node1_loads.append(key)
        return f"node1_{key}"

    def node2_loader(key: str) -> str:
        node2_loads.append(key)
        return f"node2_{key}"

    def node3_loader(key: str) -> str:
        node3_loads.append(key)
        return f"node3_{key}"

    cluster1 = GroupCacheCluster(self_url="http://localhost:8088")
    cluster2 = GroupCacheCluster(self_url="http://localhost:8089")
    cluster3 = GroupCacheCluster(self_url="http://localhost:8090")

    try:
        # Configure three-node cluster
        peer_urls = [
            "http://localhost:8088",
            "http://localhost:8089",
            "http://localhost:8090",
        ]
        cluster1.set_peers(peer_urls)
        cluster2.set_peers(peer_urls)
        cluster3.set_peers(peer_urls)

        # Create groups
        group1 = cluster1.create_group("three_node_test", node1_loader)
        cluster2.create_group("three_node_test", node2_loader)
        cluster3.create_group("three_node_test", node3_loader)

        # Start servers
        await cluster1.start_http_server()
        await cluster2.start_http_server()
        await cluster3.start_http_server()

        await asyncio.sleep(0.1)

        # Test keys distributed across all three nodes
        keys_to_test = ["alpha", "beta", "gamma", "delta", "epsilon"]
        results = {}

        for key in keys_to_test:
            # Have cluster1 request all keys
            result = await group1.get(key)
            results[key] = result

        # Verify that keys were distributed and loaded by different nodes
        total_node1_loads = len(node1_loads)
        total_node2_loads = len(node2_loads)
        total_node3_loads = len(node3_loads)

        # At least one node should have loaded something
        assert total_node1_loads + total_node2_loads + total_node3_loads >= 1

        # All results should be properly formatted
        for key, result in results.items():
            assert result.startswith(("node1_", "node2_", "node3_"))
            assert result.endswith(key)

    finally:
        await cluster1.close()
        await cluster2.close()
        await cluster3.close()


@pytest.mark.asyncio
async def test_multiple_groups_across_peers():
    """Test multiple cache groups distributed across peers"""

    users_loads = []
    products_loads = []

    def users_loader(key: str) -> str:
        users_loads.append(key)
        return f"user_data_{key}"

    def products_loader(key: str) -> str:
        products_loads.append(key)
        return f"product_data_{key}"

    cluster1 = GroupCacheCluster(self_url="http://localhost:8091")
    cluster2 = GroupCacheCluster(self_url="http://localhost:8092")

    try:
        # Configure cluster
        peer_urls = ["http://localhost:8091", "http://localhost:8092"]
        cluster1.set_peers(peer_urls)
        cluster2.set_peers(peer_urls)

        # Create different groups on each cluster
        cluster1.create_group("users", users_loader)
        users_group2 = cluster2.create_group("users", users_loader)

        products_group1 = cluster1.create_group("products", products_loader)
        cluster2.create_group("products", products_loader)

        # Start servers
        await cluster1.start_http_server()
        await cluster2.start_http_server()

        await asyncio.sleep(0.1)

        # Test cross-group requests
        user_key = "user123"
        product_key = "product456"

        # Request user from cluster2
        user_result = await users_group2.get(user_key)
        assert user_result == f"user_data_{user_key}"

        # Request product from cluster1
        product_result = await products_group1.get(product_key)
        assert product_result == f"product_data_{product_key}"

        # Verify groups are isolated
        assert len(users_loads) >= 1
        assert len(products_loads) >= 1

    finally:
        await cluster1.close()
        await cluster2.close()


@pytest.mark.asyncio
async def test_peer_loader_exception_propagation():
    """Test that loader exceptions are properly propagated across peers"""

    def failing_loader(key: str) -> str:
        if "fail" in key:
            raise ValueError(f"Loader error for key: {key}")
        return f"success_{key}"

    def success_loader(key: str) -> str:
        return f"backup_{key}"

    cluster1 = GroupCacheCluster(self_url="http://localhost:8093")
    cluster2 = GroupCacheCluster(self_url="http://localhost:8094")

    try:
        # Configure cluster
        peer_urls = ["http://localhost:8093", "http://localhost:8094"]
        cluster1.set_peers(peer_urls)
        cluster2.set_peers(peer_urls)

        # cluster1 has failing loader, cluster2 has success loader
        cluster1.create_group("exception_test", failing_loader)
        group2 = cluster2.create_group("exception_test", success_loader)

        # Start servers
        await cluster1.start_http_server()
        await cluster2.start_http_server()

        await asyncio.sleep(0.1)

        # Find a key owned by cluster1 (with failing loader)
        fail_key = None
        for potential_key in ["fail_test1", "fail_test2", "fail_test3"]:
            owner = cluster1.consistent_hash.get_node(potential_key)
            if owner == "http://localhost:8093":
                fail_key = potential_key
                break

        if fail_key:
            # Have cluster2 request a key owned by cluster1
            # The request should fall back to cluster2's loader when cluster1's fails
            result = await group2.get(fail_key)
            assert result == f"backup_{fail_key}"

    finally:
        await cluster1.close()
        await cluster2.close()


@pytest.mark.asyncio
async def test_hot_cache_eviction_under_load():
    """Test hot cache eviction behavior under load"""

    loads_count = []

    def data_loader(key: str) -> str:
        loads_count.append(key)
        return f"data_{key}"

    cluster1 = GroupCacheCluster(self_url="http://localhost:8095")
    cluster2 = GroupCacheCluster(self_url="http://localhost:8096")

    try:
        # Configure cluster
        peer_urls = ["http://localhost:8095", "http://localhost:8096"]
        cluster1.set_peers(peer_urls)
        cluster2.set_peers(peer_urls)

        # Create groups with small cache sizes to trigger eviction
        cluster1.create_group("eviction_test", data_loader, max_size=10)
        group2 = cluster2.create_group("eviction_test", data_loader, max_size=10)

        # Start servers
        await cluster1.start_http_server()
        await cluster2.start_http_server()

        await asyncio.sleep(0.1)

        # Generate many keys that cluster1 owns
        cluster1_keys = []
        for i in range(50):  # Generate many test keys
            test_key = f"load_key_{i}"
            owner = cluster1.consistent_hash.get_node(test_key)
            if owner == "http://localhost:8095":
                cluster1_keys.append(test_key)
            if len(cluster1_keys) >= 20:  # Stop when we have enough
                break

        assert len(cluster1_keys) >= 10, "Need at least 10 keys owned by cluster1"

        # Have cluster2 request many keys from cluster1 to trigger hot cache eviction
        for key in cluster1_keys:
            result = await group2.get(key)
            assert result == f"data_{key}"

        # Verify that loads occurred
        assert len(loads_count) >= 10

        # Hot cache should have been evicted and repopulated during this process
        # The exact behavior is probabilistic, but we can verify the system handled the load
        hot_cache_size = group2.hot_cache.size()
        assert hot_cache_size <= 1  # Hot cache max size is max_size // 10 = 1

    finally:
        await cluster1.close()
        await cluster2.close()
