import asyncio
import logging
import zlib
import bisect
import random
from functools import wraps, _make_key
from typing import Any, Callable, OrderedDict


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LRUCache:
    """Optimized LRU cache with get/set operations"""

    __slots__ = ("max_size", "cache", "_hits", "_misses", "_size")

    def __init__(self, max_size: int | None = 10000):
        self.max_size = max_size
        self.cache: OrderedDict[str, Any] = OrderedDict()
        self._hits = 0
        self._misses = 0
        self._size = 0

    def get(self, key: str) -> Any | None:
        try:
            value = self.cache[key]
            # Move to end (most recently used) - O(1) operation
            self.cache.move_to_end(key)
            self._hits += 1
            return value
        except KeyError:
            self._misses += 1
            return None

    def set(self, key: str, value: Any) -> None:
        if key in self.cache:
            # Update existing - O(1)
            self.cache[key] = value
            self.cache.move_to_end(key)
        else:
            # Add new
            self.cache[key] = value
            self._size += 1

            # Evict oldest if over capacity - O(1)
            # max_size = None means unlimited
            if self.max_size is not None and self._size > self.max_size:
                self.cache.popitem(last=False)
                self._size -= 1

    def size(self) -> int:
        return self._size

    def stats(self) -> dict[str, int | None]:
        return {
            "hits": self._hits,
            "misses": self._misses,
            "size": self._size,
            "max_size": self.max_size,
        }


class ConsistentHash:
    """Consistent hashing for peer selection"""

    def __init__(self, replicas: int = 150):
        self.replicas = replicas
        self.ring: dict[int, str] = {}
        self.sorted_keys: list[int] = []

    def add_node(self, node: str) -> None:
        for i in range(self.replicas):
            # Match Go's key format: strconv.Itoa(i) + key
            key = self._hash(f"{i}{node}")
            self.ring[key] = node

        self.sorted_keys = sorted(self.ring.keys())
        logger.info(f"Added node {node} to consistent hash ring")

    def remove_node(self, node: str) -> None:
        for i in range(self.replicas):
            # Match Go's key format: strconv.Itoa(i) + key
            key = self._hash(f"{i}{node}")
            if key in self.ring:
                del self.ring[key]

        self.sorted_keys = sorted(self.ring.keys())
        logger.info(f"Removed node {node} from consistent hash ring")

    def get_node(self, key: str) -> str | None:
        if not self.ring:
            return None

        hash_key = self._hash(key)

        # Binary search for appropriate replica (matching Go's sort.Search)
        idx = bisect.bisect_left(self.sorted_keys, hash_key)

        # If idx == len, all keys are < hash_key, wrap to first replica
        if idx == len(self.sorted_keys):
            idx = 0

        return self.ring[self.sorted_keys[idx]]

    def get_nodes(self) -> list[str]:
        return list(set(self.ring.values()))

    def _hash(self, key: str) -> int:
        """Hash function for consistent hashing - matches Go's CRC32"""
        return zlib.crc32(key.encode()) & 0xFFFFFFFF


class ChannelSingleFlight:
    __slots__ = ("_calls",)

    def __init__(self):
        # Store Future for coordination
        self._calls: dict[str, asyncio.Future[Any]] = {}

    async def do(self, key: str, fn: Callable) -> Any:
        """Execute function with singleflight coordination (lock-free)"""

        # Try to atomically insert our future
        future = asyncio.Future[Any]()
        existing = self._calls.setdefault(key, future)

        if existing is not future:
            # Someone else won the race, wait for their result
            try:
                return await existing
            except Exception:
                # Re-raise the exception from the original call
                raise

        # We won the race, execute the function
        try:
            result = await fn()
            future.set_result(result)
            return result
        except Exception as e:
            # Set exception on future so other waiters get it
            if not future.done():
                future.set_exception(e)
            raise
        finally:
            # Clean up (atomic operation)
            self._calls.pop(key, None)


class MockPeerClient:
    """TODO: Replace with real calls to peers"""

    async def get(self, peer_url: str, group: str, key: str) -> Any | None:
        await asyncio.sleep(0.001)  # Simulate network latency
        return None


class GroupCacheCluster:
    """GroupCache cluster management"""

    def __init__(self, self_url: str):
        self.self_url = self_url
        self.consistent_hash = ConsistentHash()
        self.groups: dict[str, "GroupCacheGroup"] = {}
        self.peer_client = MockPeerClient()

    def set_peers(self, peer_urls: list[str]) -> None:
        """Configure cluster peers"""
        # Clear existing nodes
        for node in self.consistent_hash.get_nodes():
            self.consistent_hash.remove_node(node)

        # Add all peers including self
        all_peers = set(peer_urls + [self.self_url])
        for peer in all_peers:
            self.consistent_hash.add_node(peer)

        logger.info(f"Configured cluster with {len(all_peers)} peers")

    def create_group(self, name: str, max_size: int = 10000) -> "GroupCacheGroup":
        """Create a cache group (raises if group already exists)"""
        if name in self.groups:
            raise ValueError(
                f"Group '{name}' already exists. Use get_group() to access existing groups."
            )

        self.groups[name] = GroupCacheGroup(name=name, cluster=self, max_size=max_size)
        logger.info(f"Created cache group '{name}' with max_size={max_size}")
        return self.groups[name]

    def get_group(self, name: str) -> "GroupCacheGroup":
        """Get an existing cache group (raises if group doesn't exist)"""
        if name not in self.groups:
            raise ValueError(
                f"Group '{name}' does not exist. Use create_group() to create it first."
            )
        return self.groups[name]

    def get_stats(self) -> dict[str, Any]:
        """Get cluster statistics"""
        stats = {
            "peers": len(self.consistent_hash.get_nodes()),
            "groups": len(self.groups),
            "self_url": self.self_url,
        }

        for name, group in self.groups.items():
            stats[f"group_{name}"] = group.get_stats()

        return stats


class GroupCacheGroup:
    """A cache group within the cluster (matches Go groupcache semantics)"""

    def __init__(self, name: str, cluster: GroupCacheCluster, max_size: int = 10000):
        self.name = name
        self.cluster = cluster

        # Go groupcache cache architecture:
        # mainCache: authoritative data (keys we own via consistent hashing)
        # hotCache: popular remote data (keys owned by other peers)
        self.main_cache = LRUCache(max_size)  # Keys this peer is authoritative for
        self.hot_cache = LRUCache(max_size // 10)  # Popular keys from other peers

        self.singleflight = ChannelSingleFlight()

        # Metrics
        self.peer_requests = 0
        self.peer_hits = 0
        self.source_loads = 0
        self.main_cache_hits = 0
        self.hot_cache_hits = 0

    def _lookup_cache(self, key: str) -> Any | None:
        """Look up key in both mainCache and hotCache (Go groupcache pattern)"""

        # Check mainCache first (authoritative data)
        value = self.main_cache.get(key)
        if value is not None:
            self.main_cache_hits += 1
            return value

        # Check hotCache second (popular remote data)
        value = self.hot_cache.get(key)
        if value is not None:
            self.hot_cache_hits += 1
            return value

        return None

    async def get(self, key: str, loader: Callable | None = None) -> Any | None:
        """Get value with peer coordination (Go groupcache semantics)"""

        # 1. Look up in both caches (mainCache + hotCache)
        cached_value = self._lookup_cache(key)
        if cached_value is not None:
            return cached_value

        # 2. Use singleflight to prevent duplicate loads
        return await self.singleflight.do(key, lambda: self._load_key(key, loader))

    def _populate_cache(self, key: str, value: Any, cache: LRUCache) -> None:
        """Populate cache with value (Go groupcache pattern)"""
        cache.set(key, value)

    async def _load_key(self, key: str, loader: Callable | None) -> Any | None:
        """Load key from peer or source (Go groupcache semantics)"""

        # Double-check cache (singleflight pattern from Go)
        cached_value = self._lookup_cache(key)
        if cached_value is not None:
            return cached_value

        # Determine key owner via consistent hashing
        owner_peer = self.cluster.consistent_hash.get_node(key)

        if owner_peer is None or owner_peer == self.cluster.self_url:
            # We are authoritative for this key - load from source
            if loader:
                self.source_loads += 1
                value = await loader()
                if value is not None:
                    # Store in mainCache (we're authoritative)
                    self._populate_cache(key, value, self.main_cache)
                return value
            return None
        else:
            # Request from peer (we're not authoritative)
            try:
                self.peer_requests += 1
                value = await self.cluster.peer_client.get(owner_peer, self.name, key)
                if value is not None:
                    self.peer_hits += 1
                    # Store in hot cache with probability (popular remote data)
                    # Go uses 10% probability to avoid hot cache bloat
                    if random.randint(1, 10) == 1:  # 10% chance
                        self._populate_cache(key, value, self.hot_cache)

                    return value
            except Exception as e:
                logger.warning(f"Peer request failed: {e}")

            # Fallback to local load if peer fails
            if loader:
                self.source_loads += 1
                value = await loader()
                if value is not None:
                    # Store in main cache (fallback makes us authoritative)
                    self._populate_cache(key, value, self.main_cache)
                return value

            return None

    async def set(self, key: str, value: Any) -> None:
        """Set value in appropriate cache based on ownership"""
        owner_peer = self.cluster.consistent_hash.get_node(key)

        if owner_peer is None or owner_peer == self.cluster.self_url:
            # We own this key - store in main cache
            self._populate_cache(key, value, self.main_cache)
        else:
            # We don't own this key - store in hot cache
            self._populate_cache(key, value, self.hot_cache)

    def get_stats(self) -> dict[str, Any]:
        """Get group statistics (Go groupcache style)"""
        main_stats = self.main_cache.stats()
        hot_stats = self.hot_cache.stats()

        return {
            "main_cache": main_stats,
            "hot_cache": hot_stats,
            "main_cache_hits": self.main_cache_hits,
            "hot_cache_hits": self.hot_cache_hits,
            "peer_requests": self.peer_requests,
            "peer_hits": self.peer_hits,
            "source_loads": self.source_loads,
            "peer_hit_rate": self.peer_hits / max(self.peer_requests, 1),
            "total_cache_hits": self.main_cache_hits + self.hot_cache_hits,
        }


def _make_cache_key(func_name: str, *args, **kwargs) -> str:
    """Generate cache key using functools._make_key (battle-tested)"""
    # Prepend function name to args for uniqueness across functions
    key_args = (func_name,) + args

    # Use functools._make_key for optimal performance
    key = _make_key(key_args, kwargs, typed=False)

    # Convert to string for our cache (functools returns various types)
    if isinstance(key, (str, int)):
        return str(key)
    else:
        return str(hash(key))


# Global cluster instance
_global_cluster: GroupCacheCluster | None = None


def configure_cluster(
    self_url: str, peer_urls: list[str] | None = None
) -> GroupCacheCluster:
    """Configure the global GroupCache cluster (singleton pattern)"""
    global _global_cluster

    _global_cluster = GroupCacheCluster(self_url)
    if peer_urls:
        _global_cluster.set_peers(peer_urls)

    return _global_cluster


def get_cluster() -> GroupCacheCluster | None:
    """Get the global cluster instance"""
    return _global_cluster


def cached(group: str):
    """Decorator for distributed caching (group must already exist)"""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            if not _global_cluster:
                raise RuntimeError(
                    "GroupCache cluster not configured. Call configure_cluster() first."
                )

            # Get existing cache group (will raise if doesn't exist)
            cache_group = _global_cluster.get_group(group)

            # Generate cache key
            key = _make_cache_key(func.__name__, *args, **kwargs)

            # Try to get from distributed cache
            cached_value = await cache_group.get(key)
            if cached_value is not None:
                return cached_value

            # Cache miss - execute function and cache result
            async def loader():
                result = await func(*args, **kwargs)
                return result

            return await cache_group.get(key, loader)

        return wrapper

    return decorator
