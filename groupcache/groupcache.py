import asyncio
import pickle
import base64
import logging
import zlib
import bisect
import random
from functools import wraps
from typing import Any, Callable, OrderedDict

from .http_server import GroupCacheHTTPServer
from .http_client import GroupCacheHTTPClient


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
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            self._hits += 1
            return value
        except KeyError:
            self._misses += 1
            return None

    def set(self, key: str, value: Any) -> None:
        if key in self.cache:
            # Update existing
            self.cache[key] = value
            self.cache.move_to_end(key)
        else:
            # Add new
            self.cache[key] = value
            self._size += 1

            # Evict oldest if over capacity
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
            key = self._hash(f"{i}{node}")
            self.ring[key] = node

        self.sorted_keys = sorted(self.ring.keys())
        logger.info(f"Added node {node} to consistent hash ring")

    def remove_node(self, node: str) -> None:
        for i in range(self.replicas):
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
        self._calls: dict[str, asyncio.Future[Any]] = {}

    async def do(self, key: str, fn: Callable) -> Any:
        """Execute function with singleflight coordination (lock-free)"""
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


class GroupCacheCluster:
    """GroupCache cluster management"""

    def __init__(self, self_url: str, base_path: str = "/_groupcache/"):
        self.self_url = self_url
        self.base_path = base_path
        self.consistent_hash = ConsistentHash()
        self.groups: dict[str, "GroupCacheGroup"] = {}
        self.peer_client = GroupCacheHTTPClient(base_path)
        self.http_server = GroupCacheHTTPServer(base_path, self_url)
        self._server_started = False

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

    def create_group(
        self, name: str, loader: Callable[[str], Any], max_size: int = 10000
    ) -> "GroupCacheGroup":
        """Create a cache group (raises if group already exists)"""
        if name in self.groups:
            raise ValueError(
                f"Group '{name}' already exists. Use get_group() to access existing groups."
            )

        self.groups[name] = GroupCacheGroup(
            name=name, cluster=self, loader=loader, max_size=max_size
        )
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

    async def _handle_peer_request(self, group_name: str, key: str) -> Any | None:
        """Handle incoming peer requests for cache values (strict ownership model)"""
        if group_name not in self.groups:
            logger.debug(f"Peer requested unknown group: {group_name}")
            return None

        group = self.groups[group_name]

        # Check mainCache first
        value = group.main_cache.get(key)
        if value is not None:
            group.main_cache_hits += 1
            return value

        # Not in cache - load from source if we have a loader
        if group.loader:
            try:
                group.source_loads += 1

                if asyncio.iscoroutinefunction(group.loader):
                    value = await group.loader(key)
                else:
                    value = group.loader(key)

                if value is not None:
                    # Cache it in mainCache (we're the owner)
                    group._populate_cache(key, value, group.main_cache)

                return value
            except Exception as e:
                logger.error(f"Error loading from source: {e}")

        return None

    async def start_http_server(self):
        """Start the HTTP server for peer requests"""
        if self._server_started:
            logger.warning("HTTP server already started")
            return

        # Set the handler and start server (server auto-parses host/port from self_url)
        self.http_server.set_get_handler(self._handle_peer_request)
        await self.http_server.start()
        self._server_started = True

    async def close(self):
        """Close the cluster and cleanup resources"""
        if self.peer_client:
            await self.peer_client.close()
        if self._server_started:
            await self.http_server.stop()
            self._server_started = False


class GroupCacheGroup:
    """A cache group within the cluster (matches Go groupcache semantics)"""

    def __init__(
        self,
        name: str,
        cluster: GroupCacheCluster,
        loader: Callable[[str], Any],
        max_size: int = 10000,
    ):
        self.name = name
        self.cluster = cluster
        self.loader = loader  # Single loader for this group (like Go's Getter)
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
        value = self.main_cache.get(key)
        if value is not None:
            self.main_cache_hits += 1
            return value

        value = self.hot_cache.get(key)
        if value is not None:
            self.hot_cache_hits += 1
            return value

        return None

    async def get(self, key: str) -> Any | None:
        """Get value with peer coordination (Go groupcache semantics)"""
        cached_value = self._lookup_cache(key)
        if cached_value is not None:
            return cached_value

        return await self.singleflight.do(key, lambda: self._load_key(key))

    def _populate_cache(self, key: str, value: Any, cache: LRUCache) -> None:
        """Populate cache with value (Go groupcache pattern)"""
        cache.set(key, value)

    async def _load_key(self, key: str) -> Any | None:
        """Load key from peer or source (Go groupcache semantics)"""

        cached_value = self._lookup_cache(key)
        if cached_value is not None:
            return cached_value

        # Determine key owner via consistent hashing
        owner_peer = self.cluster.consistent_hash.get_node(key)

        if owner_peer is None or owner_peer == self.cluster.self_url:
            # We are authoritative for this key - load from source
            if self.loader:
                self.source_loads += 1
                if asyncio.iscoroutinefunction(self.loader):
                    value = await self.loader(key)
                else:
                    value = self.loader(key)
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
            if self.loader:
                self.source_loads += 1
                if asyncio.iscoroutinefunction(self.loader):
                    value = await self.loader(key)
                else:
                    value = self.loader(key)
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
    """Generate reversible cache key for distributed caching"""
    # Create a reversible key by encoding the function call data
    key_data = {"func": func_name, "args": args, "kwargs": kwargs}

    # Serialize and encode for safe string representation
    serialized = pickle.dumps(key_data)
    encoded = base64.urlsafe_b64encode(serialized).decode("ascii")

    return f"cached:{encoded}"


def _parse_cache_key(cache_key: str) -> tuple[str, tuple, dict]:
    """Parse cache key back into function name, args, and kwargs"""
    if not cache_key.startswith("cached:"):
        raise ValueError(f"Invalid cache key format: {cache_key}")

    encoded = cache_key[7:]  # Remove "cached:" prefix
    serialized = base64.urlsafe_b64decode(encoded.encode("ascii"))
    key_data = pickle.loads(serialized)

    return key_data["func"], key_data["args"], key_data["kwargs"]


# Global cluster singleton
_global_cluster: GroupCacheCluster | None = None


async def configure_cluster(
    self_url: str, peer_urls: list[str] | None = None, auto_start_server: bool = True
) -> GroupCacheCluster:
    """Configure the global GroupCache cluster (singleton pattern)"""
    global _global_cluster

    _global_cluster = GroupCacheCluster(self_url)
    if peer_urls:
        _global_cluster.set_peers(peer_urls)

    if auto_start_server:
        await _global_cluster.start_http_server()

    return _global_cluster


def get_cluster() -> GroupCacheCluster | None:
    """Get the global cluster instance"""
    return _global_cluster


# Store decorated functions for peer requests - all peers have the same functions
_group_functions: dict[str, tuple[Callable, str]] = {}  # group -> (func, func_name)


def cached(group: str):
    """Decorator for distributed caching (Go-style: one loader per group)"""

    def decorator(func: Callable) -> Callable:
        # Register this function for the group
        _group_functions[group] = (func, func.__name__)

        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            if not _global_cluster:
                raise RuntimeError(
                    "GroupCache cluster not configured. Call configure_cluster() first."
                )

            # Get or create cache group with this function as the loader
            try:
                cache_group = _global_cluster.get_group(group)
            except ValueError:
                # Group doesn't exist, create it with this function as the loader
                async def group_loader(cache_key: str):
                    """Reconstruct and execute the original function call from cache key"""
                    try:
                        # Parse the cache key to extract function call details
                        func_name, parsed_args, parsed_kwargs = _parse_cache_key(
                            cache_key
                        )
                        registered_func, expected_func_name = _group_functions[group]

                        # Verify this is the right function
                        if func_name != expected_func_name:
                            logger.error(
                                f"Cache key function {func_name} doesn't match registered function {expected_func_name}"
                            )
                            return None

                        # Execute the original function with the parsed arguments
                        logger.debug(
                            f"Peer executing: {func_name}({parsed_args}, {parsed_kwargs})"
                        )

                        if asyncio.iscoroutinefunction(registered_func):
                            return await registered_func(*parsed_args, **parsed_kwargs)
                        else:
                            # Run sync function in thread to avoid blocking event loop
                            return await asyncio.to_thread(
                                registered_func, *parsed_args, **parsed_kwargs
                            )

                    except Exception as e:
                        logger.error(f"Error in group loader for {group}: {e}")
                        return None

                cache_group = _global_cluster.create_group(group, group_loader)

            key = _make_cache_key(func.__name__, *args, **kwargs)
            return await cache_group.get(key)

        return wrapper

    return decorator
