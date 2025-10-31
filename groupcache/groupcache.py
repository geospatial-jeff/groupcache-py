import logging
import zlib
import bisect
from typing import Any, OrderedDict


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

        # If we didn't find exact match, we need first element >= hash_key
        if idx < len(self.sorted_keys) and self.sorted_keys[idx] == hash_key:
            # Exact match
            pass
        elif idx < len(self.sorted_keys):
            # Found insertion point, keys[idx] > hash_key, which is what we want
            pass
        else:
            # All keys are < hash_key, wrap around to first node
            idx = 0

        return self.ring[self.sorted_keys[idx]]

    def get_nodes(self) -> list[str]:
        return list(set(self.ring.values()))

    def _hash(self, key: str) -> int:
        """Hash function for consistent hashing - matches Go's CRC32"""
        return zlib.crc32(key.encode()) & 0xFFFFFFFF
