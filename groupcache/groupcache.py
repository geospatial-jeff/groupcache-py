from typing import Any, Dict, OrderedDict


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

    def stats(self) -> Dict[str, int | None]:
        return {
            "hits": self._hits,
            "misses": self._misses,
            "size": self._size,
            "max_size": self.max_size,
        }
