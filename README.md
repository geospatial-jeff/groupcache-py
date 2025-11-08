# groupcache-py

A Python implementation of [groupcache](https://github.com/golang/groupcache) - a distributed caching library that turns your existing application servers into a unified cache cluster.  Groupcache connects to its own peers, forming a distributed cache.  Your application servers *are* the cache.  No Redis, Memcached, or additional infrastructure required.

## Core API

The core API organizes caching into three levels:

- **Cluster** - A collection of cache servers that work together.
- **Group** - A namespace for related data (like "users" or "posts") with its own loader function.
- **Key** - The specific item you want to cache (like "123" for user ID 123).

This lets you create cache groups where you define how to load data when it's not cached. Perfect for caching database queries, API calls, or any expensive operations:

```python
import asyncio
from groupcache import configure_cluster

async def user_loader(key: str) -> str:
    """Load user data when not in cache"""
    print(f"Loading {key} from database...")
    return f"User data for {key}"

async def main():
    # Configure a single-node cluster
    cluster = await configure_cluster()
    
    # Create cache group with loader function
    users = cluster.create_group("users", user_loader)
    
    # Use the cache
    data1 = await users.get("123")  # Cache miss - calls loader
    data2 = await users.get("123")  # Cache hit - returns cached value
    
    print(f"First call: {data1}")
    print(f"Second call: {data2}")
    
    await cluster.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## Multi-Peer Setup

For distributed caching across multiple servers, each server runs similar code with different URLs. See below for what one server would look like:

```python
import asyncio
from groupcache import configure_cluster

async def user_loader(key: str) -> str:
    """Load user data when not in cache"""
    print(f"Loading {key} from database...")
    return f"User data for {key}"

async def main():
    # Configure cluster - server 1 on port 8080
    cluster = await configure_cluster(
        self_url="http://localhost:8080",  # This server's URL
        peer_urls=[
            "http://localhost:8080",       # This server
            "http://localhost:8081",       # Peer server 2
            "http://localhost:8082"        # Peer server 3
        ]
    )
    
    # Create the same cache group on all servers
    users = cluster.create_group("users", user_loader)
    
    # Keys are automatically distributed across peers
    data1 = await users.get("123")  # May load locally or from peer
    data2 = await users.get("456")  # May load locally or from peer
    
    print(f"User 123: {data1}")
    print(f"User 456: {data2}")
    
    await cluster.close()

if __name__ == "__main__":
    asyncio.run(main())
```

**To run a multi-peer setup:**
- Run the same code on each server, changing only the `self_url` (8080, 8081, 8082)
- Each server needs the same `peer_urls` list and loader function
- Keys are automatically distributed - if server 8080 owns key "123", other servers will fetch it via HTTP
- If a peer is unavailable, the system falls back to loading locally

See the [examples/](examples/) folder for complete working multi-peer examples you can run locally.

## Decorator API

For even simpler usage, you can use the `@cached` decorator to automatically cache function calls. This is syntactic sugar over the Core API:

```python
import asyncio
from groupcache import configure_cluster, cached

async def main():
    # Configure cluster  
    await configure_cluster()
    
    @cached(group="users")
    async def load_user(user_id: str) -> str:
        """Load user data - only called on cache miss"""
        print(f"Loading user {user_id} from database...")
        return f"User data for {user_id}"
    
    # Use the cached function
    data1 = await load_user("123")  # Cache miss - calls function
    data2 = await load_user("123")  # Cache hit - returns cached value
    
    print(f"First call: {data1}")
    print(f"Second call: {data2}")

if __name__ == "__main__":
    asyncio.run(main())
```

The decorator automatically handles cache key generation and group management. Each function gets its own cache group based on the function name and arguments.

**Note:** The decorator API requires that the same cached function exists on all peer servers. This works well when you deploy identical copies of your application across multiple servers (ex. a FastAPI app), but won't work if different servers run different code.

## How GroupCache Works

GroupCache uses several key techniques to provide efficient distributed caching:

**Consistent Hashing** - Keys are distributed across servers using consistent hashing. Each key is assigned to a specific "owner" server based on the hash of the key. This ensures the same key always maps to the same server, even when servers are added or removed.
**Cache Ownership** - Each server is authoritative for a subset of keys. When a server needs a key it doesn't own, it makes an HTTP request to the owner server. The owner loads the data (if not cached) and returns it.
**Hot Cache** - Servers maintain a small "hot cache" for frequently accessed keys they don't own. This reduces repeated requests to peer servers for popular data.
**Singleflight** - When multiple concurrent requests need the same uncached key, only one request actually loads the data while others wait for the result. This prevents duplicate expensive operations (like database queries).
**Fallback Loading** - If a peer server is unavailable, the requesting server can fall back to loading the data locally, ensuring availability even during partial outages.


## Design Differences from Go Implementation

This Python port maintains the core groupcache semantics while adapting to Python's ecosystem:

- **Async/await support** - All APIs are async-first since distributed caching is I/O bound and async/await is optimal for handling concurrent network operations in Python.
- **Decorator API** - Python-specific addition that provides syntactic sugar over the core API for common use cases.
- **Pickle serialization** - Go groupcache only supports string keys while the decorator API adds pickle serialization to handle complex function arguments.  I may remove pickle support in the future.
- **Single-peer optimization** - When no peers are configured, HTTP overhead is automatically skipped for local-only caching.

The core distributed caching behavior, consistent hashing, and singleflight coordination remain faithful to the original Go implementation.