#!/usr/bin/env python3
import asyncio
import os
import sys
import time

sys.path.insert(0, "/app")
from groupcache.groupcache import configure_cluster, cached

node_id = os.getenv("NODE_ID", "1")


@cached(group="data")
async def compute(key: str) -> str:
    print(f"[NODE {node_id}] Computing {key}...", flush=True)
    await asyncio.sleep(2)  # Simulate expensive work
    return f"result_{key}_from_node_{node_id}"


async def main():
    # Start cluster
    print(f"[NODE {node_id}] Starting cluster...", flush=True)
    await configure_cluster(
        self_url=os.getenv("SELF_URL", "http://localhost:8080"),
        peer_urls=[
            url.strip() for url in os.getenv("PEER_URLS", "").split(",") if url.strip()
        ],
    )

    print(f"[NODE {node_id}] Node ready", flush=True)

    # Test the cache
    await asyncio.sleep(3)  # Let other nodes start
    print(f"[NODE {node_id}] Starting test...", flush=True)

    for i in range(5):
        key = f"item_{i % 3}"  # Repeat some keys
        print(f"[NODE {node_id}] Requesting {key}...", flush=True)

        start_time = time.time()
        result = await compute(key)
        duration = time.time() - start_time

        # Determine if this was a cache hit or miss based on duration
        if duration > 1.0:  # Computation takes ~2s, cache lookups are fast
            status = "CACHE MISS"
        else:
            status = "CACHE HIT"

        print(
            f"[NODE {node_id}] {status} {key} -> {result} ({duration:.2f}s)", flush=True
        )
        await asyncio.sleep(1)

    print(f"[NODE {node_id}] Test complete, keeping server running...", flush=True)
    # Keep running
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
