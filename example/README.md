# GroupCache Distributed Example

This example demonstrates GroupCache working in a distributed environment with 3 nodes communicating over HTTP.

## Architecture

```
┌─────────┐    HTTP     ┌─────────┐    HTTP     ┌─────────┐
│  Node 1 │◄───────────►│  Node 2 │◄───────────►│  Node 3 │
│ :8081   │             │ :8082   │             │ :8083   │
└─────────┘             └─────────┘             └─────────┘
     │                       │                       │
     └───────────────────────┼───────────────────────┘
              Consistent Hash Ring
```


## Quick Start

```bash
# Start the 3-node cluster
docker-compose up --build
```

## What You'll See

Each node will:
1. Start up and join the cluster
2. Test the same keys (`item_A`, `item_B`, `item_C`) 
3. Show cache hits vs misses with timing information

**Expected behavior:**
- **Round 1**: Cache misses (slow ~2s) - first time computing each key
- **Round 2**: Cache hits (fast ~0.01s) - data retrieved from cache/peers
- **Round 3**: Cache hits (fast ~0.01s) - data still cached
