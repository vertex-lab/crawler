# Crawler v1 - Deprecated

This was the initial implementation of our Nostr network crawler, designed to continuously ingest follow lists (`kind:3` events), compute Monte Carlo Pageranks (global and personalized) via random walks, and persist the graph in Redis.

## Replacement
Several architectural flaws ultimately limited its maintainability and scalability, which is why active development has moved to [crawler_v2](https://github.com/vertex-lab/crawler_v2).
This second version introduces a simpler and more modular architecture, clearer separation of concerns, and multiple runtime modes (`crawler` and `sync`).

## Status

This repository is now **read-only**. It remains available for historical reference.  
For future work, contributions, or deployment, please refer to [crawler_v2](https://github.com/vertex-lab/crawler_v2).
