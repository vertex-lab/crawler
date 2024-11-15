# Nostrcrawler

The goals of this project are

1. Crawl the nostr network 24/7/365, looking for follow lists (kind3s).

2. Quicky estimate whether these new follow lists should be added to the DB or not, based on the author's rank.

3. Generate random walks for the nodes in the graph, and keep them updated all the time.

4. Use these random walks to efficiently compute acyclic Monte Carlo Pageranks (personalized and normal).

This project implements the algorithms described in [this paper](http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf)

## Structure

`/cmd/app/main.go`: the main function, which should:
- generate the random walks on restart
- listen for graph updates (e.g. a node is added, or a node has changed it's out-edges)
- update the random walks

`/pkg/models/`: defines the fundamental interfaces and structures that are used across packages

`/pkg/store/`: contains two implementations of the `RandomWalkStore` interface, which deals with saving and fetching random walks from an in-memory database.
- `/pkg/store/redistore/`: Implementation using redis, for production use.
- `/pkg/store/mock/`: Implementation in-memory, for tests.

`/pkg/database/`: contains two implementations of the `Database` interface, which deals with saving and fetching nodes and edges from a persistent database.
- `/pkg/database/redisdb/`: Implementation using redis, for production use (WIP).
- `/pkg/database/mock/`: Implementation in-memory, for tests.

`/pkg/walks/`: responsible for all the logic around the random walks, including generating new random walks and updating existing ones.

`/pkg/pagerank/`: contains the definitions of all algorithms that use random walks, such as pagerank and personalized pagerank.

`/tests/stochastic/`: contains stochastic tests that ensure that the walk logic and the algorithms (pagerank, personalized pagerank) work together as expected.