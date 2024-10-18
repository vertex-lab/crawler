# Nostrcrawler

The goals of this project are

1. Crawl the nostr network 24/7/365, looking for follow lists (kind3s).

2. Quicky estimate whether these new followlists should be added to the DB or not, based on the author's rank.

3. Generate random walks for the nodes in the graph, and keep them updated all the time.

4. Use these random walks to efficiently compute acyclic Monte Carlo Pageranks (personalized and normal).

This project implements the algorithms described in [this paper](http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf)

## Structure

`/cmd/app/main.go`: the main function, which should
- generate the random walks on restart
- listen for graph updates (e.g. a node is added, or a node has changed it's out-edges)
- update the random walks

`/pkg/graph/`: includes `database.go` which describes the database interface and the node struct.

`/pkg/walks/`: contains the definitions of the fundamental structures of `RandomWalk` and `RandomWalksManager` and the algorithms that deal with generating and updating them.

`/pkg/pagerank/`: contains the definitions of all algorithms that use random walks, such as pagerank and personalized pagerank.