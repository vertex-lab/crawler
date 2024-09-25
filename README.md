# Analytics Engine for a graph

The goal of this project is to efficiently generate random walks for the underlying graph, and keep them updated over time.

This project implements the algorithms described in [this paper](http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf)

## Structure

`/cmd/app/main.go`: the main function, that should:
- generate the random walks upon restart
- listen to graph updates (e.g. a node is added, or a node has changed it's out-edges)
- recompute the random walks

`/pkg/graph/`: includes `database.go` and `graph.go` which describe the database
interface and the graph struct.

`/mock_database/mock_database.go`: creates a mock database structure that is 
used to test other functions by simulating calls to a database

`/pagerank/random_walks.go`: creates the RandomWalksMap function, used to store
and access random walks in memory