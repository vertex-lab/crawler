# Pipeline Scenarios

**Active node**: A node is said to be active if we generate random walks starting from that node. This implies that such node is contributing to the pagerank distribution with its view of the network. To be active, a node needs to acquire sufficient pagerank mass, higher than the threshold.



**Inactive node**: A node is said to be inactive if we DON'T generate random walks starting from that node. This implies that such node is NOT contributing to the pagerank distribution with its view of the network.

---

## Scenarios

- [x] **New event from an active node.**
  Firehose --> Event Channel --> Process Events

- [x] **New event from an unknown node.**
  Discarted by Firehose after one Database query

- [x] **New event from an inactive node.**
  Discarted by Firehose after one Database query

- [x] **Unknown pubkey in the follow list of an active node**
  Added to the database in the Process Events as inactive node.

- [x] **Inactive node acquires enough pagerank**
  Gets promoted by Node Arbiter --> Pubkey Channel --> Query Pubkeys --> Event Channel --> Process Events

- [x] **Active node loses enough pagerank**
  Gets demoted by Node Arbiter.