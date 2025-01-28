# Pipeline

![data_pipeline.png](/home/filippo/Documents/Code/Nostrcrawler/docs/data_pipeline.png)

# Definitions

**Active node**: A node is said to be active if we generate random walks starting from that node. This implies that such node is contributing to the pagerank distribution with its view of the network. To be active, a node needs to acquire sufficient pagerank mass, higher than the threshold.

**Inactive node**: A node is said to be inactive if we DON'T generate random walks starting from that node. This implies that such node is NOT contributing to the pagerank distribution with its view of the network.

**Our Goal**: Our goal is to have a good enough balance between active and inactive nodes. It's important to note that these two sets are only going to influence our internal system dynamics and global pagerank, NOT personalized pagerank which solely depends on the source node.

---

# Scenarios

- [x] **New event from an active node.**
  Firehose --> Event Channel --> Process Events

- [x] **New event from an unknown node.**
  Discarted by Firehose after one Database query

- [x] **New event from an inactive node.**
  Discarted by Firehose after one Database query

- [x] **Unknown pubkey in the follow list of an active node**
  Added to the database in the Process Events as an inactive node.

- [x] **Inactive node acquires enough pagerank**
  Gets promoted by Node Arbiter --> Pubkey Channel --> Query Pubkeys --> Event Channel --> Process Events

- [x] **Active node loses enough pagerank**
  Gets demoted by Node Arbiter.